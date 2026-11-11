package bullmq

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Connection timeouts. Blocking commands (BZPOPMIN, XREAD BLOCK) get their
// own deadline: go-redis extends the read deadline to the block time + 10s,
// which doubles as a watchdog against half-open sockets.
const (
	dialTimeout  = 10 * time.Second
	ioTimeout    = 10 * time.Second
	poolTimeout  = 30 * time.Second
	redisVersion = 2 // RESP2, matching how the Lua scripts' replies are consumed
)

// RedisConnection is a shared, goroutine-safe Redis client used by queues,
// workers, flow producers and event listeners. go-redis pools connections and
// transparently re-dials after a network failure or a Redis restart.
type RedisConnection struct {
	client    redis.UniversalClient
	opts      RedisConnectionOptions
	owned     bool
	closeOnce sync.Once
	closeErr  error
}

// NewRedisConnection opens a connection (standalone or cluster) and verifies
// it with PING.
func NewRedisConnection(ctx context.Context, opts RedisConnectionOptions) (*RedisConnection, error) {
	client, err := newUniversalClient(opts, "")
	if err != nil {
		return nil, err
	}

	conn := &RedisConnection{client: client, opts: opts, owned: true}
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("bullmq: connect to redis: %w", err)
	}

	return conn, nil
}

// NewRedisConnectionFromClient wraps an existing go-redis client. The client
// is not closed by [RedisConnection.Close]; its owner remains responsible for
// it. It must be a *redis.Client or *redis.ClusterClient for workers and
// event listeners, which open dedicated blocking connections with the same
// settings.
func NewRedisConnectionFromClient(client redis.UniversalClient) *RedisConnection {
	return &RedisConnection{client: client}
}

// Client returns the underlying go-redis client, e.g. to run raw commands.
func (c *RedisConnection) Client() redis.UniversalClient { return c.client }

// Options returns the options the connection was created with (zero when it
// wraps an external client).
func (c *RedisConnection) Options() RedisConnectionOptions { return c.opts }

// IsCluster reports whether the connection targets a Redis Cluster.
func (c *RedisConnection) IsCluster() bool {
	_, ok := c.client.(*redis.ClusterClient)
	return ok
}

// Ping verifies connectivity.
func (c *RedisConnection) Ping(ctx context.Context) error { return c.client.Ping(ctx).Err() }

// Close closes the connection if it is owned. It is idempotent.
func (c *RedisConnection) Close() error {
	c.closeOnce.Do(func() {
		if c.owned {
			c.closeErr = c.client.Close()
		}
	})

	return c.closeErr
}

// newDedicatedClient opens a separate client with a single connection per
// node, used for blocking commands so they never starve the shared pool. A
// non-empty name is registered with CLIENT SETNAME on every (re)connection.
func (c *RedisConnection) newDedicatedClient(name string) (redis.UniversalClient, error) {
	switch cl := c.client.(type) {
	case *redis.Client:
		o := *cl.Options()
		o.PoolSize, o.MinIdleConns, o.MaxIdleConns = 1, 0, 1
		o.ClientName = name
		o.MaxRetries = -1

		return redis.NewClient(&o), nil
	case *redis.ClusterClient:
		o := *cl.Options()
		o.PoolSize, o.MinIdleConns, o.MaxIdleConns = 1, 0, 1
		o.ClientName = name
		o.MaxRetries = -1

		return redis.NewClusterClient(&o), nil
	default:
		if c.owned {
			return newUniversalClient(c.opts, name)
		}

		return nil, invalidConfig("dedicated connections need a *redis.Client or *redis.ClusterClient, got %T", c.client)
	}
}

// clientLists returns the CLIENT LIST output of the server, or of every
// primary of a cluster (nodes failing to answer are skipped).
func (c *RedisConnection) clientLists(ctx context.Context) ([]string, error) {
	cc, ok := c.client.(*redis.ClusterClient)
	if !ok {
		s, err := c.client.ClientList(ctx).Result()
		if err != nil {
			return nil, err
		}

		return []string{s}, nil
	}

	var (
		mu    sync.Mutex
		lists []string
	)

	err := cc.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
		s, err := node.ClientList(ctx).Result()
		if err != nil {
			return nil //nolint:nilerr // unreachable nodes are skipped on purpose
		}

		mu.Lock()

		lists = append(lists, s)
		mu.Unlock()

		return nil
	})

	return lists, err
}

func newUniversalClient(opts RedisConnectionOptions, name string) (redis.UniversalClient, error) {
	tlsCfg, err := buildTLSConfig(opts)
	if err != nil {
		return nil, err
	}

	poolSize := opts.MaxConnections
	if name != "" {
		poolSize = 1
	}

	if opts.IsCluster() {
		co := &redis.ClusterOptions{
			Username:     opts.Username,
			Password:     opts.Password,
			ClientName:   name,
			Protocol:     redisVersion,
			DialTimeout:  dialTimeout,
			ReadTimeout:  ioTimeout,
			WriteTimeout: ioTimeout,
			PoolTimeout:  poolTimeout,
			PoolSize:     poolSize,
			MaxRetries:   -1,
			TLSConfig:    tlsCfg,
		}
		for _, node := range opts.ClusterNodes {
			o, err := redis.ParseURL(normalizeURL(node))
			if err != nil {
				return nil, invalidConfig("invalid cluster node %q: %v", redactURLUserinfo(node), err)
			}

			co.Addrs = append(co.Addrs, o.Addr)
			if co.Username == "" {
				co.Username = o.Username
			}

			if co.Password == "" {
				co.Password = o.Password
			}

			if co.TLSConfig == nil && o.TLSConfig != nil {
				co.TLSConfig = o.TLSConfig
			}
		}

		return redis.NewClusterClient(co), nil
	}

	u := opts.EffectiveURL()

	o, err := redis.ParseURL(normalizeURL(u))
	if err != nil {
		return nil, invalidConfig("invalid redis url %q: %v", redactURLUserinfo(u), err)
	}

	if opts.Host == "" {
		// Typed credentials and database complement a plain URL.
		if opts.Username != "" {
			o.Username = opts.Username
		}

		if opts.Password != "" {
			o.Password = opts.Password
		}

		if opts.DB != nil {
			o.DB = *opts.DB
		}
	}

	if tlsCfg != nil {
		if o.TLSConfig != nil && tlsCfg.ServerName == "" {
			tlsCfg.ServerName = o.TLSConfig.ServerName
		}

		o.TLSConfig = tlsCfg
	}

	o.ClientName = name
	o.Protocol = redisVersion
	o.DialTimeout = dialTimeout
	o.ReadTimeout = ioTimeout
	o.WriteTimeout = ioTimeout
	o.PoolTimeout = poolTimeout

	o.MaxRetries = -1
	if poolSize > 0 {
		o.PoolSize = poolSize
	}

	return redis.NewClient(o), nil
}

// normalizeURL drops query parameters go-redis does not understand (such as
// redis-rs' `protocol=resp3`), keeping the ones it does.
func normalizeURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.RawQuery == "" {
		return raw
	}

	q := u.Query()
	q.Del("protocol")
	u.RawQuery = q.Encode()

	return u.String()
}

func buildTLSConfig(opts RedisConnectionOptions) (*tls.Config, error) {
	certs := opts.TLSCerts
	if certs == nil {
		if opts.TLS && opts.IsCluster() {
			return &tls.Config{MinVersion: tls.VersionTLS12}, nil
		}

		return nil, nil
	}

	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	if len(certs.RootCert) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(certs.RootCert) {
			return nil, invalidConfig("tls root certificate is not valid PEM")
		}

		cfg.RootCAs = pool
	}

	if len(certs.ClientCert) > 0 || len(certs.ClientKey) > 0 {
		pair, err := tls.X509KeyPair(certs.ClientCert, certs.ClientKey)
		if err != nil {
			return nil, invalidConfig("tls client certificate: %v", err)
		}

		cfg.Certificates = []tls.Certificate{pair}
	}

	if host := opts.Host; host != "" {
		cfg.ServerName = strings.Trim(host, "[]")
	}

	return cfg, nil
}

// isClosedErr reports errors caused by using a closed client.
func isClosedErr(err error) bool {
	return errors.Is(err, redis.ErrClosed) || errors.Is(err, context.Canceled)
}
