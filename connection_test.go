package bullmq_test

// Connection option tests — typed fields and URL building
// (port of bullmq_rust/tests/connection_test.rs).
//
// Rust's `{:?}` (Debug) output maps to the Go Stringer
// (RedisConnectionOptions.String / TLSCerts.String), which redacts
// credentials and certificate material.

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"

	"github.com/imdat99/bullmq"
)

func mxAssertURL(t *testing.T, opts bullmq.RedisConnectionOptions, want string) {
	t.Helper()
	if got := opts.EffectiveURL(); got != want {
		t.Fatalf("EffectiveURL() = %q, want %q", got, want)
	}
}

// mxAssertRedacted checks that s contains every want and none of secrets.
func mxAssertRedacted(t *testing.T, s string, want, secrets []string) {
	t.Helper()
	for _, w := range want {
		if !strings.Contains(s, w) {
			t.Fatalf("%q does not contain %q", s, w)
		}
	}
	for _, sec := range secrets {
		if strings.Contains(s, sec) {
			t.Fatalf("%q leaks %q", s, sec)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// EffectiveURL() — typed connection options build the right URL
// ═══════════════════════════════════════════════════════════════════════════

func TestEffectiveUrlFallsBackToUrl(t *testing.T) {
	mxAssertURL(t, bullmq.RedisConnectionOptions{URL: "redis://example.com:6380"}, "redis://example.com:6380")
}

func TestEffectiveUrlFromHostOnly(t *testing.T) {
	mxAssertURL(t, bullmq.RedisConnectionOptions{Host: "127.0.0.1"}, "redis://127.0.0.1:6379")
}

func TestEffectiveUrlWithPortAndDb(t *testing.T) {
	mxAssertURL(t, bullmq.RedisConnectionOptions{Host: "redis.local", Port: 6380, DB: bullmq.Ptr(3)},
		"redis://redis.local:6380/3")
}

func TestEffectiveUrlWithAuth(t *testing.T) {
	mxAssertURL(t, bullmq.RedisConnectionOptions{Host: "redis.local", Username: "alice", Password: "secret"},
		"redis://alice:secret@redis.local:6379")
}

func TestEffectiveUrlWithAuthPercentEncoding(t *testing.T) {
	mxAssertURL(t, bullmq.RedisConnectionOptions{Host: "redis.local", Username: "alice@example.com", Password: "p@ss:w/rd"},
		"redis://alice%40example.com:p%40ss%3Aw%2Frd@redis.local:6379")
}

func TestEffectiveUrlWithPasswordOnly(t *testing.T) {
	mxAssertURL(t, bullmq.RedisConnectionOptions{Host: "redis.local", Password: "secret"},
		"redis://:secret@redis.local:6379")
}

func TestEffectiveUrlTlsScheme(t *testing.T) {
	mxAssertURL(t, bullmq.RedisConnectionOptions{Host: "secure.redis", Port: 6380, TLS: true},
		"rediss://secure.redis:6380")
}

func TestEffectiveUrlTlsUpgradesUrlFallback(t *testing.T) {
	mxAssertURL(t, bullmq.RedisConnectionOptions{TLS: true}, "rediss://127.0.0.1:6379")

	// Builder call order must not matter (Go: plain struct fields).
	mxAssertURL(t, bullmq.RedisConnectionOptions{TLS: true, URL: "redis://example.com:6380"}, "rediss://example.com:6380")
	mxAssertURL(t, bullmq.RedisConnectionOptions{URL: "redis://example.com:6380", TLS: true}, "rediss://example.com:6380")

	// An explicit rediss:// URL is left untouched.
	mxAssertURL(t, bullmq.RedisConnectionOptions{URL: "rediss://example.com:6380", TLS: true}, "rediss://example.com:6380")
}

func TestEffectiveUrlIpv6HostBracketing(t *testing.T) {
	mxAssertURL(t, bullmq.RedisConnectionOptions{Host: "2001:db8::1", Port: 6380}, "redis://[2001:db8::1]:6380")
}

func TestEffectiveUrlKeepsBracketedIpv6Host(t *testing.T) {
	mxAssertURL(t, bullmq.RedisConnectionOptions{Host: "[2001:db8::1]"}, "redis://[2001:db8::1]:6379")
}

func TestDebugRedactsUrlCredentials(t *testing.T) {
	opts := bullmq.RedisConnectionOptions{URL: "redis://alice:secret@redis.local:6379/3"}
	debug := fmt.Sprintf("%v", opts)
	mxAssertRedacted(t, debug, []string{"redis://***@redis.local:6379/3"}, []string{"alice", "secret"})
}

func TestDebugRedactsTypedCredentials(t *testing.T) {
	opts := bullmq.RedisConnectionOptions{Host: "redis.local", Username: "alice", Password: "secret"}
	// The Go Stringer omits username/password entirely (Rust prints "***").
	debug := fmt.Sprintf("%v", opts)
	mxAssertRedacted(t, debug, []string{"redis.local"}, []string{"alice", "secret"})
	debug = fmt.Sprintf("%+v", opts)
	mxAssertRedacted(t, debug, nil, []string{"alice", "secret"})
}

func TestEffectiveUrlTlsCertsImplyRedissScheme(t *testing.T) {
	opts := bullmq.RedisConnectionOptions{
		Host:     "secure.redis",
		Port:     6380,
		TLSCerts: &bullmq.TLSCerts{RootCert: []byte("-----BEGIN CERTIFICATE-----")},
	}
	mxAssertURL(t, opts, "rediss://secure.redis:6380")
}

func TestEffectiveUrlTlsCertsImplyRedissSchemeForUrlFallback(t *testing.T) {
	opts := bullmq.RedisConnectionOptions{
		URL:      "redis://secure.redis:6380",
		TLSCerts: &bullmq.TLSCerts{RootCert: []byte("-----BEGIN CERTIFICATE-----")},
	}
	mxAssertURL(t, opts, "rediss://secure.redis:6380")
}

func TestDebugRedactsTlsCerts(t *testing.T) {
	opts := bullmq.RedisConnectionOptions{
		Host: "secure.redis",
		TLSCerts: &bullmq.TLSCerts{
			RootCert:   []byte("root-ca-pem"),
			ClientCert: []byte("client-cert-pem"),
			ClientKey:  []byte("client-key-pem"),
		},
	}
	secrets := []string{"root-ca-pem", "client-cert-pem", "client-key-pem"}
	mxAssertRedacted(t, fmt.Sprintf("%v", opts), nil, secrets)
	mxAssertRedacted(t, fmt.Sprintf("%+v", opts), nil, secrets)
	// The certificates themselves render redacted.
	mxAssertRedacted(t, fmt.Sprintf("%v", opts.TLSCerts), []string{"***"}, secrets)
}

func TestConnectViaTypedOptions(t *testing.T) {
	ctx := context.Background()
	// Parse REDIS_URL using go-redis itself so common forms work:
	// rediss://, credentials, db paths, and IPv6 literals.
	ro, err := redis.ParseURL(testRedisURL())
	if err != nil {
		t.Fatalf("REDIS_URL must be a valid redis:// or rediss:// URL: %v", err)
	}
	if ro.Network == "unix" {
		t.Fatal("TestConnectViaTypedOptions only supports TCP/TLS REDIS_URL, not unix sockets")
	}
	host, portStr, err := net.SplitHostPort(ro.Addr)
	if err != nil {
		t.Fatalf("unsupported connection address for test: %q: %v", ro.Addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("bad port %q: %v", portStr, err)
	}

	queue := newTestQueue(t, testQueueName(), &bullmq.QueueOptions{
		Connection: bullmq.RedisConnectionOptions{
			Host:     host,
			Port:     port,
			Username: ro.Username,
			Password: ro.Password,
			DB:       bullmq.Ptr(ro.DB),
			TLS:      ro.TLSConfig != nil,
		},
	})

	// Sanity: add a job and read it back.
	job := must(queue.Add(ctx, "typed", map[string]any{"ok": true}, nil))
	if job.ID == "" {
		t.Fatal("job id should not be empty")
	}
	if fetched := must(queue.GetJob(ctx, job.ID)); fetched == nil {
		t.Fatal("job should be readable")
	}
}

func TestClusterNodesOption(t *testing.T) {
	opts := bullmq.RedisConnectionOptions{}
	if opts.IsCluster() {
		t.Fatal("default options should not be a cluster")
	}

	opts = bullmq.RedisConnectionOptions{ClusterNodes: []string{
		fmt.Sprintf("redis://%s:%s@127.0.0.1:7000", "user", "secret"),
		"redis://127.0.0.1:7001",
	}}
	if !opts.IsCluster() {
		t.Fatal("options with cluster nodes should be a cluster")
	}
	if len(opts.ClusterNodes) != 2 {
		t.Fatalf("cluster nodes = %d, want 2", len(opts.ClusterNodes))
	}

	debug := fmt.Sprintf("%v", opts)
	mxAssertRedacted(t, debug, []string{"redis://***@127.0.0.1:7000"}, []string{"secret"})
}
