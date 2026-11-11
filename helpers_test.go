package bullmq_test

import (
	"bullmq"
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// testRedisURL returns REDIS_URL or the local default.
func testRedisURL() string {
	if u := os.Getenv("REDIS_URL"); u != "" {
		return u
	}
	return "redis://127.0.0.1:6379"
}

// testConnection returns the connection options used by integration tests.
func testConnection() bullmq.RedisConnectionOptions {
	return bullmq.RedisConnectionOptions{URL: testRedisURL()}
}

// testQueueName returns a unique queue name for test isolation.
func testQueueName() string {
	return "test-" + strings.Split(uuid.NewString(), "-")[0]
}

// newTestQueue creates a queue that is obliterated and closed at cleanup.
func newTestQueue(t testing.TB, name string, opts *bullmq.QueueOptions) *bullmq.Queue {
	t.Helper()
	if opts == nil {
		opts = &bullmq.QueueOptions{}
	}
	if opts.Connection.URL == "" && opts.Connection.Host == "" && len(opts.Connection.ClusterNodes) == 0 {
		opts.Connection = testConnection()
	}
	q, err := bullmq.NewQueue(context.Background(), name, opts)
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	t.Cleanup(func() { cleanupQueue(q) })
	return q
}

// cleanupQueue obliterates and closes q.
func cleanupQueue(q *bullmq.Queue) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = q.Obliterate(ctx, true, 1000)
	_ = q.Close()
}

// newTestWorker creates a worker closed at cleanup.
func newTestWorker(t testing.TB, name string, p bullmq.Processor, opts *bullmq.WorkerOptions) *bullmq.Worker {
	t.Helper()
	if opts == nil {
		opts = &bullmq.WorkerOptions{}
	}
	if opts.Connection.URL == "" && opts.Connection.Host == "" && len(opts.Connection.ClusterNodes) == 0 {
		opts.Connection = testConnection()
	}
	w, err := bullmq.NewWorker(context.Background(), name, p, opts)
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	t.Cleanup(func() { _ = w.Close(5 * time.Second) })
	return w
}

// waitForEvent reads worker events until match returns true or timeout.
func waitForEvent(t testing.TB, w *bullmq.Worker, timeout time.Duration, match func(bullmq.WorkerEvent) bool) bullmq.WorkerEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		ev, ok := w.NextEvent(ctx)
		if !ok {
			t.Fatalf("timeout waiting for worker event")
		}
		if match(ev) {
			return ev
		}
	}
}

// eventType matches events of type typ.
func eventType(typ bullmq.WorkerEventType) func(bullmq.WorkerEvent) bool {
	return func(e bullmq.WorkerEvent) bool { return e.Type == typ }
}

// eventually polls cond every 50ms until it holds or timeout elapses.
func eventually(t testing.TB, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", timeout, msg)
}

// must returns v, panicking on err (which fails the running test with a
// stack trace). Use it only on the test goroutine.
func must[T any](v T, err error) T {
	if err != nil {
		panic(fmt.Sprintf("unexpected error: %v", err))
	}
	return v
}

// noErr fails the test on err.
func noErr(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// isolatedConnection returns test connection options pinned to logical
// database db. Tests that kill connections on purpose use a dedicated
// database so they do not disturb tests running in parallel.
func isolatedConnection(db int) bullmq.RedisConnectionOptions {
	o := testConnection()
	o.URL = withDatabase(o.URL, db)
	return o
}

// withDatabase returns rawURL with its database (path) replaced by db,
// preserving credentials, scheme and query.
func withDatabase(rawURL string, db int) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic("REDIS_URL must be a valid URL: " + err.Error())
	}
	u.Path = "/" + strconv.Itoa(db)
	return u.String()
}

// killConnectionsInDB kills every client connection that selected database
// db, as a Redis restart or network blip would, and returns how many were
// killed. The killer connects to the default database and is never killed.
func killConnectionsInDB(t testing.TB, db int) int64 {
	t.Helper()
	ctx := context.Background()
	opts, err := redis.ParseURL(testRedisURL())
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	killer := redis.NewClient(opts)
	defer killer.Close()
	list, err := killer.ClientList(ctx).Result()
	if err != nil {
		t.Fatalf("CLIENT LIST: %v", err)
	}
	var killed int64
	want := "db=" + strconv.Itoa(db)
	for _, line := range strings.Split(list, "\n") {
		var id string
		inDB := false
		for _, f := range strings.Fields(line) {
			if v, ok := strings.CutPrefix(f, "id="); ok {
				id = v
			}
			if f == want {
				inDB = true
			}
		}
		if id == "" || !inDB {
			continue
		}
		n, _ := killer.Do(ctx, "CLIENT", "KILL", "ID", id).Int64()
		killed += n
	}
	return killed
}

// rawClient returns a plain go-redis client for raw assertions, closed at
// cleanup.
func rawClient(t testing.TB) *redis.Client {
	t.Helper()
	opts, err := redis.ParseURL(testRedisURL())
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	c := redis.NewClient(opts)
	t.Cleanup(func() { _ = c.Close() })
	return c
}
