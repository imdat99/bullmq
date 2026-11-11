package bullmq_test

// Redis Cluster integration tests.
//
// These tests run against a real Redis Cluster whose seed node URLs are given
// (comma separated) in `REDIS_CLUSTER_NODES`, e.g.
// `REDIS_CLUSTER_NODES=redis://127.0.0.1:7000,redis://127.0.0.1:7001`.
// They are skipped when the variable is not set.
//
// All keys of a queue must hash to the same slot, so every test uses a
// hash-tagged prefix.
// Ported from bullmq_rust/tests/cluster_test.rs.

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/imdat99/bullmq"
)

const evClusterPrefix = "{bull-rs}"

// evClusterConnection returns the cluster connection options from
// REDIS_CLUSTER_NODES, or false when it is unset or empty.
func evClusterConnection() (bullmq.RedisConnectionOptions, bool) {
	raw := os.Getenv("REDIS_CLUSTER_NODES")
	var nodes []string
	for _, n := range strings.Split(raw, ",") {
		if n = strings.TrimSpace(n); n != "" {
			nodes = append(nodes, n)
		}
	}
	if len(nodes) == 0 {
		return bullmq.RedisConnectionOptions{}, false
	}
	return bullmq.RedisConnectionOptions{ClusterNodes: nodes}, true
}

// evClusterOrSkip mirrors Rust's cluster_or_skip! macro.
func evClusterOrSkip(t *testing.T) bullmq.RedisConnectionOptions {
	t.Helper()
	opts, ok := evClusterConnection()
	if !ok {
		t.Skip("REDIS_CLUSTER_NODES not set; skipping Redis Cluster test")
	}
	return opts
}

// evClusterQueue creates a queue on the cluster with the hash-tagged prefix,
// obliterated and closed at cleanup.
func evClusterQueue(t *testing.T, name string, conn bullmq.RedisConnectionOptions) *bullmq.Queue {
	t.Helper()
	return newTestQueue(t, name, &bullmq.QueueOptions{Connection: conn, Prefix: evClusterPrefix})
}

// evEchoProcessor returns each job's data as its result.
func evEchoProcessor() bullmq.Processor {
	return func(ctx context.Context, job *bullmq.Job) (any, error) { return job.Data, nil }
}

func TestClusterConnectionRoutesCommands(t *testing.T) {
	opts := evClusterOrSkip(t)
	ctx := context.Background()
	conn := must(bullmq.NewRedisConnection(ctx, opts))
	defer conn.Close()
	if !conn.IsCluster() {
		t.Fatalf("expected a cluster connection")
	}
	// Rust also checks that the standalone-only accessors (try_managed_conn,
	// try_client, try_conn, dedicated_connection) are unavailable; the Go
	// connection exposes a single UniversalClient, which must be a cluster
	// client (Rust AnyConnection::Cluster).
	client, ok := conn.Client().(*redis.ClusterClient)
	if !ok {
		t.Fatalf("client is %T, want *redis.ClusterClient", conn.Client())
	}
	noErr(t, conn.Ping(ctx))

	// Keys in different slots are routed to their own nodes.
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("bullmq-rs-cluster-test:%d", i)
		noErr(t, client.Set(ctx, key, i, 0).Err())
		value := must(client.Get(ctx, key).Int64())
		if value != int64(i) {
			t.Fatalf("GET %s = %d, want %d", key, value, i)
		}
		noErr(t, client.Del(ctx, key).Err())
	}

	// Blocking connections work on the cluster too: a QueueEvents sharing the
	// connection opens its dedicated blocking client from it.
	events, err := bullmq.NewQueueEventsWithConnection(ctx, testQueueName(), conn, &bullmq.QueueEventsOptions{
		Prefix:          evClusterPrefix,
		BlockingTimeout: 1000 * time.Millisecond,
	})
	noErr(t, err)
	noErr(t, events.Close())
}

func TestClusterIdleWorkerPicksUpNewJobQuickly(t *testing.T) {
	opts := evClusterOrSkip(t)
	ctx := context.Background()
	name := testQueueName()
	queue := evClusterQueue(t, name, opts)
	worker := newTestWorker(t, name, evEchoProcessor(), &bullmq.WorkerOptions{
		Connection: opts,
		Prefix:     evClusterPrefix,
		DrainDelay: 30 * time.Second,
	})

	ev := waitForEvent(t, worker, 5*time.Second, func(e bullmq.WorkerEvent) bool {
		return e.Type == bullmq.EventDrained || e.Type == bullmq.EventError
	})
	if ev.Type == bullmq.EventError {
		t.Fatalf("worker error: %v", ev.Err)
	}

	// Keep BZPOPMIN blocked past the regular connection's 10s response timeout,
	// but enqueue well before the 30s drain delay so polling cannot pass.
	time.Sleep(12 * time.Second)

	job := must(queue.Add(ctx, "wake-up", map[string]any{"hello": "world"}, nil))
	ev = waitForEvent(t, worker, 3*time.Second, func(e bullmq.WorkerEvent) bool {
		return e.Type == bullmq.EventCompleted || e.Type == bullmq.EventError
	})
	if ev.Type == bullmq.EventError {
		t.Fatalf("idle cluster worker emitted a spurious error: %v", ev.Err)
	}
	if ev.JobID != job.ID {
		t.Fatalf("completed job %q, want %q", ev.JobID, job.ID)
	}

	noErr(t, worker.Close(5*time.Second))
}

func TestClusterIdleQueueEmitsNoSpuriousErrorEvents(t *testing.T) {
	opts := evClusterOrSkip(t)
	ctx := context.Background()
	name := testQueueName()
	queue := evClusterQueue(t, name, opts)
	events := evNewEvents(t, name, &bullmq.QueueEventsOptions{
		Connection:      opts,
		Prefix:          evClusterPrefix,
		LastEventID:     "0",
		BlockingTimeout: 30 * time.Second,
	})

	// XREAD BLOCK must outlive the regular connection's 10s response timeout
	// without emitting errors, then still deliver events when the queue wakes.
	time.Sleep(12 * time.Second)
	for {
		entry, ok := events.TryNextEvent()
		if !ok {
			break
		}
		if entry.Event.Type == bullmq.QueueEventError {
			t.Fatalf("idle cluster queue emitted a spurious error event: %s", entry.Event.Message)
		}
	}

	job := must(queue.Add(ctx, "wake-up", map[string]any{}, nil))
	ev := evWaitFor(t, events, 3*time.Second, func(e bullmq.QueueEvent) bool {
		return e.Type == bullmq.QueueEventAdded || e.Type == bullmq.QueueEventError
	})
	if ev.Type == bullmq.QueueEventError {
		t.Fatalf("cluster queue emitted a spurious error event: %s", ev.Message)
	}
	if ev.JobID != job.ID {
		t.Fatalf("added job %q, want %q", ev.JobID, job.ID)
	}

	noErr(t, events.Close())
}

func TestClusterQueueWorkerAndEvents(t *testing.T) {
	opts := evClusterOrSkip(t)
	ctx := context.Background()
	name := testQueueName()
	queue := evClusterQueue(t, name, opts)

	events := evNewEvents(t, name, &bullmq.QueueEventsOptions{
		Connection:      opts,
		Prefix:          evClusterPrefix,
		LastEventID:     "0",
		BlockingTimeout: 1000 * time.Millisecond,
	})

	worker := newTestWorker(t, name, evEchoProcessor(), &bullmq.WorkerOptions{
		Connection: opts,
		Prefix:     evClusterPrefix,
	})

	job := must(queue.Add(ctx, "cluster-job", map[string]any{"foo": "bar"}, nil))

	completed := evWaitFor(t, events, 15*time.Second, func(e bullmq.QueueEvent) bool {
		return e.Type == bullmq.QueueEventCompleted && e.JobID == job.ID
	})
	var returnValue map[string]any
	noErr(t, completed.DecodeReturnValue(&returnValue))
	if !reflect.DeepEqual(returnValue, map[string]any{"foo": "bar"}) {
		t.Fatalf("return value = %s, want {\"foo\":\"bar\"}", completed.ReturnValue)
	}
	if n := must(queue.GetCompletedCount(ctx)); n != 1 {
		t.Fatalf("completed count = %d, want 1", n)
	}

	// The worker names its blocking connection on every node, so it is
	// discoverable through CLIENT LIST on the cluster.
	var workers []map[string]string
	eventually(t, 5*time.Second, func() bool {
		workers = must(queue.GetWorkers(ctx))
		return len(workers) > 0
	}, "timed out waiting for the worker to be listed")
	if len(workers) != 1 {
		t.Fatalf("workers = %d, want 1", len(workers))
	}

	noErr(t, worker.Close(5*time.Second))
	noErr(t, events.Close())
}

func TestClusterFlowAcrossQueues(t *testing.T) {
	opts := evClusterOrSkip(t)
	ctx := context.Background()
	parentName := testQueueName()
	childName := testQueueName()
	parentQueue := evClusterQueue(t, parentName, opts)
	childQueue := evClusterQueue(t, childName, opts)

	flow := must(bullmq.NewFlowProducer(ctx, &bullmq.FlowProducerOptions{
		Connection: opts,
		Prefix:     evClusterPrefix,
	}))
	defer flow.Close()

	child := func(i int) bullmq.FlowJob {
		return bullmq.FlowJob{
			Name:      "child",
			QueueName: childName,
			Data:      map[string]any{"value": i},
		}
	}
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent",
		QueueName: parentName,
		Data:      map[string]any{},
		Children:  []bullmq.FlowJob{child(1), child(2)},
	}))

	makeWorker := func(queueName string) *bullmq.Worker {
		return newTestWorker(t, queueName, evEchoProcessor(), &bullmq.WorkerOptions{
			Connection: opts,
			Prefix:     evClusterPrefix,
		})
	}
	childWorker := makeWorker(childName)
	parentWorker := makeWorker(parentName)

	eventually(t, 15*time.Second, func() bool {
		return must(parentQueue.GetCompletedCount(ctx)) >= 1
	}, "timed out waiting for the parent job to complete")

	if n := must(childQueue.GetCompletedCount(ctx)); n != 2 {
		t.Fatalf("child completed count = %d, want 2", n)
	}
	parent := must(parentQueue.GetJob(ctx, tree.Job.ID))
	if parent == nil {
		t.Fatalf("parent job exists")
	}
	values := must(parent.GetChildrenValues(ctx))
	if len(values) != 2 {
		t.Fatalf("children values = %d, want 2", len(values))
	}

	noErr(t, childWorker.Close(5*time.Second))
	noErr(t, parentWorker.Close(5*time.Second))
}
