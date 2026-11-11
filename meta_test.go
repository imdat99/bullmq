package bullmq_test

// Queue meta/version tests — GetMeta, GetVersion, IsMaxed
// (port of bullmq_rust/tests/meta_test.rs).
//
// Requires a running Redis instance at redis://127.0.0.1:6379.

import (
	"bullmq"
	"context"
	"strconv"
	"strings"
	"testing"
	"time"
)

// mxOptInt64 renders an optional int64 for assertion messages.
func mxOptInt64(p *int64) string {
	if p == nil {
		return "None"
	}
	return strconv.FormatInt(*p, 10)
}

// mxAssertOptInt64 asserts that p equals want (nil = None).
func mxAssertOptInt64(t *testing.T, field string, p, want *int64) {
	t.Helper()
	if mxOptInt64(p) != mxOptInt64(want) {
		t.Fatalf("%s = %s, want %s", field, mxOptInt64(p), mxOptInt64(want))
	}
}

func TestGetVersionRecordsLibrary(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	version := must(queue.GetVersion(ctx))
	if version == "" {
		t.Fatal("library version should be recorded on create")
	}
	// The Rust crate records "bullmq-official:<v>"; the Go port records its
	// own library name.
	if !strings.HasPrefix(version, "bullmq-go:") {
		t.Fatalf("unexpected version: %s", version)
	}
}

func TestGetMetaDefaults(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	meta := must(queue.GetMeta(ctx))
	mxAssertOptInt64(t, "concurrency", meta.Concurrency, nil)
	mxAssertOptInt64(t, "max", meta.Max, nil)
	mxAssertOptInt64(t, "duration", meta.Duration, nil)
	if meta.Paused {
		t.Fatal("paused should be false")
	}
	// The library field is preserved among the "other" entries.
	if _, ok := meta.Other["library"]; !ok {
		t.Fatalf("other should contain library: %v", meta.Other)
	}
}

func TestGetMetaReflectsGlobalConcurrencyAndRateLimit(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	noErr(t, queue.SetGlobalConcurrency(ctx, 5))
	noErr(t, queue.SetGlobalRateLimit(ctx, 10, 1000*time.Millisecond))

	meta := must(queue.GetMeta(ctx))
	mxAssertOptInt64(t, "concurrency", meta.Concurrency, bullmq.Ptr[int64](5))
	mxAssertOptInt64(t, "max", meta.Max, bullmq.Ptr[int64](10))
	mxAssertOptInt64(t, "duration", meta.Duration, bullmq.Ptr[int64](1000))
}

func TestGetMetaReflectsPaused(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	noErr(t, queue.Pause(ctx))
	if meta := must(queue.GetMeta(ctx)); !meta.Paused {
		t.Fatal("meta should be paused")
	}

	noErr(t, queue.Resume(ctx))
	if meta := must(queue.GetMeta(ctx)); meta.Paused {
		t.Fatal("meta should not be paused")
	}
}

func TestIsMaxed(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// No global concurrency configured → never maxed.
	if must(queue.IsMaxed(ctx)) {
		t.Fatal("queue should not be maxed without global concurrency")
	}

	// With a global concurrency of 1 and one job held active, the queue is maxed.
	noErr(t, queue.SetGlobalConcurrency(ctx, 1))
	must(queue.Add(ctx, "hold", map[string]any{}, nil))

	release := make(chan struct{})
	var releaseOnce bool
	doRelease := func() {
		if !releaseOnce {
			releaseOnce = true
			close(release)
		}
	}
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, nil
	}, nil)
	t.Cleanup(doRelease) // runs before the worker cleanup (LIFO)

	// Wait until the job is active (queue becomes maxed).
	eventually(t, 10*time.Second, func() bool { return must(queue.IsMaxed(ctx)) },
		"timed out waiting for queue to become maxed")

	doRelease()
	noErr(t, worker.Close(5*time.Second))
}
