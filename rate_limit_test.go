package bullmq_test

// Rate limiting tests (port of bullmq_rust/tests/rate_limit_test.rs).

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/bullmq"
)

// mxTimestampProcessor returns a processor that sends the time at which each
// job started, plus the receiving channel.
func mxTimestampProcessor() (bullmq.Processor, chan time.Time) {
	ch := make(chan time.Time, 10)
	return func(ctx context.Context, job *bullmq.Job) (any, error) {
		ch <- time.Now()
		return nil, nil
	}, ch
}

// mxCollectTimes receives n timestamps, each within timeout.
func mxCollectTimes(t *testing.T, ch <-chan time.Time, n int, timeout time.Duration, msg string) []time.Time {
	t.Helper()
	out := make([]time.Time, 0, n)
	for i := 0; i < n; i++ {
		select {
		case ts := <-ch:
			out = append(out, ts)
		case <-time.After(timeout):
			t.Fatalf("%s (got %d of %d)", msg, len(out), n)
		}
	}
	return out
}

// ═══════════════════════════════════════════════════════════════════════════
// Worker-level rate limiting
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkerRateLimitsJobs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add 4 jobs
	for i := 0; i < 4; i++ {
		must(queue.Add(ctx, "rate-test", map[string]any{"idx": i}, &bullmq.JobOptions{}))
	}

	// Worker with rate limit: max 2 jobs per 1000ms
	proc, ch := mxTimestampProcessor()
	worker := newTestWorker(t, name, proc, &bullmq.WorkerOptions{
		Limiter: &bullmq.RateLimiterOptions{Max: 2, Duration: 1000 * time.Millisecond},
	})

	// Collect timestamps of all 4 processed jobs
	timestamps := mxCollectTimes(t, ch, 4, 5*time.Second, "Timed out waiting for job")

	// First 2 should be processed quickly, next 2 should be after ~1s
	gap := timestamps[2].Sub(timestamps[1])

	// The gap should be at least 800ms (allowing some tolerance)
	if gap < 800*time.Millisecond {
		t.Fatalf("Rate limit gap should be >= 800ms, got %v", gap)
	}

	noErr(t, worker.Close(5*time.Second))
}

func TestWorkerRateLimitDoesNotBlockWithoutLimiter(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add 4 jobs
	for i := 0; i < 4; i++ {
		must(queue.Add(ctx, "no-limit", map[string]any{"idx": i}, &bullmq.JobOptions{}))
	}

	proc, ch := mxTimestampProcessor()
	// No limiter set
	worker := newTestWorker(t, name, proc, nil)

	timestamps := mxCollectTimes(t, ch, 4, 3*time.Second, "Timed out")

	// All 4 should still be processed quickly without rate limiting.
	if total := timestamps[3].Sub(timestamps[0]); total >= 1500*time.Millisecond {
		t.Fatalf("Without limiter, all jobs should process fast, took %v", total)
	}

	noErr(t, worker.Close(5*time.Second))
}

func TestWorkerFetchNextRespectsLimiter(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	for i := 0; i < 3; i++ {
		must(queue.Add(ctx, "fetch-next-rate-test", map[string]any{"idx": i}, &bullmq.JobOptions{}))
	}

	var processed atomic.Int32
	ch := make(chan time.Time, 10)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		processed.Add(1)
		ch <- time.Now()
		return nil, nil
	}, &bullmq.WorkerOptions{
		Limiter: &bullmq.RateLimiterOptions{Max: 1, Duration: 1000 * time.Millisecond},
	})

	timestamps := mxCollectTimes(t, ch, 3, 8*time.Second, "Timed out waiting for rate-limited job")

	firstGap := timestamps[1].Sub(timestamps[0])
	secondGap := timestamps[2].Sub(timestamps[1])
	if firstGap < 800*time.Millisecond {
		t.Fatalf("First fetch-next limiter gap should be >= 800ms, got %v", firstGap)
	}
	if secondGap < 800*time.Millisecond {
		t.Fatalf("Second fetch-next limiter gap should be >= 800ms, got %v", secondGap)
	}
	if n := processed.Load(); n != 3 {
		t.Fatalf("processed = %d, want 3", n)
	}

	noErr(t, worker.Close(5*time.Second))

	counts := must(queue.GetJobCounts(ctx))
	if counts.Completed != 3 {
		t.Fatalf("completed = %d, want 3", counts.Completed)
	}
	if counts.Active != 0 {
		t.Fatalf("active = %d, want 0", counts.Active)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Queue-level rate limiting
// ═══════════════════════════════════════════════════════════════════════════

func TestQueueRateLimitBlocksProcessing(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add a job
	must(queue.Add(ctx, "test", map[string]any{"v": 1}, nil))

	// Set global rate limit (needed for queue.RateLimit to take effect)
	// and then override with queue.RateLimit for 1.5s
	noErr(t, queue.SetGlobalRateLimit(ctx, 1, 60_000*time.Millisecond))
	noErr(t, queue.RateLimit(ctx, 1500*time.Millisecond))

	ch := make(chan struct{}, 5)
	start := time.Now()
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		ch <- struct{}{}
		return nil, nil
	}, nil)

	// Job should be processed after rate limit expires (~1.5s)
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out")
	}

	if elapsed := time.Since(start); elapsed < 1200*time.Millisecond {
		t.Fatalf("Job should be delayed by rate limit, processed after %v", elapsed)
	}

	noErr(t, worker.Close(5*time.Second))
	noErr(t, queue.RemoveGlobalRateLimit(ctx))
}

func TestQueueRemoveRateLimitKey(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	// Set rate limit
	noErr(t, queue.RateLimit(ctx, 60_000*time.Millisecond))

	// Remove it
	if !must(queue.RemoveRateLimitKey(ctx)) {
		t.Fatal("first RemoveRateLimitKey should return true")
	}

	// Second removal should return false
	if must(queue.RemoveRateLimitKey(ctx)) {
		t.Fatal("second RemoveRateLimitKey should return false")
	}
}

func TestQueueSetGlobalRateLimit(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Set global rate limit
	noErr(t, queue.SetGlobalRateLimit(ctx, 5, 2000*time.Millisecond))

	// Add 6 jobs
	for i := 0; i < 6; i++ {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, nil))
	}

	proc, ch := mxTimestampProcessor()
	worker := newTestWorker(t, name, proc, nil)

	// Collect first 6 processing timestamps
	timestamps := mxCollectTimes(t, ch, 6, 8*time.Second, "Timed out")

	// The 6th job should start at least ~2s after the 1st (rate limit window)
	if gap := timestamps[5].Sub(timestamps[0]); gap < 1500*time.Millisecond {
		t.Fatalf("Global rate limit should enforce delay, gap was %v", gap)
	}

	noErr(t, worker.Close(5*time.Second))

	// Cleanup
	noErr(t, queue.RemoveGlobalRateLimit(ctx))
}
