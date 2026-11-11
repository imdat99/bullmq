package bullmq_test

// Stalled jobs detection and retry/backoff tests
// (port of bullmq_rust/tests/stalled_jobs_test.rs).

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/bullmq"
)

// mxStallingProcessor returns a processor that "stalls": it holds the job for
// 60s (like the Rust test's sleep) and ignores cancellation, so that closing
// the worker with a zero timeout leaves the job active with an expiring lock,
// as a crashed process would. release unblocks it early; it is registered as
// a cleanup so no processor goroutine outlives the test.
func mxStallingProcessor(t *testing.T) (bullmq.Processor, func()) {
	release := make(chan struct{})
	var once sync.Once
	releaseFn := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseFn)
	return func(ctx context.Context, job *bullmq.Job) (any, error) {
		select {
		case <-release:
		case <-time.After(60 * time.Second):
		}
		return nil, nil
	}, releaseFn
}

// ═══════════════════════════════════════════════════════════════════════════
// Stalled Jobs
// ═══════════════════════════════════════════════════════════════════════════

func TestStalledJobsMovedToWait(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	for i := 0; i < 4; i++ {
		must(queue.Add(ctx, "test", map[string]any{"bar": "baz"}, nil))
	}

	// Worker 1: picks up jobs but "stalls" (very long processing, short lock)
	processor1, release := mxStallingProcessor(t)
	worker1 := newTestWorker(t, name, processor1, &bullmq.WorkerOptions{
		Concurrency:     4,
		LockDuration:    500 * time.Millisecond,
		StalledInterval: 100 * time.Millisecond,
	})

	// Wait for all jobs to be active
	time.Sleep(300 * time.Millisecond)

	// Force close worker1 (simulates crash)
	noErr(t, worker1.Close(0))

	// Worker 2: picks up stalled jobs and processes them
	ch := make(chan struct{}, 10)
	worker2 := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		ch <- struct{}{}
		return nil, nil
	}, &bullmq.WorkerOptions{
		Concurrency:     4,
		StalledInterval: 100 * time.Millisecond,
	})

	// All 4 stalled jobs should be processed by worker2
	count := 0
	timeout := time.After(10 * time.Second)
	for count < 4 {
		select {
		case <-ch:
			count++
		case <-timeout:
			t.Fatalf("timeout waiting for stalled jobs to be reprocessed (%d of 4)", count)
		}
	}
	if count != 4 {
		t.Fatalf("count = %d, want 4", count)
	}

	release()
	noErr(t, worker2.Close(5*time.Second))
}

// The Rust test is #[ignore]d ("requires stalled event emission feature");
// the Go worker emits EventStalled, so the test runs here.
func TestStalledEventEmitted(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"bar": "baz"}, nil))

	// Worker 1: picks up the job and stalls
	processor1, release := mxStallingProcessor(t)
	worker1 := newTestWorker(t, name, processor1, &bullmq.WorkerOptions{
		LockDuration:    500 * time.Millisecond,
		StalledInterval: 100 * time.Millisecond,
	})

	// Wait for job to be active
	time.Sleep(200 * time.Millisecond)
	noErr(t, worker1.Close(0))

	// Worker 2: should emit stalled event
	worker2 := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	}, &bullmq.WorkerOptions{StalledInterval: 100 * time.Millisecond})

	waitForEvent(t, worker2, 5*time.Second, eventType(bullmq.EventStalled))

	release()
	noErr(t, worker2.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Retry / Backoff
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkerRetriesWithFixedBackoff(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.FixedBackoff(200 * time.Millisecond),
	}))

	var attemptCount atomic.Int32
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		attempt := attemptCount.Add(1)
		if attempt < 3 {
			return nil, fmt.Errorf("fail attempt %d", attempt)
		}
		return "success", nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	waitForEvent(t, worker, 10*time.Second, eventType(bullmq.EventCompleted))

	if n := attemptCount.Load(); n != 3 {
		t.Fatalf("attempts = %d, want 3", n)
	}

	noErr(t, worker.Close(5*time.Second))
}

func TestWorkerMovesToFailedAfterMaxAttempts(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{
		Attempts: 2,
		Backoff:  bullmq.FixedBackoff(100 * time.Millisecond),
	}))
	jobID := job.ID

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, errors.New("always fails")
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Wait for final failed event (after all attempts exhausted)
	failedCount := 0
	waitForEvent(t, worker, 10*time.Second, func(ev bullmq.WorkerEvent) bool {
		if ev.Type == bullmq.EventFailed {
			failedCount++
		}
		return failedCount >= 2
	})

	time.Sleep(200 * time.Millisecond)

	fetched := must(queue.GetJob(ctx, jobID))
	if fetched == nil {
		t.Fatal("job missing")
	}
	if fetched.AttemptsMade != 2 {
		t.Fatalf("attemptsMade = %d, want 2", fetched.AttemptsMade)
	}

	if state := must(queue.GetJobState(ctx, jobID)); state != bullmq.StateFailed {
		t.Fatalf("state = %s, want failed", state)
	}

	noErr(t, worker.Close(5*time.Second))
}
