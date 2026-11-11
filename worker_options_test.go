package bullmq_test

// Worker behavioural option tests — MaxStartedAttempts, SkipStalledCheck,
// SkipLockRenewal (port of bullmq_rust/tests/worker_options_test.rs).
//
// Mirrors Node.js `tests/worker.test.ts` (`maxStartedAttempts`) and
// `tests/stalled_jobs.test.ts` (`skipStalledCheck`). Requires a running Redis
// instance at redis://127.0.0.1:6379.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/bullmq"
)

// mxWaitForFailed waits for the next EventFailed worker event and returns its
// error message.
func mxWaitForFailed(t *testing.T, w *bullmq.Worker, timeout time.Duration) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		ev, ok := w.NextEvent(ctx)
		if !ok {
			if ctx.Err() == nil {
				t.Fatal("worker event channel closed")
			}
			t.Fatal("timed out waiting for failed event")
		}
		if ev.Type == bullmq.EventFailed {
			if ev.Err == nil {
				return ""
			}
			return ev.Err.Error()
		}
	}
}

// mxRedelayProcessor moves the job to delayed for d and returns ErrDelayed,
// counting the runs in runs (may be nil).
func mxRedelayProcessor(d time.Duration, runs *atomic.Int32) bullmq.Processor {
	return func(ctx context.Context, job *bullmq.Job) (any, error) {
		if runs != nil {
			runs.Add(1)
		}
		if err := job.MoveToDelayed(ctx, time.Now().Add(d)); err != nil {
			return nil, err
		}
		return nil, bullmq.ErrDelayed
	}
}

func TestMaxStartedAttemptsFailsAfterLimit(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{}, nil))

	// The processor moves the job to delayed on its first run, so the worker
	// picks it up a second time. On the second pickup attemptsStarted == 2 which
	// exceeds maxStartedAttempts (1), so the job fails before processing.
	worker := newTestWorker(t, name, mxRedelayProcessor(150*time.Millisecond, nil),
		&bullmq.WorkerOptions{MaxStartedAttempts: bullmq.Ptr(1)})

	if reason := mxWaitForFailed(t, worker, 15*time.Second); reason != "job started more than allowable limit" {
		t.Fatalf("reason = %q", reason)
	}

	noErr(t, worker.Close(5*time.Second))
}

func TestMaxStartedAttemptsZeroFailsOnFirstPickup(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{}, nil))

	// With a limit of 0, the very first pickup (attemptsStarted == 1) already
	// exceeds it, so the processor must never run.
	//
	// NOTE: Rust uses `max_started_attempts: Some(0)`. The Go option is a plain
	// int where 0 means "disabled", so Some(0) cannot be expressed; this port
	// passes 0 as the faithful translation (see report: API gap).
	var ran atomic.Int32
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		ran.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{MaxStartedAttempts: bullmq.Ptr(0)})

	if reason := mxWaitForFailed(t, worker, 15*time.Second); reason != "job started more than allowable limit" {
		t.Fatalf("reason = %q", reason)
	}
	if n := ran.Load(); n != 0 {
		t.Fatalf("processor must not run, ran %d times", n)
	}

	noErr(t, worker.Close(5*time.Second))
}

func TestMaxStartedAttemptsRunsUpToLimitThenFails(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{}, nil))

	// With a limit of 2, the job may start twice (attemptsStarted 1 and 2). The
	// processor re-delays itself each run; the third pickup (attemptsStarted 3)
	// exceeds the limit and fails before running.
	var runs atomic.Int32
	worker := newTestWorker(t, name, mxRedelayProcessor(100*time.Millisecond, &runs),
		&bullmq.WorkerOptions{MaxStartedAttempts: bullmq.Ptr(2)})

	if reason := mxWaitForFailed(t, worker, 20*time.Second); reason != "job started more than allowable limit" {
		t.Fatalf("reason = %q", reason)
	}
	if n := runs.Load(); n != 2 {
		t.Fatalf("processor should have run exactly up to the limit, ran %d times", n)
	}

	noErr(t, worker.Close(5*time.Second))
}

func TestSkipStalledCheckProcessesAllJobs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const concurrency = 4
	for i := 0; i < concurrency; i++ {
		must(queue.Add(ctx, "test", map[string]any{"i": i}, nil))
	}

	var completed atomic.Int32
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		time.Sleep(400 * time.Millisecond)
		completed.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{
		Concurrency:      concurrency,
		StalledInterval:  50 * time.Millisecond,
		SkipStalledCheck: true,
	})

	eventually(t, 15*time.Second, func() bool { return completed.Load() >= concurrency },
		"timed out waiting for all jobs to complete")

	noErr(t, worker.Close(5*time.Second))
}

func TestSkipLockRenewalStillProcesses(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		// Longer than the default lock_renew_time but well within lock_duration.
		time.Sleep(300 * time.Millisecond)
		return nil, nil
	}, &bullmq.WorkerOptions{
		LockDuration:    30_000 * time.Millisecond,
		SkipLockRenewal: true,
	})

	waitForEvent(t, worker, 15*time.Second, eventType(bullmq.EventCompleted))

	noErr(t, worker.Close(5*time.Second))
}
