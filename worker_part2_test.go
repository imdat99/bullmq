package bullmq_test

// Port of bullmq_rust/tests/worker_test.rs, tests starting in lines 3532-7100.

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/imdat99/bullmq"
)

// ── helpers (prefix w2) ─────────────────────────────────────────────────────

// w2Send sends v on ch unless ctx is done first, so a processor never blocks
// forever on a channel nobody reads any more (mirrors a Rust mpsc send).
func w2Send[T any](ctx context.Context, ch chan<- T, v T) {
	select {
	case ch <- v:
	case <-ctx.Done():
	}
}

// w2Recv waits up to timeout for a value on ch and fails the test otherwise.
func w2Recv[T any](t testing.TB, ch <-chan T, timeout time.Duration) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(timeout):
		t.Fatalf("timeout after %s waiting on channel", timeout)
	}
	var zero T
	return zero
}

// w2TryRecv reports whether a value is immediately available on ch.
func w2TryRecv[T any](ch <-chan T) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// w2WaitFailed waits until n Failed events were received.
func w2WaitFailed(t testing.TB, w *bullmq.Worker, n int, timeout time.Duration) {
	t.Helper()
	count := 0
	waitForEvent(t, w, timeout, func(e bullmq.WorkerEvent) bool {
		if e.Type == bullmq.EventFailed {
			count++
		}
		return count >= n
	})
}

// w2Null is a processor returning null.
func w2Null(context.Context, *bullmq.Job) (any, error) { return nil, nil }

// w2Data decodes the job data into a generic map.
func w2Data(t testing.TB, job *bullmq.Job) map[string]any {
	var m map[string]any
	if err := job.DecodeData(&m); err != nil {
		t.Errorf("decode data: %v", err)
	}
	return m
}

// w2Sleep sleeps d.
func w2Sleep(d time.Duration) { time.Sleep(d) }

// ── Retries ─────────────────────────────────────────────────────────────────

func TestAttemptsMadeIsCorrect(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.FixedBackoff(50 * time.Millisecond),
	}))

	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		return nil, errors.New("fail")
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	w2WaitFailed(t, worker, 3, 10*time.Second)
	w2Sleep(200 * time.Millisecond)

	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched.AttemptsMade != 3 {
		t.Fatalf("attempts_made = %d, want 3", fetched.AttemptsMade)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Validation Tests
// ═══════════════════════════════════════════════════════════════════════════

// The Go API maps a zero option to its default, so the Rust "0" invalid
// values are expressed with the closest invalid Go value (a negative one).
func w2ExpectInvalid(t *testing.T, queueName string, o bullmq.WorkerOptions, msg string) {
	t.Helper()
	o.Connection = testConnection()
	w, err := bullmq.NewWorker(context.Background(), queueName, w2Null, &o)
	if err == nil {
		_ = w.Close(5 * time.Second)
		t.Fatalf("expected error containing %q", msg)
	}
	if !errors.Is(err, bullmq.ErrInvalidConfig) || !strings.Contains(err.Error(), msg) {
		t.Fatalf("got: %v", err)
	}
}

func TestConcurrencyCannotBeZero(t *testing.T) {
	w2ExpectInvalid(t, testQueueName(), bullmq.WorkerOptions{Concurrency: -1},
		"concurrency must be a finite number greater than 0")
}

func TestQueueNameCannotContainColon(t *testing.T) {
	w2ExpectInvalid(t, "invalid:queue", bullmq.WorkerOptions{}, "Queue name cannot contain :")
}

func TestStalledIntervalCannotBeZero(t *testing.T) {
	w2ExpectInvalid(t, testQueueName(), bullmq.WorkerOptions{StalledInterval: -1},
		"stalledInterval must be greater than 0")
}

func TestDrainDelayCannotBeZero(t *testing.T) {
	w2ExpectInvalid(t, testQueueName(), bullmq.WorkerOptions{DrainDelay: -1},
		"drainDelay must be greater than 0")
}

// ═══════════════════════════════════════════════════════════════════════════
// Worker Name
// ═══════════════════════════════════════════════════════════════════════════

func TestSetsWorkerNameOnJob(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	done := make(chan struct{}, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, _ *bullmq.Job) (any, error) {
		w2Send(ctx, done, struct{}{})
		return nil, nil
	}, &bullmq.WorkerOptions{Name: "foobar"})

	w2Recv(t, done, 5*time.Second)

	// Give time for job to be moved to completed
	w2Sleep(200 * time.Millisecond)

	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched.ProcessedBy != "foobar" {
		t.Fatalf("processedBy = %q, want foobar", fetched.ProcessedBy)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Concurrency Getter
// ═══════════════════════════════════════════════════════════════════════════

func TestConcurrencyGetter(t *testing.T) {
	worker := newTestWorker(t, testQueueName(), w2Null, &bullmq.WorkerOptions{
		ManualStart: true,
		Concurrency: 100,
	})
	if got := worker.Concurrency(); got != 100 {
		t.Fatalf("concurrency = %d, want 100", got)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// AutoRun / Run
// ═══════════════════════════════════════════════════════════════════════════

func TestProcessesJobsWithAutorunFalseThenRun(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const numJobs = 10
	for i := 1; i <= numJobs; i++ {
		must(queue.Add(ctx, "test", map[string]any{"num": i}, nil))
	}

	ch := make(chan int, 16)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		var d struct{ Num int }
		if err := job.DecodeData(&d); err != nil {
			return nil, err
		}
		w2Send(ctx, ch, d.Num)
		return nil, nil
	}, &bullmq.WorkerOptions{ManualStart: true})

	// Worker should not process until run() is called
	w2Sleep(200 * time.Millisecond)
	if w2TryRecv(ch) {
		t.Fatal("worker processed a job before Run")
	}

	noErr(t, worker.Run())

	// Now collect all processed jobs
	count := 0
	deadline := time.After(10 * time.Second)
	for count < numJobs {
		select {
		case <-ch:
			count++
		case <-deadline:
			t.Fatalf("timeout: processed %d/%d", count, numJobs)
		}
	}
	if count != numJobs {
		t.Fatalf("count = %d", count)
	}
	noErr(t, worker.Close(5*time.Second))
}

func TestRunThrowsWhenAlreadyRunning(t *testing.T) {
	worker := newTestWorker(t, testQueueName(), w2Null, nil)

	err := worker.Run()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "Worker is already running") {
		t.Fatalf("got: %v", err)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Close During Processing
// ═══════════════════════════════════════════════════════════════════════════

func TestCloseWaitsForJobToComplete(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	done := make(chan struct{}, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, _ *bullmq.Job) (any, error) {
		// Simulate slow processing
		time.Sleep(500 * time.Millisecond)
		w2Send(ctx, done, struct{}{})
		return nil, nil
	}, nil)

	// Wait for processing to start
	w2Sleep(100 * time.Millisecond)

	// Close with enough timeout to allow job to finish
	noErr(t, worker.Close(5*time.Second))

	// Job should have completed (signal received)
	if !w2TryRecv(done) {
		t.Fatal("Job should have completed before close returned")
	}

	// Verify job is completed
	counts := must(queue.GetJobCounts(ctx))
	if counts.Completed != 1 || counts.Active != 0 {
		t.Fatalf("counts = %+v", counts)
	}
}

func TestCloseAllowsJobThatFailsToFinish(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		time.Sleep(500 * time.Millisecond)
		return nil, bullmq.NewUnrecoverableError("Job Failed")
	}, nil)

	// Wait for processing to start
	w2Sleep(100 * time.Millisecond)

	// Close with enough timeout
	noErr(t, worker.Close(5*time.Second))

	// Verify job is failed
	counts := must(queue.GetJobCounts(ctx))
	if counts.Failed != 1 || counts.Active != 0 {
		t.Fatalf("counts = %+v", counts)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Pause Behavior
// ═══════════════════════════════════════════════════════════════════════════

func TestPauseDoesNotFetchNewJobs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	ch := make(chan int, 16)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		var d struct{ Num int }
		if err := job.DecodeData(&d); err != nil {
			return nil, err
		}
		time.Sleep(100 * time.Millisecond)
		w2Send(ctx, ch, d.Num)
		return nil, nil
	}, nil)

	// Add a job that will be processed
	must(queue.Add(ctx, "test", map[string]any{"num": 1}, nil))

	// Wait until the first job is active, then pause while it is still running.
	deadline := time.Now().Add(5 * time.Second)
	for worker.ActiveCount() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for first job to become active")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Pause the worker before the current job completes.
	worker.Pause()
	if !worker.IsPaused() {
		t.Fatal("worker should be paused")
	}

	// First job should still complete.
	w2Recv(t, ch, 5*time.Second)

	// Add another job while paused
	must(queue.Add(ctx, "test", map[string]any{"num": 2}, nil))

	// Wait a bit - paused worker should not pick up the job
	w2Sleep(500 * time.Millisecond)
	if w2TryRecv(ch) {
		t.Fatal("Paused worker should not process new jobs")
	}

	// Resume and it should process
	worker.Resume()

	if val := w2Recv(t, ch, 5*time.Second); val != 2 {
		t.Fatalf("val = %d, want 2", val)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Retries Exhausted
// ═══════════════════════════════════════════════════════════════════════════

func TestJobOnlyExecutesOnceWhenAttemptsIs1(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{Attempts: 1}))

	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		return nil, errors.New("failed")
	}, nil)

	// Wait for failed event
	w2WaitFailed(t, worker, 1, 5*time.Second)
	w2Sleep(200 * time.Millisecond)

	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched.AttemptsMade != 1 {
		t.Fatalf("attempts_made = %d, want 1", fetched.AttemptsMade)
	}

	// Verify state is failed (no retry since attempts=1)
	counts := must(queue.GetJobCounts(ctx))
	if counts.Failed != 1 {
		t.Fatalf("failed = %d, want 1", counts.Failed)
	}
	noErr(t, worker.Close(5*time.Second))
}

func TestDoesNotRetryMoreThanAttemptsFixedBackoff(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.FixedBackoff(50 * time.Millisecond),
	}))

	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		return nil, errors.New("Not yet!")
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Wait for 3 failed events (retries exhausted)
	w2WaitFailed(t, worker, 3, 10*time.Second)
	w2Sleep(200 * time.Millisecond)

	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched.AttemptsMade != 3 {
		t.Fatalf("attempts_made = %d, want 3", fetched.AttemptsMade)
	}

	// State should be failed
	counts := must(queue.GetJobCounts(ctx))
	if counts.Failed != 1 {
		t.Fatalf("failed = %d, want 1", counts.Failed)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Retry with Fixed Backoff Timing
// ═══════════════════════════════════════════════════════════════════════════

func TestRetryRespectsFixedBackoffDelay(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.FixedBackoff(500 * time.Millisecond),
	}))

	done := make(chan struct{}, 1)
	var attempt atomic.Int32
	start := time.Now()
	worker := newTestWorker(t, name, func(ctx context.Context, _ *bullmq.Job) (any, error) {
		if attempt.Add(1)-1 < 2 {
			return nil, errors.New("Not yet!")
		}
		w2Send(ctx, done, struct{}{})
		return nil, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	w2Recv(t, done, 10*time.Second)

	elapsed := time.Since(start)
	// Should have waited at least 1000ms total (2 retries * 500ms each)
	if elapsed < 900*time.Millisecond {
		t.Fatalf("Expected at least 900ms, got %s", elapsed)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Unrecoverable Error Stops Retries Immediately
// ═══════════════════════════════════════════════════════════════════════════

func TestUnrecoverableStopsRetriesWithBackoff(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.FixedBackoff(500 * time.Millisecond),
	}))

	var attempt atomic.Int32
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		if attempt.Add(1)-1 < 1 {
			return nil, errors.New("Not yet!")
		}
		return nil, bullmq.NewUnrecoverableError("Unrecoverable")
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Wait for 2 failed events (first retry + unrecoverable)
	w2WaitFailed(t, worker, 2, 10*time.Second)
	w2Sleep(300 * time.Millisecond)

	fetched := must(queue.GetJob(ctx, job.ID))
	// Only 2 attempts made (not 3), because unrecoverable stopped retries
	if fetched.AttemptsMade != 2 {
		t.Fatalf("attempts_made = %d, want 2", fetched.AttemptsMade)
	}
	if !strings.Contains(fetched.FailedReason, "Unrecoverable") {
		t.Fatalf("failedReason = %q", fetched.FailedReason)
	}

	counts := must(queue.GetJobCounts(ctx))
	if counts.Failed != 1 {
		t.Fatalf("failed = %d, want 1", counts.Failed)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Process Multiple Jobs with Concurrency > 1
// ═══════════════════════════════════════════════════════════════════════════

func TestConcurrency4ProcessesJobsInParallel(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const totalJobs = 8
	for range totalJobs {
		must(queue.Add(ctx, "test", map[string]any{}, nil))
	}

	var maxConcurrent, current atomic.Int32
	done := make(chan struct{}, 16)
	worker := newTestWorker(t, name, func(ctx context.Context, _ *bullmq.Job) (any, error) {
		c := current.Add(1)
		// Update max
		for {
			m := maxConcurrent.Load()
			if c <= m || maxConcurrent.CompareAndSwap(m, c) {
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
		current.Add(-1)
		w2Send(ctx, done, struct{}{})
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: 4})

	// Wait for all jobs to complete
	deadline := time.After(15 * time.Second)
	for completed := 0; completed < totalJobs; completed++ {
		select {
		case <-done:
		case <-deadline:
			t.Fatalf("timeout: %d/%d completed", completed, totalJobs)
		}
	}

	observedMax := maxConcurrent.Load()
	// Should have had at most 4 concurrent jobs
	if observedMax > 4 {
		t.Fatalf("Max concurrent was %d, expected <= 4", observedMax)
	}
	// Should have had more than 1 concurrent (proves parallelism)
	if observedMax <= 1 {
		t.Fatalf("Max concurrent was %d, expected > 1 (parallel)", observedMax)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Drained Event
// ═══════════════════════════════════════════════════════════════════════════

func TestDrainedEventEmitted(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{}, nil))

	worker := newTestWorker(t, name, w2Null, nil)

	// After the job completes, we should get a Drained event
	gotCompleted, gotDrained := false, false
	waitForEvent(t, worker, 10*time.Second, func(e bullmq.WorkerEvent) bool {
		switch e.Type {
		case bullmq.EventCompleted:
			gotCompleted = true
		case bullmq.EventDrained:
			if gotCompleted {
				gotDrained = true
				return true
			}
		}
		return false
	})
	if !gotDrained {
		t.Fatal("no drained event")
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Multiple Workers on Same Queue
// ═══════════════════════════════════════════════════════════════════════════

func TestMultipleWorkersProcessAllJobs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const totalJobs = 20
	for i := range totalJobs {
		must(queue.Add(ctx, "test", map[string]any{"i": i}, nil))
	}

	var count atomic.Int32
	makeProcessor := func(count *atomic.Int32) bullmq.Processor {
		return func(context.Context, *bullmq.Job) (any, error) {
			count.Add(1)
			time.Sleep(50 * time.Millisecond)
			return nil, nil
		}
	}

	worker1 := newTestWorker(t, name, makeProcessor(&count), &bullmq.WorkerOptions{Concurrency: 2})
	worker2 := newTestWorker(t, name, makeProcessor(&count), &bullmq.WorkerOptions{Concurrency: 2})

	// Wait until all jobs are processed
	deadline := time.Now().Add(15 * time.Second)
	for count.Load() < totalJobs {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %d/%d processed", count.Load(), totalJobs)
		}
		time.Sleep(100 * time.Millisecond)
	}

	if got := count.Load(); got != totalJobs {
		t.Fatalf("count = %d, want %d", got, totalJobs)
	}
	noErr(t, worker1.Close(5*time.Second))
	noErr(t, worker2.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Error Event from Worker
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkerEmitsErrorEventOnFailure(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{}, nil))

	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		return nil, errors.New("something went wrong")
	}, nil)

	// Should receive a Failed event with the error message
	ev := waitForEvent(t, worker, 5*time.Second, eventType(bullmq.EventFailed))
	if ev.Err == nil || !strings.Contains(ev.Err.Error(), "something went wrong") {
		t.Fatalf("failed event error = %v", ev.Err)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Delayed Job with Immediate Retry (retryJob script)
// ═══════════════════════════════════════════════════════════════════════════

func TestAutoRetryWithoutBackoffRetriesImmediately(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// No backoff — should retry immediately
	must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{Attempts: 3}))

	var attempt atomic.Int32
	done := make(chan struct{}, 1)
	start := time.Now()
	worker := newTestWorker(t, name, func(ctx context.Context, _ *bullmq.Job) (any, error) {
		if attempt.Add(1)-1 < 2 {
			return nil, errors.New("Not yet!")
		}
		w2Send(ctx, done, struct{}{})
		return nil, nil
	}, nil)

	w2Recv(t, done, 10*time.Second)

	// Without backoff, retries should be fast (< 3 seconds total)
	if elapsed := time.Since(start); elapsed >= 3*time.Second {
		t.Fatalf("Expected fast retries without backoff, got %s", elapsed)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Return Value Types
// ═══════════════════════════════════════════════════════════════════════════

// w2ReturnValueStored runs one job returning ret and returns the decoded
// stored return value.
func w2ReturnValueStored(t *testing.T, ret any) any {
	t.Helper()
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{}, nil))

	done := make(chan struct{}, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, _ *bullmq.Job) (any, error) {
		w2Send(ctx, done, struct{}{})
		return ret, nil
	}, nil)

	w2Recv(t, done, 5*time.Second)
	w2Sleep(200 * time.Millisecond)

	fetched := must(queue.GetJob(ctx, job.ID))
	var rv any
	if err := fetched.DecodeReturnValue(&rv); err != nil {
		t.Fatalf("decode returnvalue %q: %v", fetched.ReturnValue, err)
	}
	noErr(t, worker.Close(5*time.Second))
	return rv
}

func TestReturnValueObjectStored(t *testing.T) {
	rv, ok := w2ReturnValueStored(t, map[string]any{"status": "ok", "count": 42}).(map[string]any)
	if !ok || rv["status"] != "ok" || rv["count"] != float64(42) {
		t.Fatalf("returnvalue = %#v", rv)
	}
}

func TestReturnValueArrayStored(t *testing.T) {
	rv := w2ReturnValueStored(t, []int{1, 2, 3})
	if !reflect.DeepEqual(rv, []any{float64(1), float64(2), float64(3)}) {
		t.Fatalf("returnvalue = %#v", rv)
	}
}

func TestReturnValueNumberStored(t *testing.T) {
	rv := w2ReturnValueStored(t, 123)
	if rv != float64(123) {
		t.Fatalf("returnvalue = %#v", rv)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Worker Does Not Process Jobs From Other Queue
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkerOnlyProcessesItsOwnQueue(t *testing.T) {
	ctx := context.Background()
	name1 := testQueueName()
	name2 := testQueueName()
	queue1 := newTestQueue(t, name1, nil)
	queue2 := newTestQueue(t, name2, nil)

	// Add jobs to both queues
	must(queue1.Add(ctx, "test", map[string]any{"queue": 1}, nil))
	must(queue2.Add(ctx, "test", map[string]any{"queue": 2}, nil))

	ch := make(chan int, 4)
	// Worker only on queue1
	worker := newTestWorker(t, name1, func(ctx context.Context, job *bullmq.Job) (any, error) {
		var d struct{ Queue int }
		if err := job.DecodeData(&d); err != nil {
			return nil, err
		}
		w2Send(ctx, ch, d.Queue)
		return nil, nil
	}, nil)

	if val := w2Recv(t, ch, 5*time.Second); val != 1 {
		t.Fatalf("val = %d, want 1", val)
	}

	// Should not receive anything from queue2
	w2Sleep(500 * time.Millisecond)
	if w2TryRecv(ch) {
		t.Fatal("worker processed a job of another queue")
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Job Data Integrity
// ═══════════════════════════════════════════════════════════════════════════

func TestJobDataAvailableInProcessor(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"name": "Alice", "age": 30, "tags": []string{"a", "b"}}, nil))

	ch := make(chan map[string]any, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		w2Send(ctx, ch, w2Data(t, job))
		return nil, nil
	}, nil)

	data := w2Recv(t, ch, 5*time.Second)
	if data["name"] != "Alice" || data["age"] != float64(30) ||
		!reflect.DeepEqual(data["tags"], []any{"a", "b"}) {
		t.Fatalf("data = %#v", data)
	}
	noErr(t, worker.Close(5*time.Second))
}

func TestJobNameAvailableInProcessor(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "my-job-name", map[string]any{}, nil))

	ch := make(chan string, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		w2Send(ctx, ch, job.Name)
		return nil, nil
	}, nil)

	if jobName := w2Recv(t, ch, 5*time.Second); jobName != "my-job-name" {
		t.Fatalf("job name = %q", jobName)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Delay Property Updated on Backoff
// ═══════════════════════════════════════════════════════════════════════════

func TestDelayPropertyUpdatedOnFixedBackoff(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"bar": "baz"}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.FixedBackoff(300 * time.Millisecond),
	}))

	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		return nil, errors.New("error")
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Wait for first failed event
	w2WaitFailed(t, worker, 1, 5*time.Second)
	w2Sleep(100 * time.Millisecond)

	// After first failure with backoff, delay should be set to 300
	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched.Delay != 300*time.Millisecond {
		t.Fatalf("delay = %s, want 300ms", fetched.Delay)
	}
	noErr(t, worker.Close(5*time.Second))
}

func TestDelayPropertyUpdatedOnExponentialBackoff(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"bar": "baz"}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.ExponentialBackoff(200 * time.Millisecond),
	}))

	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		return nil, errors.New("error")
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Wait for second failed event (to check exponential growth)
	w2WaitFailed(t, worker, 2, 10*time.Second)
	w2Sleep(100 * time.Millisecond)

	// After second failure with exponential backoff:
	// First retry delay: 2^0 * 200 = 200
	// Second retry delay: 2^1 * 200 = 400
	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched.Delay < 400*time.Millisecond {
		t.Fatalf("Expected delay >= 400, got %s", fetched.Delay)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Token Deleted After Moving to Delayed
// ═══════════════════════════════════════════════════════════════════════════

func TestTokenDeletedAfterMoveToDelayed(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"bar": "baz"}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.FixedBackoff(100 * time.Millisecond),
	}))

	done := make(chan struct{}, 1)
	var attempt atomic.Int32
	worker := newTestWorker(t, name, func(ctx context.Context, _ *bullmq.Job) (any, error) {
		if attempt.Add(1)-1 < 2 {
			return nil, errors.New("error")
		}
		w2Send(ctx, done, struct{}{})
		return nil, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second, LockDuration: 10 * time.Second})

	// Wait for completion
	w2Recv(t, done, 10*time.Second)
	w2Sleep(100 * time.Millisecond)

	// After completion, lock should be gone
	lockKey := fmt.Sprintf("bull:%s:%s:lock", name, job.ID)
	_, err := rawClient(t).Get(ctx, lockKey).Result()
	if !errors.Is(err, redis.Nil) {
		t.Fatalf("Lock should be deleted after completion (err=%v)", err)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Promotes Delayed Jobs First (during retry)
// ═══════════════════════════════════════════════════════════════════════════

func TestPromotesDelayedJobsFirst(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add job 1 with attempts (will fail once then succeed)
	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{Attempts: 2}))

	// Add 3 delayed jobs
	for range 3 {
		must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{Delay: 200 * time.Millisecond}))
	}

	var mu sync.Mutex
	var order []string
	var attempt atomic.Int32
	done := make(chan struct{}, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		time.Sleep(50 * time.Millisecond)
		a := attempt.Add(1) - 1
		mu.Lock()
		order = append(order, job.ID)
		mu.Unlock()

		// First attempt on job 1 fails
		if job.ID == "1" && a == 0 {
			return nil, errors.New("Not yet!")
		}

		// After 4 completions (job1 retry + 3 delayed), we're done
		mu.Lock()
		n := len(order)
		mu.Unlock()
		if n >= 5 {
			w2Send(ctx, done, struct{}{})
		}
		return nil, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	w2Recv(t, done, 10*time.Second)

	// The delayed jobs should have been promoted and processed
	// Job 1 should appear twice (first attempt + retry)
	mu.Lock()
	finalOrder := slices.Clone(order)
	mu.Unlock()
	if len(finalOrder) < 5 {
		t.Fatalf("Expected at least 5 entries, got %d", len(finalOrder))
	}
	// First entry should be job 1 (initial attempt)
	if finalOrder[0] != "1" {
		t.Fatalf("order = %v", finalOrder)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Fixed Backoff Timing Verification
// ═══════════════════════════════════════════════════════════════════════════

func TestFixedBackoffRespectsDelayTiming(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{
		Attempts: 4,
		Backoff:  bullmq.FixedBackoff(300 * time.Millisecond),
	}))

	done := make(chan struct{}, 1)
	var attempt atomic.Int32
	start := time.Now()
	worker := newTestWorker(t, name, func(ctx context.Context, _ *bullmq.Job) (any, error) {
		if attempt.Add(1)-1 < 3 {
			return nil, errors.New("error")
		}
		w2Send(ctx, done, struct{}{})
		return nil, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	w2Recv(t, done, 15*time.Second)

	elapsed := time.Since(start)
	// 3 retries * 300ms = 900ms minimum
	if elapsed < 800*time.Millisecond {
		t.Fatalf("Expected at least 800ms for 3 retries at 300ms, got %s", elapsed)
	}
	// Should not take too long
	if elapsed >= 5*time.Second {
		t.Fatalf("Took too long: %s", elapsed)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Exponential Backoff Timing Verification
// ═══════════════════════════════════════════════════════════════════════════

func TestExponentialBackoffTiming(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.ExponentialBackoff(200 * time.Millisecond),
	}))

	done := make(chan struct{}, 1)
	var attempt atomic.Int32
	start := time.Now()
	worker := newTestWorker(t, name, func(ctx context.Context, _ *bullmq.Job) (any, error) {
		if attempt.Add(1)-1 < 2 {
			return nil, errors.New("Not yet!")
		}
		w2Send(ctx, done, struct{}{})
		return nil, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	w2Recv(t, done, 10*time.Second)

	elapsed := time.Since(start)
	// Exponential: 200ms + 400ms = 600ms minimum (2^0 * 200 + 2^1 * 200)
	if elapsed < 500*time.Millisecond {
		t.Fatalf("Expected at least 500ms, got %s", elapsed)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Prioritized Job Processing Without Delay
// ═══════════════════════════════════════════════════════════════════════════

func TestProcessPrioritizedJobWithoutDelay(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add first job with priority 2
	must(queue.Add(ctx, "test1", map[string]any{"p": 2}, &bullmq.JobOptions{Priority: 2}))

	var count atomic.Int32
	done := make(chan struct{}, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		// When processing test1, add another job with same priority
		if job.Name == "test1" {
			q, err := bullmq.NewQueue(ctx, name, &bullmq.QueueOptions{Connection: testConnection()})
			if err != nil {
				t.Errorf("NewQueue: %v", err)
				return nil, err
			}
			_, err = q.Add(ctx, "test2", map[string]any{"p": 2}, &bullmq.JobOptions{Priority: 2})
			_ = q.Close()
			if err != nil {
				t.Errorf("add test2: %v", err)
				return nil, err
			}
		}

		if p := w2Data(t, job)["p"]; p != float64(2) {
			t.Errorf("data.p = %v, want 2", p)
		}

		if count.Add(1) >= 2 {
			w2Send(ctx, done, struct{}{})
		}
		return nil, nil
	}, nil)

	w2Recv(t, done, 5*time.Second)
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Pause Waits for All Concurrent Processing
// ═══════════════════════════════════════════════════════════════════════════

func TestPauseWaitsForConcurrentProcessing(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add 8 jobs
	for range 8 {
		must(queue.Add(ctx, "test", map[string]any{}, nil))
	}

	var finishCount, activeCount atomic.Int32
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		activeCount.Add(1)
		time.Sleep(100 * time.Millisecond)
		activeCount.Add(-1)
		finishCount.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: 4})

	// Wait for first batch to start processing
	w2Sleep(50 * time.Millisecond)

	// Pause while jobs are still being processed
	worker.Pause()

	// Wait for active jobs to finish
	w2Sleep(300 * time.Millisecond)

	// After pause, active count should be 0 (active jobs finished)
	if active := activeCount.Load(); active != 0 {
		t.Fatalf("Expected 0 active jobs after pause, got %d", active)
	}

	// Some jobs finished before pause took full effect
	if finished := finishCount.Load(); finished < 4 {
		t.Fatalf("Expected at least 4 finished before pause, got %d", finished)
	}

	// Resume and let remaining jobs process
	worker.Resume()

	// Wait for all 8 jobs to complete
	eventually(t, 10*time.Second, func() bool { return finishCount.Load() >= 8 }, "8 jobs finished")
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Delay Reset After Retries Exhausted
// ═══════════════════════════════════════════════════════════════════════════

func TestDelayResetToZeroAfterRetriesExhausted(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.FixedBackoff(100 * time.Millisecond),
	}))

	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		return nil, errors.New("error")
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Wait for all 3 failures
	w2WaitFailed(t, worker, 3, 10*time.Second)
	w2Sleep(200 * time.Millisecond)

	// After retries exhausted, delay should be reset to 0
	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched.Delay != 0 {
		t.Fatalf("Delay should be 0 after retries exhausted, got %s", fetched.Delay)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Attempts Made Tracked Correctly Through Retries
// ═══════════════════════════════════════════════════════════════════════════

func TestAttemptsMadeIncrementsOnEachRetry(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{
		Attempts: 4,
		Backoff:  bullmq.FixedBackoff(50 * time.Millisecond),
	}))

	var mu sync.Mutex
	var observed []int
	done := make(chan struct{}, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		mu.Lock()
		observed = append(observed, job.AttemptsMade)
		mu.Unlock()
		if job.AttemptsMade < 3 {
			return nil, errors.New("Not yet!")
		}
		w2Send(ctx, done, struct{}{})
		return nil, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	w2Recv(t, done, 10*time.Second)
	w2Sleep(100 * time.Millisecond)

	mu.Lock()
	attempts := slices.Clone(observed)
	mu.Unlock()
	// attempts_made should be 0, 1, 2, 3 on successive processing
	if !reflect.DeepEqual(attempts, []int{0, 1, 2, 3}) {
		t.Fatalf("observed attempts = %v, want [0 1 2 3]", attempts)
	}

	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched.AttemptsMade != 4 {
		t.Fatalf("attempts_made = %d, want 4", fetched.AttemptsMade)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Worker Events Order (Ready → Completed → Drained)
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkerEventsOrder(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{}, nil))

	worker := newTestWorker(t, name, w2Null, nil)

	var events []string
	waitForEvent(t, worker, 10*time.Second, func(e bullmq.WorkerEvent) bool {
		events = append(events, e.Type.String())
		return e.Type == bullmq.EventDrained
	})

	// Should have Ready first, then Completed, then Drained
	readyIdx := slices.Index(events, "ready")
	completedIdx := slices.Index(events, "completed")
	drainedIdx := slices.Index(events, "drained")
	if readyIdx < 0 || completedIdx < 0 || drainedIdx < 0 {
		t.Fatalf("events = %v", events)
	}
	if readyIdx >= completedIdx || completedIdx >= drainedIdx {
		t.Fatalf("bad order: %v", events)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Failed Job Reason Saved on Retry
// ═══════════════════════════════════════════════════════════════════════════

func TestFailedReasonSavedDuringRetry(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.FixedBackoff(50 * time.Millisecond),
	}))

	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		return nil, errors.New("custom error message")
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Wait for all attempts to fail (job moves to permanently failed)
	w2WaitFailed(t, worker, 3, 10*time.Second)
	w2Sleep(100 * time.Millisecond)

	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched.FailedReason != "custom error message" {
		t.Fatalf("failedReason = %q", fetched.FailedReason)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Worker Does Not Process During Close
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkerDoesNotFetchNewJobsDuringClose(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	var processed atomic.Int32
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		processed.Add(1)
		time.Sleep(200 * time.Millisecond)
		return nil, nil
	}, nil)

	// Add a job and wait for it to start processing
	must(queue.Add(ctx, "test", map[string]any{}, nil))
	w2Sleep(100 * time.Millisecond)

	// Now close and simultaneously add more jobs
	must(queue.Add(ctx, "test2", map[string]any{}, nil))
	must(queue.Add(ctx, "test3", map[string]any{}, nil))

	noErr(t, worker.Close(5*time.Second))

	// Should have processed at most 1 job (the one that started before close)
	if n := processed.Load(); n > 2 {
		t.Fatalf("Expected at most 2 jobs processed during close, got %d", n)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Worker Close Returns After Active Job Completes
// ═══════════════════════════════════════════════════════════════════════════

func TestCloseResolvesAfterActiveJobFinishes(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{}, nil))

	started := make(chan struct{}, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, _ *bullmq.Job) (any, error) {
		w2Send(ctx, started, struct{}{})
		time.Sleep(500 * time.Millisecond)
		return "done", nil
	}, nil)

	// Wait for processing to start
	w2Recv(t, started, 5*time.Second)

	// Close should wait for the active job
	start := time.Now()
	noErr(t, worker.Close(5*time.Second))
	elapsed := time.Since(start)

	// Should have waited for the job (at least ~400ms remaining)
	if elapsed < 300*time.Millisecond {
		t.Fatalf("Close returned too fast: %s", elapsed)
	}

	// Job should be completed
	counts := must(queue.GetJobCounts(ctx))
	if counts.Completed != 1 {
		t.Fatalf("completed = %d, want 1", counts.Completed)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Default Job Options - removeOnComplete
// ═══════════════════════════════════════════════════════════════════════════

func TestDefaultJobOptionsRemoveOnComplete(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{
		DefaultJobOptions: bullmq.JobOptions{RemoveOnComplete: bullmq.RemoveAll()},
	})

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, func(_ context.Context, job *bullmq.Job) (any, error) {
		if foo := w2Data(t, job)["foo"]; foo != "bar" {
			t.Errorf("data.foo = %v", foo)
		}
		return "done", nil
	}, nil)

	waitForEvent(t, worker, 5*time.Second, eventType(bullmq.EventCompleted))
	w2Sleep(100 * time.Millisecond)

	// Job should be removed after completion
	if fetched := must(queue.GetJob(ctx, job.ID)); fetched != nil {
		t.Fatalf("Job should have been removed: %+v", fetched)
	}
	if counts := must(queue.GetJobCounts(ctx)); counts.Completed != 0 {
		t.Fatalf("completed = %d, want 0", counts.Completed)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Default Job Options - removeOnFail
// ═══════════════════════════════════════════════════════════════════════════

func TestDefaultJobOptionsRemoveOnFail(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{
		DefaultJobOptions: bullmq.JobOptions{RemoveOnFail: bullmq.RemoveAll()},
	})

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		return nil, errors.New("fail")
	}, nil)

	waitForEvent(t, worker, 5*time.Second, eventType(bullmq.EventFailed))
	w2Sleep(100 * time.Millisecond)

	// Job should be removed after failure
	if fetched := must(queue.GetJob(ctx, job.ID)); fetched != nil {
		t.Fatalf("Job should have been removed: %+v", fetched)
	}
	if counts := must(queue.GetJobCounts(ctx)); counts.Failed != 0 {
		t.Fatalf("failed = %d, want 0", counts.Failed)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Default Job Options - keep count with removeOnComplete
// ═══════════════════════════════════════════════════════════════════════════

func TestDefaultJobOptionsKeepCountRemoveOnComplete(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	const keepJobs = 3
	queue := newTestQueue(t, name, &bullmq.QueueOptions{
		DefaultJobOptions: bullmq.JobOptions{RemoveOnComplete: bullmq.KeepLast(keepJobs)},
	})

	const totalJobs = 9
	for i := range totalJobs {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, nil))
	}

	var completed atomic.Int32
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		completed.Add(1)
		return "done", nil
	}, nil)

	eventually(t, 10*time.Second, func() bool { return completed.Load() >= totalJobs }, "all jobs processed")
	w2Sleep(200 * time.Millisecond)

	if counts := must(queue.GetJobCounts(ctx)); counts.Completed != keepJobs {
		t.Fatalf("completed = %d, want %d", counts.Completed, keepJobs)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Process prioritized jobs with custom jobId
// ═══════════════════════════════════════════════════════════════════════════

func TestProcessPrioritizedJobsCustomId(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test1", map[string]any{"p": 2}, &bullmq.JobOptions{Priority: 2, JobID: "custom1"}))
	must(queue.Add(ctx, "test2", map[string]any{"p": 3}, &bullmq.JobOptions{Priority: 3, JobID: "custom2"}))

	var completed atomic.Int32
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		completed.Add(1)
		return nil, nil
	}, nil)

	eventually(t, 5*time.Second, func() bool { return completed.Load() >= 2 }, "2 jobs processed")

	if counts := must(queue.GetJobCounts(ctx)); counts.Completed != 2 {
		t.Fatalf("completed = %d, want 2", counts.Completed)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Add Bulk processing
// ═══════════════════════════════════════════════════════════════════════════

func TestAddBulkProcessing(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	jobs := make([]bullmq.BulkJob, 0, 5)
	for i := range 5 {
		jobs = append(jobs, bullmq.BulkJob{Name: fmt.Sprintf("job-%d", i), Data: map[string]any{"idx": i}})
	}
	added := must(queue.AddBulk(ctx, jobs))
	if len(added) != 5 {
		t.Fatalf("added %d jobs, want 5", len(added))
	}

	var completed atomic.Int32
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		completed.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: 3})

	eventually(t, 5*time.Second, func() bool { return completed.Load() >= 5 }, "5 jobs processed")

	if counts := must(queue.GetJobCounts(ctx)); counts.Completed != 5 {
		t.Fatalf("completed = %d, want 5", counts.Completed)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Job retries once then succeeds
// ═══════════════════════════════════════════════════════════════════════════

func TestRetriesJobThenSucceeds(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{Attempts: 3}))

	var counter atomic.Int32
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		if counter.Add(1) == 1 {
			return nil, errors.New("fail first time")
		}
		return "success", nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Wait for completion
	waitForEvent(t, worker, 5*time.Second, eventType(bullmq.EventCompleted))

	if n := counter.Load(); n != 2 {
		t.Fatalf("attempts = %d, want 2", n)
	}
	counts := must(queue.GetJobCounts(ctx))
	if counts.Completed != 1 || counts.Failed != 0 {
		t.Fatalf("counts = %+v", counts)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Not leave orphaned job data when limit is less than removable jobs
// ═══════════════════════════════════════════════════════════════════════════

func TestNoOrphanedDataWhenLimitLessThanRemovable(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const totalJobs = 10
	const keepCount = 2
	for i := range totalJobs {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, &bullmq.JobOptions{
			RemoveOnComplete: bullmq.KeepLast(keepCount),
		}))
	}

	var completed atomic.Int32
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		completed.Add(1)
		return "done", nil
	}, nil)

	eventually(t, 10*time.Second, func() bool { return completed.Load() >= totalJobs }, "all jobs processed")
	w2Sleep(200 * time.Millisecond)

	if counts := must(queue.GetJobCounts(ctx)); counts.Completed != keepCount {
		t.Fatalf("completed = %d, want %d", counts.Completed, keepCount)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Keep jobs newer than specified and up to a count fail with removeOnFail
// ═══════════════════════════════════════════════════════════════════════════

func TestKeepJobsAgeAndCountRemoveOnFail(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)
	const keepCount = 5

	const totalJobs = 10
	for i := range totalJobs {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, &bullmq.JobOptions{
			RemoveOnFail: bullmq.KeepJobsPolicy(bullmq.KeepJobs{Age: bullmq.Ptr[int64](7), Count: bullmq.Ptr[int64](keepCount)}),
		}))
	}

	var failed atomic.Int32
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		failed.Add(1)
		return nil, errors.New("intentional")
	}, nil)

	eventually(t, 10*time.Second, func() bool { return failed.Load() >= totalJobs }, "all jobs processed")
	w2Sleep(200 * time.Millisecond)

	if counts := must(queue.GetJobCounts(ctx)); counts.Failed > keepCount {
		t.Fatalf("Expected at most %d failed jobs, got %d", keepCount, counts.Failed)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Retries a job that fails and resets attemptsMade
// ═══════════════════════════════════════════════════════════════════════════

func TestRetriesJobResetsAttemptsMade(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.FixedBackoff(50 * time.Millisecond),
	}))

	var counter atomic.Int32
	worker := newTestWorker(t, name, func(_ context.Context, job *bullmq.Job) (any, error) {
		attempt := counter.Add(1)
		// Fail on first two attempts, succeed on third
		if attempt < 3 {
			return nil, fmt.Errorf("fail attempt %d", attempt)
		}
		// On the final (3rd) attempt, the job's attemptsMade should be 2
		if job.AttemptsMade != 2 {
			t.Errorf("attempts_made should be 2 on third processing, got %d", job.AttemptsMade)
		}
		return "success", nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	waitForEvent(t, worker, 10*time.Second, eventType(bullmq.EventCompleted))

	if n := counter.Load(); n != 3 {
		t.Fatalf("attempts = %d, want 3", n)
	}
	if counts := must(queue.GetJobCounts(ctx)); counts.Completed != 1 {
		t.Fatalf("completed = %d, want 1", counts.Completed)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Add bulk with priority
// ═══════════════════════════════════════════════════════════════════════════

func TestAddBulkWithPriority(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add jobs with different priorities
	added := must(queue.AddBulk(ctx, []bullmq.BulkJob{
		{Name: "low", Data: map[string]any{"priority": "low"}, Opts: &bullmq.JobOptions{Priority: 10}},
		{Name: "high", Data: map[string]any{"priority": "high"}, Opts: &bullmq.JobOptions{Priority: 1}},
		{Name: "medium", Data: map[string]any{"priority": "medium"}, Opts: &bullmq.JobOptions{Priority: 5}},
	}))
	if len(added) != 3 {
		t.Fatalf("added %d jobs, want 3", len(added))
	}

	var mu sync.Mutex
	var order []string
	var completed atomic.Int32
	worker := newTestWorker(t, name, func(_ context.Context, job *bullmq.Job) (any, error) {
		mu.Lock()
		order = append(order, job.Name)
		mu.Unlock()
		completed.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: 1})

	eventually(t, 5*time.Second, func() bool { return completed.Load() >= 3 }, "3 jobs processed")

	mu.Lock()
	processingOrder := slices.Clone(order)
	mu.Unlock()
	// Higher priority (lower number) should be processed first
	if !reflect.DeepEqual(processingOrder, []string{"high", "medium", "low"}) {
		t.Fatalf("order = %v", processingOrder)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Concurrent workers keep locks
// ═══════════════════════════════════════════════════════════════════════════

func TestConcurrentWorkersKeepLocks(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const concurrency = 10
	for i := range concurrency {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, nil))
	}

	var completed atomic.Int32
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		// Hold job for 500ms to test lock maintenance
		time.Sleep(500 * time.Millisecond)
		completed.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: concurrency, LockDuration: time.Second})

	eventually(t, 10*time.Second, func() bool { return completed.Load() >= concurrency }, "all jobs processed")

	counts := must(queue.GetJobCounts(ctx))
	if counts.Completed != concurrency || counts.Failed != 0 {
		t.Fatalf("counts = %+v", counts)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Should not keep active jobs after processing
// ═══════════════════════════════════════════════════════════════════════════

func TestNoActiveJobsAfterAllProcessed(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const total = 20
	for i := range total {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, nil))
	}

	var completed atomic.Int32
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		completed.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: 5})

	eventually(t, 10*time.Second, func() bool { return completed.Load() >= total }, "all jobs processed")
	w2Sleep(200 * time.Millisecond)

	counts := must(queue.GetJobCounts(ctx))
	if counts.Active != 0 || counts.Completed != total {
		t.Fatalf("counts = %+v", counts)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Process slow job without blocking next (concurrency > 1)
// ═══════════════════════════════════════════════════════════════════════════

func TestSlowJobDoesNotBlockConcurrent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// First job is slow, second is fast
	must(queue.Add(ctx, "test", map[string]any{"index": 1, "slow": true}, nil))
	must(queue.Add(ctx, "test", map[string]any{"index": 2, "slow": false}, nil))

	var mu sync.Mutex
	var order []int
	var completed atomic.Int32
	worker := newTestWorker(t, name, func(_ context.Context, job *bullmq.Job) (any, error) {
		var d struct {
			Index int
			Slow  bool
		}
		if err := job.DecodeData(&d); err != nil {
			return nil, err
		}
		if d.Slow {
			time.Sleep(500 * time.Millisecond)
		}
		mu.Lock()
		order = append(order, d.Index)
		mu.Unlock()
		completed.Add(1)
		return "done", nil
	}, &bullmq.WorkerOptions{Concurrency: 2})

	eventually(t, 5*time.Second, func() bool { return completed.Load() >= 2 }, "2 jobs processed")

	mu.Lock()
	processingOrder := slices.Clone(order)
	mu.Unlock()
	// Fast job (index 2) should complete before slow job (index 1)
	if !reflect.DeepEqual(processingOrder, []int{2, 1}) {
		t.Fatalf("order = %v, want [2 1]", processingOrder)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Concurrency respects limit - never exceeds concurrency
// ═══════════════════════════════════════════════════════════════════════════

func TestConcurrencyNeverExceeded(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const totalJobs = 8
	const concurrency = 4
	for i := range totalJobs {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, nil))
	}

	var active, maxActive, completed atomic.Int32
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		current := active.Add(1)
		// Update max observed concurrency
		for {
			m := maxActive.Load()
			if current <= m || maxActive.CompareAndSwap(m, current) {
				break
			}
		}

		time.Sleep(100 * time.Millisecond)

		active.Add(-1)
		completed.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: concurrency})

	eventually(t, 10*time.Second, func() bool { return completed.Load() >= totalJobs }, "all jobs processed")

	// Max active should never exceed concurrency
	observedMax := maxActive.Load()
	if observedMax > concurrency {
		t.Fatalf("Max active (%d) exceeded concurrency (%d)", observedMax, concurrency)
	}
	if observedMax <= 1 {
		t.Fatalf("Should have had concurrent processing, max was %d", observedMax)
	}
	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Process job with async operation and verify return value
// ═══════════════════════════════════════════════════════════════════════════

func TestProcessAsyncJobWithReturnValue(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, func(_ context.Context, job *bullmq.Job) (any, error) {
		if foo := w2Data(t, job)["foo"]; foo != "bar" {
			t.Errorf("data.foo = %v", foo)
		}
		time.Sleep(100 * time.Millisecond)
		return "my data", nil
	}, nil)

	waitForEvent(t, worker, 5*time.Second, eventType(bullmq.EventCompleted))

	w2Sleep(50 * time.Millisecond)
	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched.ReturnValue != `"my data"` {
		t.Fatalf("returnvalue = %q", fetched.ReturnValue)
	}
	noErr(t, worker.Close(5*time.Second))
}
