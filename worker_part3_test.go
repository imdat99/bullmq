package bullmq_test

// Port of bullmq_rust/tests/worker_test.rs (lines 7101-end): LIFO defaults,
// queue isolation, autorun, progress/active/paused events, job data and logs,
// retries, custom backoff, manual processing, job state helpers, dynamic
// concurrency, DelayedError, Queue.clean/retryJobs/promoteJobs/getJobLogs/
// trimEvents.

import (
	"bullmq"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// w3WaitEvents mirrors the Rust `tokio::time::timeout(d, loop { next_event })`
// pattern: it feeds every event to onEvent until onEvent returns true or the
// worker emits Closed / the event stream ends (Rust `Closed | None => break`).
// It returns false only when the timeout elapsed.
func w3WaitEvents(w *bullmq.Worker, timeout time.Duration, onEvent func(bullmq.WorkerEvent) bool) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		ev, ok := w.NextEvent(ctx)
		if !ok {
			return ctx.Err() == nil
		}
		if ev.Type == bullmq.EventClosed {
			return true
		}
		if onEvent(ev) {
			return true
		}
	}
}

// w3WaitType waits for one event of type typ (see w3WaitEvents).
func w3WaitType(w *bullmq.Worker, timeout time.Duration, typ bullmq.WorkerEventType) bool {
	return w3WaitEvents(w, timeout, func(e bullmq.WorkerEvent) bool { return e.Type == typ })
}

// w3WaitCount waits for n events of type typ (see w3WaitEvents).
func w3WaitCount(w *bullmq.Worker, timeout time.Duration, typ bullmq.WorkerEventType, n int) bool {
	count := 0
	return w3WaitEvents(w, timeout, func(e bullmq.WorkerEvent) bool {
		if e.Type == typ {
			count++
		}
		return count >= n
	})
}

// w3PollUntil polls cond every 50ms and reports whether it held before timeout.
func w3PollUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return cond()
}

// w3Data decodes the job data as a JSON object.
func w3Data(t testing.TB, job *bullmq.Job) map[string]any {
	t.Helper()
	var m map[string]any
	if err := job.DecodeData(&m); err != nil {
		t.Errorf("decode job data %s: %v", job.Data, err)
	}
	return m
}

// w3NullProcessor completes every job with a null result.
func w3NullProcessor(context.Context, *bullmq.Job) (any, error) { return nil, nil }

// w3StoreMax raises m to cur when cur is a new maximum.
func w3StoreMax(m *atomic.Int64, cur int64) {
	for {
		prev := m.Load()
		if cur <= prev || m.CompareAndSwap(prev, cur) {
			return
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Default job options - LIFO
// ═══════════════════════════════════════════════════════════════════════════

func TestDefaultJobOptionsLifo(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{
		DefaultJobOptions: bullmq.JobOptions{LIFO: true},
	})

	// Add jobs: 1, 2, 3 - with LIFO, processing should be 3, 2, 1
	for i := 1; i <= 3; i++ {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, nil))
	}

	var mu sync.Mutex
	var order []float64
	var completedCount atomic.Int64
	newTestWorker(t, name, func(_ context.Context, job *bullmq.Job) (any, error) {
		idx, _ := w3Data(t, job)["idx"].(float64)
		mu.Lock()
		order = append(order, idx)
		mu.Unlock()
		completedCount.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: 1})

	if !w3PollUntil(5*time.Second, func() bool { return completedCount.Load() >= 3 }) {
		t.Fatal("timeout")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(order) < 3 || order[0] != 3 || order[1] != 2 || order[2] != 1 {
		t.Fatalf("processing order = %v, want [3 2 1]", order)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Worker should not process jobs from other queues
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkerIgnoresOtherQueues(t *testing.T) {
	ctx := context.Background()
	name1 := testQueueName()
	name2 := testQueueName()
	queue1 := newTestQueue(t, name1, nil)
	queue2 := newTestQueue(t, name2, nil)

	// Add job to queue2
	must(queue2.Add(ctx, "test", map[string]any{"queue": 2}, nil))

	// Worker listens to queue1 only
	var processed atomic.Int64
	newTestWorker(t, name1, func(context.Context, *bullmq.Job) (any, error) {
		processed.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Wait a bit, no jobs should be processed
	time.Sleep(2 * time.Second)
	if n := processed.Load(); n != 0 {
		t.Fatalf("processed = %d, want 0", n)
	}

	// Now add a job to queue1
	must(queue1.Add(ctx, "test", map[string]any{"queue": 1}, nil))

	if !w3PollUntil(5*time.Second, func() bool { return processed.Load() >= 1 }) {
		t.Fatal("timeout")
	}
	if n := processed.Load(); n != 1 {
		t.Fatalf("processed = %d, want 1", n)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Worker with autorun false processes jobs after run() is called
// ═══════════════════════════════════════════════════════════════════════════

func TestAutorunFalseNoProcessingUntilRun(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	var processed atomic.Int64
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		processed.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{ManualStart: true})

	// Worker is not running, so no processing should happen
	time.Sleep(500 * time.Millisecond)
	if n := processed.Load(); n != 0 {
		t.Fatalf("processed = %d, want 0", n)
	}

	// Now start the worker
	noErr(t, worker.Run())

	if !w3PollUntil(5*time.Second, func() bool { return processed.Load() >= 1 }) {
		t.Fatal("timeout")
	}
	if n := processed.Load(); n != 1 {
		t.Fatalf("processed = %d, want 1", n)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Progress, Active, Paused/Resumed Events
// ═══════════════════════════════════════════════════════════════════════════

func TestUpdateProgressAsNumber(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "progress-job", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if err := job.UpdateProgress(ctx, 42.0); err != nil {
			return nil, err
		}
		if err := job.UpdateProgress(ctx, 100.0); err != nil {
			return nil, err
		}
		return nil, nil
	}, nil)

	var progressValues []float64
	ok := w3WaitEvents(worker, 10*time.Second, func(e bullmq.WorkerEvent) bool {
		if e.Type != bullmq.EventProgress {
			return false
		}
		var n float64
		if err := json.Unmarshal(e.Progress, &n); err == nil {
			progressValues = append(progressValues, n)
		}
		return len(progressValues) == 2
	})

	if !ok {
		t.Fatal("Timed out waiting for progress events")
	}
	if len(progressValues) != 2 || progressValues[0] != 42 || progressValues[1] != 100 {
		t.Fatalf("progress values = %v, want [42 100]", progressValues)
	}
}

func TestUpdateProgressAsObject(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "progress-obj-job", map[string]any{}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if err := job.UpdateProgress(ctx, map[string]any{"total": 100, "done": 50}); err != nil {
			return nil, err
		}
		return nil, nil
	}, nil)

	gotProgress := false
	ok := w3WaitEvents(worker, 10*time.Second, func(e bullmq.WorkerEvent) bool {
		if e.Type != bullmq.EventProgress {
			return false
		}
		var val map[string]any
		if err := json.Unmarshal(e.Progress, &val); err == nil && val != nil {
			if val["total"] != float64(100) {
				t.Errorf("total = %v, want 100", val["total"])
			}
			if val["done"] != float64(50) {
				t.Errorf("done = %v, want 50", val["done"])
			}
			gotProgress = true
		}
		return true
	})

	if !ok {
		t.Fatal("Timed out waiting for progress event")
	}
	if !gotProgress {
		t.Fatal("Should have received progress event with object")
	}
}

func TestActiveEventEmitted(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "active-test", map[string]any{}, nil))

	worker := newTestWorker(t, name, w3NullProcessor, nil)

	gotActive := false
	activeJobID := ""
	ok := w3WaitEvents(worker, 10*time.Second, func(e bullmq.WorkerEvent) bool {
		switch e.Type {
		case bullmq.EventActive:
			gotActive = true
			activeJobID = e.JobID
		case bullmq.EventCompleted:
			return true
		}
		return false
	})

	if !ok {
		t.Fatal("Timed out waiting for events")
	}
	if !gotActive {
		t.Fatal("Should have received Active event")
	}
	if activeJobID == "" {
		t.Fatal("Active event should have job_id")
	}
}

func TestPausedResumedEvents(t *testing.T) {
	name := testQueueName()
	newTestQueue(t, name, nil)

	worker := newTestWorker(t, name, w3NullProcessor, nil)

	// Wait a bit for worker to start
	time.Sleep(100 * time.Millisecond)

	worker.Pause()
	worker.Resume()

	gotPaused := false
	gotResumed := false
	ok := w3WaitEvents(worker, 5*time.Second, func(e bullmq.WorkerEvent) bool {
		switch e.Type {
		case bullmq.EventPaused:
			gotPaused = true
		case bullmq.EventResumed:
			gotResumed = true
			return true
		}
		return false
	})

	if !ok {
		t.Fatal("Timed out waiting for paused/resumed")
	}
	if !gotPaused {
		t.Fatal("Should have received Paused event")
	}
	if !gotResumed {
		t.Fatal("Should have received Resumed event")
	}
}

func TestUpdateData(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "update-data-job", map[string]any{"initial": true}, nil))

	tx := make(chan json.RawMessage, 1)
	newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		newData := map[string]any{"updated": true, "value": 123}
		if err := job.UpdateData(ctx, newData); err != nil {
			return nil, err
		}
		select {
		case tx <- job.Data:
		default:
		}
		return nil, nil
	}, nil)

	var raw json.RawMessage
	select {
	case raw = <-tx:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for data")
	}

	var data map[string]any
	noErr(t, json.Unmarshal(raw, &data))
	if data["updated"] != true {
		t.Fatalf("updated = %v, want true", data["updated"])
	}
	if data["value"] != float64(123) {
		t.Fatalf("value = %v, want 123", data["value"])
	}
}

func TestJobLog(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "log-job", map[string]any{}, nil))

	tx := make(chan int64, 1)
	newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if _, err := job.Log(ctx, "First log entry"); err != nil {
			return nil, err
		}
		count, err := job.Log(ctx, "Second log entry")
		if err != nil {
			return nil, err
		}
		select {
		case tx <- count:
		default:
		}
		return nil, nil
	}, nil)

	var count int64
	select {
	case count = <-tx:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for log count")
	}

	if count != 2 {
		t.Fatalf("Should have 2 log entries, got %d", count)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Job Retry (reprocessJob)
// ═══════════════════════════════════════════════════════════════════════════

func TestRetryFailedJob(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "retry-me", map[string]any{"x": 1}, nil))

	var attemptCount atomic.Int64
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		n := attemptCount.Add(1) - 1
		if n == 0 {
			return nil, errors.New("first attempt fails")
		}
		return map[string]any{"success": true}, nil
	}, nil)

	// Wait for the first processing to fail
	if !w3WaitType(worker, 10*time.Second, bullmq.EventFailed) {
		t.Fatal("Timed out waiting for failed event")
	}

	// Now retry the failed job
	noErr(t, job.Retry(ctx, bullmq.StateFailed, nil))

	// Wait for the second processing to complete
	if !w3WaitType(worker, 10*time.Second, bullmq.EventCompleted) {
		t.Fatal("Timed out waiting for completed event after retry")
	}
	if n := attemptCount.Load(); n != 2 {
		t.Fatalf("attempt count = %d, want 2", n)
	}
}

func TestRetryCompletedJob(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "retry-completed", map[string]any{}, nil))

	var attemptCount atomic.Int64
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		attemptCount.Add(1)
		return map[string]any{"done": true}, nil
	}, nil)

	// Wait for first completion
	if !w3WaitType(worker, 10*time.Second, bullmq.EventCompleted) {
		t.Fatal("Timed out waiting for first completed event")
	}

	// Retry from completed state
	noErr(t, job.Retry(ctx, bullmq.StateCompleted, nil))

	// Wait for second completion
	if !w3WaitType(worker, 10*time.Second, bullmq.EventCompleted) {
		t.Fatal("Timed out waiting for second completed event")
	}
	if n := attemptCount.Load(); n != 2 {
		t.Fatalf("attempt count = %d, want 2", n)
	}
}

func TestRetryWithResetAttemptsMade(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "retry-reset", map[string]any{}, nil))

	tx := make(chan int, 2)
	var callCount atomic.Int64
	worker := newTestWorker(t, name, func(_ context.Context, job *bullmq.Job) (any, error) {
		n := callCount.Add(1) - 1
		select {
		case tx <- job.AttemptsMade:
		default:
		}
		if n == 0 {
			return nil, errors.New("fail first")
		}
		return nil, nil
	}, nil)

	// Wait for failure
	if !w3WaitType(worker, 10*time.Second, bullmq.EventFailed) {
		t.Fatal("timeout waiting for failed event")
	}

	// Retry with reset_attempts_made
	noErr(t, job.Retry(ctx, bullmq.StateFailed, &bullmq.RetryOptions{ResetAttemptsMade: true}))

	// Wait for second processing to complete
	if !w3WaitType(worker, 10*time.Second, bullmq.EventCompleted) {
		t.Fatal("timeout waiting for completed event")
	}

	// Check that attempts_made was reset: first call had attempts_made from first processing,
	// second call should have attempts_made=0 because we reset it
	<-tx
	second := <-tx
	// first processing increments to 1 inside moveToFinished, but we see pre-increment value
	// second processing after reset should see 0
	if second != 0 {
		t.Fatalf("attempts_made should be reset to 0, got %d", second)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Custom Backoff Strategy
// ═══════════════════════════════════════════════════════════════════════════

func TestCustomBackoffStrategy(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "custom-backoff", map[string]any{}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.CustomBackoff("custom"),
	}))

	var attemptCount atomic.Int64
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		n := attemptCount.Add(1) - 1
		if n < 2 {
			return nil, errors.New("fail")
		}
		return nil, nil
	}, &bullmq.WorkerOptions{
		DrainDelay: time.Second,
		BackoffStrategy: func(int, string, error, *bullmq.Job) time.Duration {
			return 100 * time.Millisecond // 100ms delay
		},
	})

	// Wait for completion (should retry twice with custom backoff then succeed)
	failedCount := 0
	completed := false
	ok := w3WaitEvents(worker, 10*time.Second, func(e bullmq.WorkerEvent) bool {
		switch e.Type {
		case bullmq.EventCompleted:
			completed = true
			return true
		case bullmq.EventFailed:
			failedCount++
			t.Logf("FAILED event #%d: %v", failedCount, e.Err)
			return failedCount >= 3
		}
		return false
	})
	if !ok {
		t.Fatalf("Timed out waiting for completion (failed_count=%d)", failedCount)
	}
	if !completed {
		t.Fatalf("Should have completed (failed_count=%d)", failedCount)
	}
	if n := attemptCount.Load(); n != 3 {
		t.Fatalf("attempt count = %d, want 3", n)
	}
}

func TestCustomBackoffReturnsMinusOneNoRetry(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "no-retry-custom", map[string]any{}, &bullmq.JobOptions{
		Attempts: 5,
		Backoff:  bullmq.CustomBackoff("myCustom"),
	}))

	var attemptCount atomic.Int64
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		attemptCount.Add(1)
		return nil, errors.New("always fails")
	}, &bullmq.WorkerOptions{
		// Return -1 to signal "don't retry"
		BackoffStrategy: func(int, string, error, *bullmq.Job) time.Duration {
			return -1 * time.Millisecond
		},
	})

	// Wait for the failed event (should only process once since backoff returns -1)
	if !w3WaitType(worker, 10*time.Second, bullmq.EventFailed) {
		t.Fatal("Timed out waiting for failed event")
	}

	// Give a moment for any unexpected retries
	time.Sleep(200 * time.Millisecond)
	if n := attemptCount.Load(); n != 1 {
		t.Fatalf("Should only attempt once, got %d", n)
	}
}

func TestCustomBackoffBasedOnError(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "error-based-backoff", map[string]any{}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.CustomBackoff("errorBased"),
	}))

	var attemptCount atomic.Int64
	tx := make(chan string, 4)
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		n := attemptCount.Add(1) - 1
		if n < 2 {
			return nil, errors.New("SpecificError")
		}
		return nil, nil
	}, &bullmq.WorkerOptions{
		DrainDelay: time.Second,
		BackoffStrategy: func(_ int, _ string, err error, _ *bullmq.Job) time.Duration {
			select {
			case tx <- err.Error():
			default:
			}
			return 50 * time.Millisecond
		},
	})

	// Wait for completion
	if !w3WaitType(worker, 10*time.Second, bullmq.EventCompleted) {
		t.Fatal("Timed out")
	}

	// Verify the error was passed to the backoff function
	var errMsg string
	select {
	case errMsg = <-tx:
	case <-time.After(5 * time.Second):
		t.Fatal("backoff function was never called")
	}
	if !strings.Contains(errMsg, "SpecificError") {
		t.Fatalf("error passed to backoff = %q, want it to contain SpecificError", errMsg)
	}
}

func TestCustomBackoffBasedOnJobData(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "data-based-backoff", map[string]any{"delay_factor": 200}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.CustomBackoff("dataBased"),
	}))

	var attemptCount atomic.Int64
	tx := make(chan int64, 4)
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		n := attemptCount.Add(1) - 1
		if n < 1 {
			return nil, errors.New("fail")
		}
		return nil, nil
	}, &bullmq.WorkerOptions{
		DrainDelay: time.Second,
		BackoffStrategy: func(_ int, _ string, _ error, job *bullmq.Job) time.Duration {
			delay := int64(100)
			var data map[string]any
			if job.DecodeData(&data) == nil {
				if f, ok := data["delay_factor"].(float64); ok {
					delay = int64(f)
				}
			}
			select {
			case tx <- delay:
			default:
			}
			return time.Duration(delay) * time.Millisecond
		},
	})

	// Wait for completion
	if !w3WaitType(worker, 10*time.Second, bullmq.EventCompleted) {
		t.Fatal("Timed out")
	}

	// Verify delay was computed from job data
	var delay int64
	select {
	case delay = <-tx:
	case <-time.After(5 * time.Second):
		t.Fatal("backoff function was never called")
	}
	if delay != 200 {
		t.Fatalf("delay = %d, want 200", delay)
	}
}

func TestCustomBackoffForCustomType(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "typed-backoff", map[string]any{}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.CustomBackoff("mySpecialType"),
	}))

	var attemptCount atomic.Int64
	tx := make(chan string, 4)
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		n := attemptCount.Add(1) - 1
		if n < 1 {
			return nil, errors.New("fail")
		}
		return nil, nil
	}, &bullmq.WorkerOptions{
		DrainDelay: time.Second,
		BackoffStrategy: func(_ int, typeName string, _ error, _ *bullmq.Job) time.Duration {
			select {
			case tx <- typeName:
			default:
			}
			return 50 * time.Millisecond
		},
	})

	// Wait for completion
	if !w3WaitType(worker, 10*time.Second, bullmq.EventCompleted) {
		t.Fatal("Timed out")
	}

	// Verify the type name was passed
	var receivedType string
	select {
	case receivedType = <-tx:
	case <-time.After(5 * time.Second):
		t.Fatal("backoff function was never called")
	}
	if receivedType != "mySpecialType" {
		t.Fatalf("type = %q, want mySpecialType", receivedType)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Progress Variants (string, boolean)
// ═══════════════════════════════════════════════════════════════════════════

func TestUpdateProgressAsString(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if err := job.UpdateProgress(ctx, "progress as string"); err != nil {
			return nil, err
		}
		return nil, nil
	}, nil)

	got := false
	ok := w3WaitEvents(worker, 10*time.Second, func(e bullmq.WorkerEvent) bool {
		if e.Type != bullmq.EventProgress {
			return false
		}
		var s string
		if json.Unmarshal(e.Progress, &s) != nil {
			return false
		}
		if s != "progress as string" {
			t.Errorf("progress = %q, want %q", s, "progress as string")
		}
		got = true
		return true
	})
	if !ok || !got {
		t.Fatal("Timed out waiting for string progress event")
	}
}

func TestUpdateProgressAsBoolean(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if err := job.UpdateProgress(ctx, true); err != nil {
			return nil, err
		}
		return nil, nil
	}, nil)

	got := false
	ok := w3WaitEvents(worker, 10*time.Second, func(e bullmq.WorkerEvent) bool {
		if e.Type != bullmq.EventProgress {
			return false
		}
		var b bool
		if json.Unmarshal(e.Progress, &b) != nil {
			return false
		}
		if !b {
			t.Errorf("progress = false, want true")
		}
		got = true
		return true
	})
	if !ok || !got {
		t.Fatal("Timed out waiting for boolean progress event")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Non-blocking getNextJob
// ═══════════════════════════════════════════════════════════════════════════

func TestGetNextJobNonblockingReturnsNoneWhenEmpty(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	worker := newTestWorker(t, name, w3NullProcessor, &bullmq.WorkerOptions{ManualStart: true})

	// Non-blocking should return None immediately when no jobs
	result, err := worker.GetNextJobNonBlocking(ctx, "token-1")
	noErr(t, err)
	if result != nil {
		t.Fatalf("expected no job, got %s", result.ID)
	}

	// Add a job and fetch it
	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	// Small delay to ensure Redis propagation
	time.Sleep(50 * time.Millisecond)

	result, err = worker.GetNextJobNonBlocking(ctx, "token-2")
	noErr(t, err)
	if result == nil {
		t.Fatal("expected a job")
	}
	if foo := w3Data(t, result)["foo"]; foo != "bar" {
		t.Fatalf("data.foo = %v, want bar", foo)
	}
}

func TestGetNextJobNonblockingWhenPaused(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	worker := newTestWorker(t, name, w3NullProcessor, &bullmq.WorkerOptions{ManualStart: true})

	// Add a job
	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	// Pause the worker
	worker.Pause()

	// Non-blocking fetch while paused — should return None
	result, err := worker.GetNextJobNonBlocking(ctx, "token-1")
	noErr(t, err)
	if result != nil {
		t.Fatalf("expected no job while paused, got %s", result.ID)
	}

	// Resume and fetch
	worker.Resume()

	result, err = worker.GetNextJobNonBlocking(ctx, "token-2")
	noErr(t, err)
	if result == nil {
		t.Fatal("expected a job after resume")
	}
}

func TestGetNextJobEmitsActiveEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, w3NullProcessor, &bullmq.WorkerOptions{ManualStart: true})

	// Fetch the job manually
	fetched, err := worker.GetNextJob(ctx, "token-1")
	noErr(t, err)
	if fetched == nil {
		t.Fatal("expected a job")
	}

	// Check that Active event was emitted
	evCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ev, ok := worker.NextEvent(evCtx)
	if !ok || ev.Type != bullmq.EventActive {
		t.Fatalf("Expected Active event, got %+v (ok=%v)", ev, ok)
	}
	if ev.JobID != job.ID {
		t.Fatalf("active job id = %q, want %q", ev.JobID, job.ID)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Retry Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

func TestCannotRetryActiveJob(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	tx := make(chan struct{}, 16)
	newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		tx <- struct{}{}
		// Keep the job active for a while
		time.Sleep(2 * time.Second)
		return nil, nil
	}, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	// Wait for the job to start processing
	<-tx

	// Trying to retry while active should fail
	if err := job.Retry(ctx, bullmq.StateFailed, nil); err == nil {
		t.Fatal("retrying an active job should fail")
	}
}

func TestCannotRetryCompletedJobFromFailedState(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	var attemptCount atomic.Int64
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		attempt := attemptCount.Add(1) - 1
		if attempt == 0 {
			return nil, errors.New("first failure")
		}
		return "done", nil
	}, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	// Wait for the job to fail
	if !w3WaitType(worker, 10*time.Second, bullmq.EventFailed) {
		t.Fatal("timeout waiting for failed event")
	}

	// Retry the failed job (should succeed)
	noErr(t, job.Retry(ctx, bullmq.StateFailed, nil))

	// Wait for it to complete
	if !w3WaitType(worker, 10*time.Second, bullmq.EventCompleted) {
		t.Fatal("timeout waiting for completed event")
	}

	// Trying to retry from "failed" state when it's now "completed" should fail
	if err := job.Retry(ctx, bullmq.StateFailed, nil); err == nil {
		t.Fatal("retrying a completed job from the failed state should fail")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Close Behavior
// ═══════════════════════════════════════════════════════════════════════════

func TestCloseWhileJobCompletes(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	tx := make(chan struct{}, 16)
	worker := newTestWorker(t, name, func(_ context.Context, job *bullmq.Job) (any, error) {
		if foo := w3Data(t, job)["foo"]; foo != "bar" {
			t.Errorf("data.foo = %v, want bar", foo)
		}
		tx <- struct{}{}
		// Simulate slow processing
		time.Sleep(500 * time.Millisecond)
		return "completed", nil
	}, nil)

	// Wait for processing to start
	<-tx

	// Close while job is processing (graceful close should wait for job to finish)
	if err := worker.Close(5 * time.Second); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestCloseWhileJobFails(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	tx := make(chan struct{}, 16)
	worker := newTestWorker(t, name, func(_ context.Context, job *bullmq.Job) (any, error) {
		if foo := w3Data(t, job)["foo"]; foo != "bar" {
			t.Errorf("data.foo = %v, want bar", foo)
		}
		tx <- struct{}{}
		// Simulate slow processing then fail
		time.Sleep(500 * time.Millisecond)
		return nil, errors.New("Job Failed")
	}, nil)

	// Wait for processing to start
	<-tx

	// Close while job is processing (graceful close should wait for job to finish)
	if err := worker.Close(5 * time.Second); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Backoff Timing Verification
// ═══════════════════════════════════════════════════════════════════════════

func TestExponentialBackoffTotalTiming(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const baseDelay = 100 * time.Millisecond
	const attempts = 4

	must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{
		Attempts: attempts,
		Backoff:  bullmq.ExponentialBackoff(baseDelay),
	}))

	start := time.Now()
	var attemptCount atomic.Int64
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		attemptCount.Add(1)
		return nil, errors.New("fail")
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Wait for all attempts to be exhausted (final failure)
	if !w3WaitCount(worker, 30*time.Second, bullmq.EventFailed, attempts) {
		t.Fatal("Timed out waiting for all failures")
	}

	elapsed := time.Since(start)
	// Exponential: delay * 2^(attempt-1) for attempts 1,2,3
	// Total minimum delay: 100 + 200 + 400 = 700ms
	expectedMin := baseDelay + baseDelay*2 + baseDelay*4 // 700ms
	if elapsed < expectedMin-50*time.Millisecond {
		t.Fatalf("Elapsed %s should be >= ~%s", elapsed, expectedMin)
	}
	if n := attemptCount.Load(); n != attempts {
		t.Fatalf("attempt count = %d, want %d", n, attempts)
	}
}

func TestFixedBackoffConstantDelay(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const delay = 1500 * time.Millisecond
	const attempts = 3

	must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{
		Attempts: attempts,
		Backoff:  bullmq.FixedBackoff(delay),
	}))

	var mu sync.Mutex
	var failTimes []time.Time
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		mu.Lock()
		failTimes = append(failTimes, time.Now())
		mu.Unlock()
		return nil, errors.New("fail")
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	if !w3WaitCount(worker, 15*time.Second, bullmq.EventFailed, attempts) {
		t.Fatal("Timed out waiting for all failures")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(failTimes) != attempts {
		t.Fatalf("attempts recorded = %d, want %d", len(failTimes), attempts)
	}

	// Verify each interval is at least the fixed delay (drain_delay polling adds overhead)
	for i := 1; i < len(failTimes); i++ {
		interval := failTimes[i].Sub(failTimes[i-1])
		if interval < delay-100*time.Millisecond {
			t.Fatalf("Interval %s should be >= ~%s", interval, delay)
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Concurrency Limits
// ═══════════════════════════════════════════════════════════════════════════

func TestConcurrencyLimitsParallelProcessing(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const concurrency = 4
	const totalJobs = 8

	// Add jobs
	for i := 0; i < totalJobs; i++ {
		must(queue.Add(ctx, "test", map[string]any{"index": i}, nil))
	}

	var maxConcurrent, currentConcurrent atomic.Int64
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		current := currentConcurrent.Add(1)
		// Update max if this is a new high
		w3StoreMax(&maxConcurrent, current)
		time.Sleep(100 * time.Millisecond)
		currentConcurrent.Add(-1)
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: concurrency})

	// Wait for all jobs to complete
	if !w3WaitCount(worker, 15*time.Second, bullmq.EventCompleted, totalJobs) {
		t.Fatal("Timed out waiting for completions")
	}

	observedMax := maxConcurrent.Load()
	if observedMax > concurrency {
		t.Fatalf("Max concurrent %d should be <= concurrency %d", observedMax, concurrency)
	}
	if observedMax < 2 {
		t.Fatalf("Should have seen some parallelism, got %d", observedMax)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Pause + Retry Interaction
// ═══════════════════════════════════════════════════════════════════════════

func TestRetryMovesJobToPausedWhenQueuePaused(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	var attemptCount atomic.Int64
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		attempt := attemptCount.Add(1) - 1
		if attempt == 0 {
			return nil, errors.New("fail first time")
		}
		return "done", nil
	}, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	// Wait for the job to fail
	if !w3WaitType(worker, 10*time.Second, bullmq.EventFailed) {
		t.Fatal("timeout waiting for failed event")
	}

	// Pause the worker
	worker.Pause()
	time.Sleep(100 * time.Millisecond)

	// Retry the job while paused — it should go to paused list
	noErr(t, job.Retry(ctx, bullmq.StateFailed, nil))

	// The job should now be in the paused state (waiting in paused list)
	// Resume and it should complete
	worker.Resume()

	if !w3WaitType(worker, 10*time.Second, bullmq.EventCompleted) {
		t.Fatal("Job should complete after resume")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Job State Methods
// ═══════════════════════════════════════════════════════════════════════════

func TestJobGetStateWaiting(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	state := must(queue.GetJobState(ctx, job.ID))
	if state != bullmq.StateWaiting {
		t.Fatalf("state = %s, want waiting", state)
	}
}

func TestJobGetStateActive(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	tx := make(chan string, 16)
	newTestWorker(t, name, func(_ context.Context, job *bullmq.Job) (any, error) {
		tx <- job.ID
		time.Sleep(2 * time.Second)
		return nil, nil
	}, nil)

	job := must(queue.Add(ctx, "test", map[string]any{}, nil))

	// Wait for the job to become active
	<-tx

	state := must(queue.GetJobState(ctx, job.ID))
	if state != bullmq.StateActive {
		t.Fatalf("state = %s, want active", state)
	}
}

func TestJobGetStateCompleted(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		return "done", nil
	}, nil)

	job := must(queue.Add(ctx, "test", map[string]any{}, nil))

	// Wait for completion
	if !w3WaitType(worker, 10*time.Second, bullmq.EventCompleted) {
		t.Fatal("timeout waiting for completed event")
	}

	state := must(queue.GetJobState(ctx, job.ID))
	if state != bullmq.StateCompleted {
		t.Fatalf("state = %s, want completed", state)
	}
}

func TestJobGetStateFailed(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		return nil, errors.New("fail")
	}, nil)

	job := must(queue.Add(ctx, "test", map[string]any{}, nil))

	// Wait for failure
	if !w3WaitType(worker, 10*time.Second, bullmq.EventFailed) {
		t.Fatal("timeout waiting for failed event")
	}

	state := must(queue.GetJobState(ctx, job.ID))
	if state != bullmq.StateFailed {
		t.Fatalf("state = %s, want failed", state)
	}
}

func TestJobGetStateDelayed(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{
		Delay: 60 * time.Second, // 60 seconds — won't be promoted during test
	}))

	state := must(queue.GetJobState(ctx, job.ID))
	if state != bullmq.StateDelayed {
		t.Fatalf("state = %s, want delayed", state)
	}
}

func TestJobIsActiveViaGetNextJob(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, w3NullProcessor, &bullmq.WorkerOptions{ManualStart: true})

	// Manually fetch the job — it should now be active
	fetched, err := worker.GetNextJob(ctx, "token-1")
	noErr(t, err)
	if fetched == nil {
		t.Fatal("expected a job")
	}

	state := must(queue.GetJobState(ctx, job.ID))
	if state != bullmq.StateActive {
		t.Fatalf("state = %s, want active", state)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Additional Edge Cases
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkerProcessesAfterPauseResumeCycle(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		return "done", nil
	}, nil)

	// Pause, add a job, resume — job should be processed
	worker.Pause()
	time.Sleep(100 * time.Millisecond)

	must(queue.Add(ctx, "test", map[string]any{}, nil))

	worker.Resume()

	if !w3WaitType(worker, 10*time.Second, bullmq.EventCompleted) {
		t.Fatal("Job should be processed after resume")
	}
}

func TestWorkerMultiplePauseResumeCycles(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	var completedCount atomic.Int64
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		completedCount.Add(1)
		return "done", nil
	}, nil)

	// Add job, wait for completion
	must(queue.Add(ctx, "test1", map[string]any{}, nil))

	if !w3WaitType(worker, 5*time.Second, bullmq.EventCompleted) {
		t.Fatal("timeout waiting for first completion")
	}

	// Pause and resume
	worker.Pause()
	time.Sleep(100 * time.Millisecond)
	worker.Resume()

	// Add another job
	must(queue.Add(ctx, "test2", map[string]any{}, nil))

	if !w3WaitType(worker, 5*time.Second, bullmq.EventCompleted) {
		t.Fatal("timeout waiting for second completion")
	}
	if n := completedCount.Load(); n != 2 {
		t.Fatalf("completed = %d, want 2", n)
	}
}

func TestWorkerUnrecoverableErrorSkipsRetries(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	var attemptCount atomic.Int64
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		attemptCount.Add(1)
		return nil, bullmq.NewUnrecoverableError("fatal error")
	}, nil)

	// Job with 3 attempts but should fail immediately with unrecoverable error
	must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.FixedBackoff(100 * time.Millisecond),
	}))

	// Wait for failure
	if !w3WaitType(worker, 5*time.Second, bullmq.EventFailed) {
		t.Fatal("timeout waiting for failed event")
	}

	// Should have only been attempted once (unrecoverable skips retries)
	if n := attemptCount.Load(); n != 1 {
		t.Fatalf("attempt count = %d, want 1", n)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Dynamic Concurrency
// ═══════════════════════════════════════════════════════════════════════════

func TestSetConcurrencyIncrease(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const totalJobs = 16
	for i := 0; i < totalJobs; i++ {
		must(queue.Add(ctx, "test", map[string]any{"index": i}, nil))
	}

	var maxConcurrentPhase1, currentConcurrent, completedCount atomic.Int64
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		current := currentConcurrent.Add(1)
		completedSoFar := completedCount.Load()

		if completedSoFar < 8 {
			w3StoreMax(&maxConcurrentPhase1, current)
		}

		time.Sleep(50 * time.Millisecond)
		currentConcurrent.Add(-1)
		completedCount.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: 4})

	// Wait for first 8 completions
	if !w3WaitCount(worker, 15*time.Second, bullmq.EventCompleted, 8) {
		t.Fatal("Timed out waiting for first 8 completions")
	}

	// Increase concurrency
	worker.SetConcurrency(8)

	// Wait for remaining 8 completions
	if !w3WaitCount(worker, 15*time.Second, bullmq.EventCompleted, 8) {
		t.Fatal("Timed out waiting for last 8 completions")
	}

	if phase1Max := maxConcurrentPhase1.Load(); phase1Max > 4 {
		t.Fatalf("Phase 1 max concurrent %d should be <= 4", phase1Max)
	}
}

func TestSetConcurrencyDecrease(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const totalJobs = 16
	for i := 0; i < totalJobs; i++ {
		must(queue.Add(ctx, "test", map[string]any{"index": i}, nil))
	}

	var maxConcurrentPhase2, currentConcurrent, completedCount atomic.Int64
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		current := currentConcurrent.Add(1)
		completedSoFar := completedCount.Load()

		// Track max concurrency only after transition settles (12+ completions)
		if completedSoFar >= 12 {
			w3StoreMax(&maxConcurrentPhase2, current)
		}

		time.Sleep(50 * time.Millisecond)
		currentConcurrent.Add(-1)
		completedCount.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: 4})

	// Wait for first 8 completions
	if !w3WaitCount(worker, 15*time.Second, bullmq.EventCompleted, 8) {
		t.Fatal("Timed out waiting for first 8")
	}

	// Decrease concurrency
	worker.SetConcurrency(2)

	// Wait for remaining 8 completions
	if !w3WaitCount(worker, 15*time.Second, bullmq.EventCompleted, 8) {
		t.Fatal("Timed out waiting for last 8")
	}

	if phase2Max := maxConcurrentPhase2.Load(); phase2Max > 2 {
		t.Fatalf("Phase 2 max concurrent %d should be <= 2 after decrease", phase2Max)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// DelayedError support
// ═══════════════════════════════════════════════════════════════════════════

func TestDelayedErrorMovesJobToDelayedAndReprocesses(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"step": 0}, nil))

	var attemptCount atomic.Int64
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		step, _ := w3Data(t, job)["step"].(float64)
		attemptCount.Add(1)

		if step == 0 {
			// Move to delayed, then throw DelayedError
			if err := job.MoveToDelayed(ctx, time.Now().Add(200*time.Millisecond)); err != nil {
				t.Errorf("move to delayed: %v", err)
				return nil, err
			}
			if err := job.UpdateData(ctx, map[string]any{"step": 1}); err != nil {
				t.Errorf("update data: %v", err)
				return nil, err
			}
			return nil, bullmq.ErrDelayed
		}

		// Step 1: complete
		return map[string]any{"done": true}, nil
	}, nil)

	// Wait for completion
	ctxT, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for {
		ev, ok := worker.NextEvent(ctxT)
		if !ok {
			if ctxT.Err() != nil {
				t.Fatal("Job should eventually complete after being delayed")
			}
			t.Fatal("worker closed unexpectedly")
		}
		if ev.Type == bullmq.EventClosed {
			t.Fatal("worker closed unexpectedly")
		}
		if ev.Type == bullmq.EventCompleted {
			break
		}
	}

	// Should have been processed twice (step 0 -> delayed, step 1 -> completed)
	if n := attemptCount.Load(); n != 2 {
		t.Fatalf("attempt count = %d, want 2", n)
	}

	// Verify job is completed
	counts := must(queue.GetJobCounts(ctx))
	if counts.Completed != 1 || counts.Failed != 0 {
		t.Fatalf("counts = %+v, want completed=1 failed=0", counts)
	}
}

func TestDelayedErrorDoesNotEmitFailedEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"step": 0}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		step, _ := w3Data(t, job)["step"].(float64)

		if step == 0 {
			if err := job.MoveToDelayed(ctx, time.Now().Add(100*time.Millisecond)); err != nil {
				t.Errorf("move to delayed: %v", err)
				return nil, err
			}
			if err := job.UpdateData(ctx, map[string]any{"step": 1}); err != nil {
				t.Errorf("update data: %v", err)
				return nil, err
			}
			return nil, bullmq.ErrDelayed
		}

		return nil, nil
	}, nil)

	sawFailed := false
	ok := w3WaitEvents(worker, 10*time.Second, func(e bullmq.WorkerEvent) bool {
		switch e.Type {
		case bullmq.EventFailed:
			sawFailed = true
		case bullmq.EventCompleted:
			return true
		}
		return false
	})
	if !ok {
		t.Fatal("Should complete")
	}
	if sawFailed {
		t.Fatal("DelayedError should NOT emit a Failed event")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Queue.clean()
// ═══════════════════════════════════════════════════════════════════════════

func TestQueueCleanCompletedJobs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add and process jobs
	for i := 0; i < 5; i++ {
		must(queue.Add(ctx, "test", map[string]any{"i": i}, nil))
	}

	worker := newTestWorker(t, name, w3NullProcessor, nil)

	// Wait for all jobs to complete
	if !w3WaitCount(worker, 5*time.Second, bullmq.EventCompleted, 5) {
		t.Fatal("Should complete all 5 jobs")
	}

	counts := must(queue.GetJobCounts(ctx))
	if counts.Completed != 5 {
		t.Fatalf("completed = %d, want 5", counts.Completed)
	}

	// Clean completed jobs with grace=0 (remove all)
	removed := must(queue.Clean(ctx, 0, 0, "completed"))
	if len(removed) != 5 {
		t.Fatalf("removed = %d, want 5", len(removed))
	}

	counts = must(queue.GetJobCounts(ctx))
	if counts.Completed != 0 {
		t.Fatalf("completed = %d, want 0", counts.Completed)
	}
}

func TestQueueCleanWithLimit(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	for i := 0; i < 10; i++ {
		must(queue.Add(ctx, "test", map[string]any{"i": i}, nil))
	}

	worker := newTestWorker(t, name, w3NullProcessor, nil)

	if !w3WaitCount(worker, 5*time.Second, bullmq.EventCompleted, 10) {
		t.Fatal("Should complete all 10 jobs")
	}

	// Clean only 3
	removed := must(queue.Clean(ctx, 0, 3, "completed"))
	if len(removed) != 3 {
		t.Fatalf("removed = %d, want 3", len(removed))
	}

	counts := must(queue.GetJobCounts(ctx))
	if counts.Completed != 7 {
		t.Fatalf("completed = %d, want 7", counts.Completed)
	}
}

func TestQueueCleanFailedJobs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	for i := 0; i < 3; i++ {
		must(queue.Add(ctx, "test", map[string]any{"i": i}, nil))
	}

	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		return nil, errors.New("fail")
	}, nil)

	if !w3WaitCount(worker, 5*time.Second, bullmq.EventFailed, 3) {
		t.Fatal("Should fail all 3 jobs")
	}

	counts := must(queue.GetJobCounts(ctx))
	if counts.Failed != 3 {
		t.Fatalf("failed = %d, want 3", counts.Failed)
	}

	removed := must(queue.Clean(ctx, 0, 0, "failed"))
	if len(removed) != 3 {
		t.Fatalf("removed = %d, want 3", len(removed))
	}

	counts = must(queue.GetJobCounts(ctx))
	if counts.Failed != 0 {
		t.Fatalf("failed = %d, want 0", counts.Failed)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Manual processing: move_to_completed / move_to_failed
// ═══════════════════════════════════════════════════════════════════════════

func TestManualMoveToCompleted(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	// Worker with no processor (manual mode)
	worker := newTestWorker(t, name, w3NullProcessor, &bullmq.WorkerOptions{ManualStart: true})

	const token = "manual-token"
	job := must(worker.GetNextJob(ctx, token))
	if job == nil {
		t.Fatal("expected a job")
	}

	if !must(job.IsActive(ctx)) {
		t.Fatal("job should be active")
	}

	noErr(t, job.MoveToCompleted(ctx, "my return value"))

	if !must(job.IsCompleted(ctx)) {
		t.Fatal("job should be completed")
	}
	if job.ReturnValue != `"my return value"` {
		t.Fatalf("returnvalue = %q", job.ReturnValue)
	}

	counts := must(queue.GetJobCounts(ctx))
	if counts.Completed != 1 || counts.Active != 0 {
		t.Fatalf("counts = %+v, want completed=1 active=0", counts)
	}
}

func TestManualMoveToFailed(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, w3NullProcessor, &bullmq.WorkerOptions{ManualStart: true})

	const token = "manual-token"
	job := must(worker.GetNextJob(ctx, token))
	if job == nil {
		t.Fatal("expected a job")
	}

	if !must(job.IsActive(ctx)) {
		t.Fatal("job should be active")
	}

	noErr(t, job.MoveToFailed(ctx, "something went wrong"))

	if !must(job.IsFailed(ctx)) {
		t.Fatal("job should be failed")
	}

	counts := must(queue.GetJobCounts(ctx))
	if counts.Failed != 1 || counts.Active != 0 {
		t.Fatalf("counts = %+v, want failed=1 active=0", counts)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Job.promote() and Job.extend_lock()
// ═══════════════════════════════════════════════════════════════════════════

func TestJobPromoteDelayedToWaiting(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add a delayed job (delay 60 seconds - should not run naturally)
	job := must(queue.Add(ctx, "test", map[string]any{"promote": true}, &bullmq.JobOptions{
		Delay: 60 * time.Second,
	}))

	// Verify it's delayed
	if !must(job.IsDelayed(ctx)) {
		t.Fatal("job should be delayed")
	}

	// Promote it
	noErr(t, job.Promote(ctx))

	// It should now be waiting
	if !must(job.IsWaiting(ctx)) {
		t.Fatal("job should be waiting")
	}
}

func TestJobExtendLockManualProcessing(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{}, nil))

	worker := newTestWorker(t, name, w3NullProcessor, &bullmq.WorkerOptions{
		ManualStart:  true,
		LockDuration: 500 * time.Millisecond,
	})

	const token = "lock-test-token"
	job := must(worker.GetNextJob(ctx, token))
	if job == nil {
		t.Fatal("expected a job")
	}

	// Extend lock - should succeed
	noErr(t, job.ExtendLock(ctx, token, 5*time.Second))

	// Wait longer than original lock duration
	time.Sleep(600 * time.Millisecond)

	// Job should still be active (lock was extended)
	if !must(job.IsActive(ctx)) {
		t.Fatal("job should still be active")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Job.change_delay() and Job.change_priority()
// ═══════════════════════════════════════════════════════════════════════════

func TestJobChangeDelay(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add a job with 60s delay
	job := must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{
		Delay: 60 * time.Second,
	}))

	if !must(job.IsDelayed(ctx)) {
		t.Fatal("job should be delayed")
	}

	// Change delay to 100ms
	noErr(t, job.ChangeDelay(ctx, 100*time.Millisecond))

	// Still delayed but with shorter delay
	if !must(job.IsDelayed(ctx)) {
		t.Fatal("job should still be delayed")
	}

	// Wait for it to become available and be processed
	worker := newTestWorker(t, name, w3NullProcessor, &bullmq.WorkerOptions{DrainDelay: time.Second})

	if !w3WaitType(worker, 5*time.Second, bullmq.EventCompleted) {
		t.Fatal("Job should complete after reduced delay")
	}
}

func TestJobChangePriority(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add jobs: low priority first, then high priority
	must(queue.Add(ctx, "low", map[string]any{"order": 1}, &bullmq.JobOptions{Priority: 10}))

	jobHigh := must(queue.Add(ctx, "high", map[string]any{"order": 2}, &bullmq.JobOptions{Priority: 10}))

	// Change second job to highest priority
	noErr(t, jobHigh.ChangePriority(ctx, 1, false))

	// Process - the job_high should come first due to priority change
	worker := newTestWorker(t, name, w3NullProcessor, &bullmq.WorkerOptions{ManualStart: true})

	const token = "prio-token"
	firstJob := must(worker.GetNextJob(ctx, token))
	if firstJob == nil {
		t.Fatal("expected a job")
	}
	if firstJob.Name != "high" {
		t.Fatalf("first job = %q, want high", firstJob.Name)
	}
}

func TestQueueRetryJobs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add jobs that will fail
	for i := 0; i < 5; i++ {
		must(queue.Add(ctx, "fail-job", map[string]any{"i": i}, nil))
	}

	// Process jobs - all will fail
	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		return nil, errors.New("intentional")
	}, nil)

	// Wait for all jobs to fail
	if !w3WaitCount(worker, 5*time.Second, bullmq.EventFailed, 5) {
		t.Fatal("Should fail all 5 jobs")
	}

	noErr(t, worker.Close(5*time.Second))

	counts := must(queue.GetJobCounts(ctx))
	if counts.Failed != 5 {
		t.Fatalf("failed = %d, want 5", counts.Failed)
	}

	// Retry all failed jobs
	noErr(t, queue.RetryJobs(ctx, "failed", 1000, time.Time{}))

	counts = must(queue.GetJobCounts(ctx))
	if counts.Failed != 0 || counts.Waiting != 5 {
		t.Fatalf("counts = %+v, want failed=0 waiting=5", counts)
	}
}

func TestQueueRetryJobsCompleted(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add and process jobs
	for i := 0; i < 3; i++ {
		must(queue.Add(ctx, "job", map[string]any{"i": i}, nil))
	}

	worker := newTestWorker(t, name, func(context.Context, *bullmq.Job) (any, error) {
		return "done", nil
	}, nil)

	if !w3WaitCount(worker, 5*time.Second, bullmq.EventCompleted, 3) {
		t.Fatal("Should complete all 3 jobs")
	}

	noErr(t, worker.Close(5*time.Second))

	counts := must(queue.GetJobCounts(ctx))
	if counts.Completed != 3 {
		t.Fatalf("completed = %d, want 3", counts.Completed)
	}

	// Retry completed jobs (move back to wait)
	noErr(t, queue.RetryJobs(ctx, "completed", 1000, time.Time{}))

	counts = must(queue.GetJobCounts(ctx))
	if counts.Completed != 0 || counts.Waiting != 3 {
		t.Fatalf("counts = %+v, want completed=0 waiting=3", counts)
	}
}

func TestQueuePromoteJobs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add delayed jobs
	for i := 0; i < 5; i++ {
		must(queue.Add(ctx, "delayed-job", map[string]any{"i": i}, &bullmq.JobOptions{
			Delay: 60 * time.Second, // 60 seconds in the future
		}))
	}

	counts := must(queue.GetJobCounts(ctx))
	if counts.Delayed != 5 || counts.Waiting != 0 {
		t.Fatalf("counts = %+v, want delayed=5 waiting=0", counts)
	}

	// Promote all delayed jobs
	noErr(t, queue.PromoteJobs(ctx, 1000))

	counts = must(queue.GetJobCounts(ctx))
	if counts.Delayed != 0 || counts.Waiting != 5 {
		t.Fatalf("counts = %+v, want delayed=0 waiting=5", counts)
	}
}

func TestQueueGetJobLogs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add a job and log to it
	job := must(queue.Add(ctx, "log-job", map[string]any{"test": true}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		for _, row := range []string{"log entry 1", "log entry 2", "log entry 3"} {
			if _, err := job.Log(ctx, row); err != nil {
				t.Errorf("log: %v", err)
				return nil, err
			}
		}
		return nil, nil
	}, nil)

	// Wait for completion
	if !w3WaitType(worker, 5*time.Second, bullmq.EventCompleted) {
		t.Fatal("Job should complete")
	}

	noErr(t, worker.Close(5*time.Second))

	// Get all logs ascending
	logs, count, err := queue.GetJobLogs(ctx, job.ID, 0, -1, true)
	noErr(t, err)
	if count != 3 || len(logs) != 3 {
		t.Fatalf("logs = %v (count %d), want 3 entries", logs, count)
	}
	if logs[0] != "log entry 1" || logs[1] != "log entry 2" || logs[2] != "log entry 3" {
		t.Fatalf("ascending logs = %v", logs)
	}

	// Get logs descending
	logsDesc, countDesc, err := queue.GetJobLogs(ctx, job.ID, 0, -1, false)
	noErr(t, err)
	if countDesc != 3 || len(logsDesc) != 3 {
		t.Fatalf("logs = %v (count %d), want 3 entries", logsDesc, countDesc)
	}
	if logsDesc[0] != "log entry 3" || logsDesc[1] != "log entry 2" || logsDesc[2] != "log entry 1" {
		t.Fatalf("descending logs = %v", logsDesc)
	}

	// Get paginated logs
	logsPage, _, err := queue.GetJobLogs(ctx, job.ID, 0, 1, true)
	noErr(t, err)
	if len(logsPage) != 2 { // LRANGE 0 1 returns 2 elements
		t.Fatalf("paginated logs = %v, want 2 entries", logsPage)
	}
}

func TestQueueTrimEvents(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add and process many jobs to generate events
	for i := 0; i < 20; i++ {
		must(queue.Add(ctx, "event-job", map[string]any{"i": i}, nil))
	}

	worker := newTestWorker(t, name, w3NullProcessor, nil)

	// Wait for all jobs to complete
	if !w3WaitCount(worker, 10*time.Second, bullmq.EventCompleted, 20) {
		t.Fatal("Should complete all 20 jobs")
	}

	noErr(t, worker.Close(5*time.Second))

	// Trim events to 5
	// Note: XTRIM with ~ is approximate, so we just verify the call succeeds
	_, err := queue.TrimEvents(ctx, 5)
	noErr(t, err)

	// Verify the event stream length is bounded (~ is very approximate for small streams)
	length := must(queue.Client().XLen(ctx, queue.Keys().Events()).Result())
	// With ~ approximation, Redis may not trim much for small streams
	// The important thing is the API works without error
	if length <= 0 {
		t.Fatal("Should still have some events")
	}
}
