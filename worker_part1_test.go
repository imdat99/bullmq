package bullmq_test

// Port of bullmq_rust/tests/worker_test.rs (lines 1-3531): worker tests —
// processing, events, concurrency, remove-on-complete, return values.

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/bullmq"
)

// ── helpers (prefix w1) ─────────────────────────────────────────────────────

// w1Send sends v on ch unless ctx is done first (a processor must never block
// past its job's cancellation).
func w1Send[T any](ctx context.Context, ch chan<- T, v T) {
	select {
	case ch <- v:
	case <-ctx.Done():
	}
}

// w1Recv receives from ch within timeout or fails the test with msg.
func w1Recv[T any](t *testing.T, ch <-chan T, timeout time.Duration, msg string) T {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case v := <-ch:
		return v
	case <-timer.C:
		t.Fatalf("timeout: %s", msg)
		panic("unreachable")
	}
}

// w1WaitEvents waits until n events of type typ were read from w within
// timeout and returns them; it fails the test with msg on timeout.
func w1WaitEvents(t *testing.T, w *bullmq.Worker, timeout time.Duration, typ bullmq.WorkerEventType, n int, msg string) []bullmq.WorkerEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var got []bullmq.WorkerEvent
	for len(got) < n {
		ev, ok := w.NextEvent(ctx)
		if !ok {
			t.Fatalf("timeout: %s (got %d/%d %s events)", msg, len(got), n, typ)
		}
		if ev.Type == typ {
			got = append(got, ev)
		}
	}
	return got
}

// w1Data decodes the job data as a JSON object.
func w1Data(job *bullmq.Job) map[string]any {
	var m map[string]any
	_ = job.DecodeData(&m)
	return m
}

// w1JSONEqual reports whether raw encodes the same JSON value as want.
func w1JSONEqual(raw []byte, want any) bool {
	wantRaw, err := json.Marshal(want)
	if err != nil {
		return false
	}
	var a, b any
	if json.Unmarshal(raw, &a) != nil || json.Unmarshal(wantRaw, &b) != nil {
		return false
	}
	return reflect.DeepEqual(a, b)
}

// w1Counts returns the job counts of q.
func w1Counts(t *testing.T, q *bullmq.Queue) bullmq.JobCounts {
	t.Helper()
	c, err := q.GetJobCounts(context.Background())
	noErr(t, err)
	return c
}

// w1Close closes w with the given timeout and fails the test on error.
func w1Close(t *testing.T, w *bullmq.Worker, timeout time.Duration) {
	t.Helper()
	noErr(t, w.Close(timeout))
}

// ═══════════════════════════════════════════════════════════════════════════
// Basic Processing
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkerProcessesJob(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "process-me", map[string]any{"x": 42}, nil))

	tx := make(chan string, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		w1Send(ctx, tx, job.ID)
		return map[string]any{"processed": true}, nil
	}, nil)

	processedID := w1Recv(t, tx, 10*time.Second, "waiting for job")
	if processedID == "" {
		t.Fatal("processed id is empty")
	}

	w1Close(t, worker, 5*time.Second)
}

// Regression test for https://github.com/taskforcesh/bullmq/issues/4512
//
// An idle worker must wake up and process a newly added job promptly via the
// marker -> BZPOPMIN path, rather than waiting for the periodic empty-queue
// re-poll (`drain_delay`, default 5s).
func TestIdleWorkerPicksUpNewJobQuickly(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	tx := make(chan string, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		w1Send(ctx, tx, job.ID)
		return map[string]any{"processed": true}, nil
	}, nil)

	// Wait for the worker to reach idle/parked state before adding a job.
	w1WaitEvents(t, worker, 3*time.Second, bullmq.EventDrained, 1, "waiting for worker to become drained")

	addedAt := time.Now()
	must(queue.Add(ctx, "wake-up", map[string]any{"hello": "world"}, nil))

	processedID := w1Recv(t, tx, 10*time.Second, "waiting for job")
	latency := time.Since(addedAt)

	if processedID == "" {
		t.Fatal("processed id is empty")
	}
	// The blocking watcher should wake the worker well before the 5s poll.
	if latency >= 3*time.Second {
		t.Fatalf("idle worker took %v to pick up a new job; expected a near-instant "+
			"wake-up via BZPOPMIN, not the 5s empty-queue poll", latency)
	}

	w1Close(t, worker, 5*time.Second)
}

// Regression test: a worker must recover on its own after its Redis
// connections are dropped (network blip, Redis restart, failover).
// `CLIENT KILL` simulates the dropped socket deterministically. The test runs
// in its own logical database so the kill does not drop the connections of
// other tests running in parallel against the same server.
func TestWorkerRecoversAfterConnectionIsKilled(t *testing.T) {
	const killDB = 14
	ctx := context.Background()
	name := testQueueName()
	connOpts := isolatedConnection(killDB)

	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: connOpts})

	tx := make(chan string, 8)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		w1Send(ctx, tx, job.Name)
		return map[string]any{"processed": true}, nil
	}, &bullmq.WorkerOptions{Connection: connOpts})

	must(queue.Add(ctx, "before", map[string]any{}, nil))
	if first := w1Recv(t, tx, 10*time.Second, "waiting for the first job"); first != "before" {
		t.Fatalf("first = %q, want before", first)
	}

	// Drop every connection of this test's database, as a Redis restart would.
	if killed := killConnectionsInDB(t, killDB); killed <= 0 {
		t.Fatal("expected CLIENT KILL to drop some connections")
	}

	// The worker must reconnect by itself and keep processing.
	eventually(t, 20*time.Second, func() bool {
		return queue.Connection().Ping(ctx) == nil
	}, "queue did not reconnect")
	must(queue.Add(ctx, "after", map[string]any{}, nil))
	if second := w1Recv(t, tx, 20*time.Second, "worker did not recover after its connection was killed"); second != "after" {
		t.Fatalf("second = %q, want after", second)
	}

	w1Close(t, worker, 5*time.Second)
}

// Stress test for the marker -> `BZPOPMIN` wake-up path.
//
// This drives many idle -> add -> process cycles, varying the delay between the
// worker going idle and the job being added (nanosecond-derived jitter) so the
// marker sometimes lands right as the worker re-parks. Every job must be picked
// up promptly; any single stall near `drain_delay` fails the test.
func TestIdleWorkerWakeupIsNotLostUnderRace(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// One job processed -> one message. Buffer a few so a fast processor never
	// blocks on send.
	tx := make(chan string, 8)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		w1Send(ctx, tx, job.ID)
		return map[string]any{"processed": true}, nil
	}, &bullmq.WorkerOptions{Concurrency: 1})

	// Wait for the worker to reach its idle/parked state before the first add.
	w1WaitEvents(t, worker, 3*time.Second, bullmq.EventDrained, 1, "waiting for worker to become drained")

	const iterations = 60
	var maxLatency time.Duration
	slowCount := 0

	for i := range iterations {
		// Jitter the gap between "worker is idle" and "job added" (0..7ms).
		jitter := time.Duration(time.Now().Nanosecond()%7_000) * time.Microsecond
		time.Sleep(jitter)

		addedAt := time.Now()
		must(queue.Add(ctx, "race", map[string]any{"i": i}, nil))

		// Tight timeout: a healthy wake-up is ~instant.
		select {
		case <-tx:
		case <-time.After(4 * time.Second):
			t.Fatalf("iteration %d: job was not processed within 4s — the marker "+
				"wake-up was likely lost, leaving the worker parked until the "+
				"%ds empty-queue poll", i, 5)
		}

		latency := time.Since(addedAt)
		maxLatency = max(maxLatency, latency)
		if latency > time.Second {
			slowCount++
		}
	}

	if maxLatency >= 3*time.Second {
		t.Fatalf("worst-case pickup latency was %v over %d iterations (%d slower than 1s); "+
			"a wake-up was likely lost", maxLatency, iterations, slowCount)
	}

	w1Close(t, worker, 5*time.Second)
}

func TestWorkerProcessesJobsSerially(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const numJobs = 10
	for i := 1; i <= numJobs; i++ {
		must(queue.Add(ctx, "serial", map[string]any{"num": i}, nil))
	}

	var counter atomic.Int32
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		counter.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: 1})

	time.Sleep(10 * time.Second)
	if got := counter.Load(); got != numJobs {
		t.Fatalf("counter = %d, want %d", got, numJobs)
	}

	w1Close(t, worker, 5*time.Second)
}

func TestWorkerAsyncProcessing(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if got := w1Data(job)["foo"]; got != "bar" {
			t.Errorf("data.foo = %v, want bar", got)
		}
		time.Sleep(100 * time.Millisecond)
		return "async result", nil
	}, nil)

	ev := w1WaitEvents(t, worker, 5*time.Second, bullmq.EventCompleted, 1, "waiting for completed event")[0]
	if !w1JSONEqual(ev.Result, "async result") {
		t.Fatalf("result = %s, want \"async result\"", ev.Result)
	}

	w1Close(t, worker, 5*time.Second)
}

func TestProcessesJobsAddedBeforeWorker(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	for i := range 4 {
		must(queue.Add(ctx, "test", map[string]any{"bar": "baz" + strconv.Itoa(i)}, nil))
	}

	tx := make(chan struct{}, 10)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		w1Send(ctx, tx, struct{}{})
		return nil, nil
	}, nil)

	count := 0
	deadline := time.After(10 * time.Second)
	for count < 4 {
		select {
		case <-tx:
			count++
		case <-deadline:
			t.Fatal("timeout waiting for pre-added jobs to process")
		}
	}
	if count != 4 {
		t.Fatalf("count = %d, want 4", count)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Concurrency
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkerConcurrency(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	for i := range 5 {
		must(queue.Add(ctx, "concurrent-"+strconv.Itoa(i), map[string]any{}, nil))
	}

	var counter atomic.Int32
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		counter.Add(1)
		time.Sleep(100 * time.Millisecond)
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: 3})

	time.Sleep(5 * time.Second)
	if got := counter.Load(); got != 5 {
		t.Fatalf("counter = %d, want 5", got)
	}

	w1Close(t, worker, 5*time.Second)
}

func TestWorkerSlowJobDoesNotBlockOthers(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "slow", map[string]any{"type": "slow"}, nil))
	must(queue.Add(ctx, "fast", map[string]any{"type": "fast"}, nil))

	tx := make(chan string, 10)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if w1Data(job)["type"] == "slow" {
			time.Sleep(500 * time.Millisecond)
		}
		w1Send(ctx, tx, job.Name)
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: 2})

	var completed []string
	deadline := time.After(5 * time.Second)
	for len(completed) < 2 {
		select {
		case n := <-tx:
			completed = append(completed, n)
		case <-deadline:
			t.Fatal("timeout waiting for jobs")
		}
	}

	if completed[0] != "fast" || completed[1] != "slow" {
		t.Fatalf("completion order = %v, want [fast slow]", completed)
	}

	w1Close(t, worker, 5*time.Second)
}

func TestWorkerMultipleConcurrentWorkers(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const numJobs = 20
	for i := range numJobs {
		must(queue.Add(ctx, "multi-"+strconv.Itoa(i), map[string]any{}, nil))
	}

	var counter atomic.Int32
	workers := make([]*bullmq.Worker, 0, 2)
	for range 2 {
		w := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
			counter.Add(1)
			time.Sleep(50 * time.Millisecond)
			return nil, nil
		}, &bullmq.WorkerOptions{Concurrency: 5})
		workers = append(workers, w)
	}

	time.Sleep(10 * time.Second)
	if got := counter.Load(); got != numJobs {
		t.Fatalf("counter = %d, want %d", got, numJobs)
	}

	for _, w := range workers {
		w1Close(t, w, 5*time.Second)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Events
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkerCompletedEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "event-test", map[string]any{"val": 99}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return map[string]any{"done": true}, nil
	}, nil)

	ev := w1WaitEvents(t, worker, 10*time.Second, bullmq.EventCompleted, 1, "waiting for completed event")[0]
	if ev.JobID == "" {
		t.Fatal("completed event job id is empty")
	}
	if !w1JSONEqual(ev.Result, map[string]any{"done": true}) {
		t.Fatalf("result = %s, want {\"done\":true}", ev.Result)
	}

	w1Close(t, worker, 5*time.Second)
}

func TestWorkerFailedEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "fail-event-test", map[string]any{}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, bullmq.NewUnrecoverableError("boom")
	}, nil)

	ev := w1WaitEvents(t, worker, 10*time.Second, bullmq.EventFailed, 1, "waiting for failed event")[0]
	if ev.JobID == "" {
		t.Fatal("failed event job id is empty")
	}
	if ev.Err == nil || !strings.Contains(ev.Err.Error(), "boom") {
		t.Fatalf("failed event error = %v, want to contain boom", ev.Err)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Job Failure
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkerJobFailure(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "fail-me", map[string]any{}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, bullmq.NewUnrecoverableError("intentional failure")
	}, nil)

	time.Sleep(5 * time.Second)

	w1Close(t, worker, 5*time.Second)
}

func TestWorkerJobStateCompleted(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "state-complete", map[string]any{}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return "ok", nil
	}, nil)

	w1WaitEvents(t, worker, 10*time.Second, bullmq.EventCompleted, 1, "waiting for completed event")

	if state := must(queue.GetJobState(ctx, job.ID)); state != bullmq.StateCompleted {
		t.Fatalf("state = %s, want completed", state)
	}
	counts := w1Counts(t, queue)
	if counts.Completed != 1 || counts.Waiting != 0 {
		t.Fatalf("counts = %+v, want completed=1 waiting=0", counts)
	}

	w1Close(t, worker, 5*time.Second)
}

func TestWorkerJobStateFailed(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "state-fail", map[string]any{}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, bullmq.NewUnrecoverableError("crash")
	}, nil)

	w1WaitEvents(t, worker, 10*time.Second, bullmq.EventFailed, 1, "waiting for failed event")

	if state := must(queue.GetJobState(ctx, job.ID)); state != bullmq.StateFailed {
		t.Fatalf("state = %s, want failed", state)
	}
	if counts := w1Counts(t, queue); counts.Failed != 1 {
		t.Fatalf("failed count = %d, want 1", counts.Failed)
	}

	w1Close(t, worker, 5*time.Second)
}

func TestGetJobFailedReason(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "fail-reason", map[string]any{}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, bullmq.NewUnrecoverableError("something went wrong")
	}, nil)

	w1WaitEvents(t, worker, 10*time.Second, bullmq.EventFailed, 1, "waiting for failed event")

	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched == nil {
		t.Fatal("job not found")
	}
	if !strings.Contains(fetched.FailedReason, "something went wrong") {
		t.Fatalf("Expected failed reason to contain 'something went wrong', got: '%s'", fetched.FailedReason)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Cancellation / Pause / Close
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkerCancellation(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "cancel-me", map[string]any{}, nil))

	tx := make(chan bool, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		select {
		case <-ctx.Done():
			tx <- true
			return nil, bullmq.ErrWorkerClosed
		case <-time.After(30 * time.Second):
			tx <- false
			return nil, nil
		}
	}, nil)

	time.Sleep(2 * time.Second)

	worker.CancelAllJobs()

	if wasCancelled := w1Recv(t, tx, 5*time.Second, "waiting for cancellation"); !wasCancelled {
		t.Fatal("job was not cancelled")
	}

	w1Close(t, worker, 5*time.Second)
}

func TestWorkerPauseResume(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	var counter atomic.Int32
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		counter.Add(1)
		return nil, nil
	}, nil)

	worker.Pause()
	if !worker.IsPaused() {
		t.Fatal("worker should be paused")
	}

	must(queue.Add(ctx, "paused-job", map[string]any{}, nil))

	time.Sleep(time.Second)
	if got := counter.Load(); got != 0 {
		t.Fatalf("counter while paused = %d, want 0", got)
	}

	worker.Resume()
	time.Sleep(3 * time.Second)
	if got := counter.Load(); got != 1 {
		t.Fatalf("counter after resume = %d, want 1", got)
	}

	w1Close(t, worker, 5*time.Second)
}

func TestWorkerGracefulClose(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "slow-job", map[string]any{}, nil))

	tx := make(chan string, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		time.Sleep(500 * time.Millisecond)
		w1Send(ctx, tx, job.ID)
		return nil, nil
	}, nil)

	time.Sleep(200 * time.Millisecond)

	w1Close(t, worker, 5*time.Second)

	w1Recv(t, tx, time.Second, "job did not finish during graceful close")
}

func TestWorkerDoesNotProcessWhenAutorunFalse(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "no-autorun", map[string]any{}, nil))

	var counter atomic.Int32
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		counter.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{ManualStart: true})

	time.Sleep(2 * time.Second)
	if got := counter.Load(); got != 0 {
		t.Fatalf("counter before run = %d, want 0", got)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- worker.Run() }()

	time.Sleep(3 * time.Second)
	if got := counter.Load(); got != 1 {
		t.Fatalf("counter after run = %d, want 1", got)
	}

	w1Close(t, worker, 5*time.Second)
	noErr(t, <-runErr)
}

// ═══════════════════════════════════════════════════════════════════════════
// LIFO ordering
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkerLifoProcessingOrder(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	noErr(t, queue.Pause(ctx))

	for i := range 4 {
		must(queue.Add(ctx, "lifo-"+strconv.Itoa(i), map[string]any{"order": i}, &bullmq.JobOptions{LIFO: true}))
	}

	var (
		mu    sync.Mutex
		order []int
	)
	done := make(chan struct{}, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		idx, _ := w1Data(job)["order"].(float64)
		mu.Lock()
		order = append(order, int(idx))
		n := len(order)
		mu.Unlock()
		if n == 4 {
			select {
			case done <- struct{}{}:
			default:
			}
		}
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: 1})

	noErr(t, queue.Resume(ctx))

	w1Recv(t, done, 10*time.Second, "waiting for LIFO jobs")

	mu.Lock()
	got := slices.Clone(order)
	mu.Unlock()
	if !slices.Equal(got, []int{3, 2, 1, 0}) {
		t.Fatalf("processed order = %v, want [3 2 1 0]", got)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Return Values
// ═══════════════════════════════════════════════════════════════════════════

func TestReturnValueStoredInDb(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if got := w1Data(job)["foo"]; got != "bar" {
			t.Errorf("data.foo = %v, want bar", got)
		}
		return 42, nil
	}, nil)

	w1WaitEvents(t, worker, 5*time.Second, bullmq.EventCompleted, 1, "waiting for completed event")

	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched == nil {
		t.Fatal("job not found")
	}
	if fetched.ReturnValue != "42" {
		t.Fatalf("returnvalue = %q, want 42", fetched.ReturnValue)
	}

	w1Close(t, worker, 5*time.Second)
}

func TestReturnValueStringStoredInDb(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"testing": true}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return "a very dignified string", nil
	}, nil)

	w1WaitEvents(t, worker, 5*time.Second, bullmq.EventCompleted, 1, "waiting for completed event")

	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched == nil {
		t.Fatal("job not found")
	}
	if fetched.ReturnValue != `"a very dignified string"` {
		t.Fatalf("returnvalue = %q, want %q", fetched.ReturnValue, `"a very dignified string"`)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Timestamps (processedOn / finishedOn)
// ═══════════════════════════════════════════════════════════════════════════

func TestProcessedOnSetAfterProcessing(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	}, nil)

	w1WaitEvents(t, worker, 5*time.Second, bullmq.EventCompleted, 1, "waiting for completed event")

	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched == nil {
		t.Fatal("job not found")
	}
	if fetched.ProcessedOn == 0 {
		t.Fatal("processedOn not set")
	}
	if fetched.FinishedOn == 0 {
		t.Fatal("finishedOn not set")
	}
	if fetched.ProcessedOn < fetched.Timestamp {
		t.Fatalf("processedOn %d < timestamp %d", fetched.ProcessedOn, fetched.Timestamp)
	}
	if fetched.FinishedOn < fetched.ProcessedOn {
		t.Fatalf("finishedOn %d < processedOn %d", fetched.FinishedOn, fetched.ProcessedOn)
	}

	w1Close(t, worker, 5*time.Second)
}

func TestGetJobFinishedOnAfterCompletion(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "finished-on-test", map[string]any{}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return "result", nil
	}, nil)

	w1WaitEvents(t, worker, 10*time.Second, bullmq.EventCompleted, 1, "waiting for completed event")

	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched == nil {
		t.Fatal("job not found")
	}
	if fetched.ProcessedOn == 0 || fetched.FinishedOn == 0 {
		t.Fatalf("processedOn=%d finishedOn=%d, want both set", fetched.ProcessedOn, fetched.FinishedOn)
	}
	if fetched.FinishedOn < fetched.ProcessedOn {
		t.Fatalf("finishedOn %d < processedOn %d", fetched.FinishedOn, fetched.ProcessedOn)
	}

	w1Close(t, worker, 5*time.Second)
}

func TestGetJobCountsAfterProcessing(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	for i := range 3 {
		must(queue.Add(ctx, "count-"+strconv.Itoa(i), map[string]any{}, nil))
	}

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	}, nil)

	time.Sleep(5 * time.Second)

	counts := w1Counts(t, queue)
	if counts.Completed != 3 || counts.Waiting != 0 {
		t.Fatalf("counts = %+v, want completed=3 waiting=0", counts)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// RemoveOnComplete / RemoveOnFail
// ═══════════════════════════════════════════════════════════════════════════

func TestRemoveOnCompleteTrue(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{RemoveOnComplete: bullmq.RemoveAll()}))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	}, nil)

	w1WaitEvents(t, worker, 5*time.Second, bullmq.EventCompleted, 1, "waiting for completed event")

	time.Sleep(100 * time.Millisecond)

	if fetched := must(queue.GetJob(ctx, job.ID)); fetched != nil {
		t.Fatal("Job should have been removed")
	}
	if counts := w1Counts(t, queue); counts.Completed != 0 {
		t.Fatalf("completed = %d, want 0", counts.Completed)
	}

	w1Close(t, worker, 5*time.Second)
}

func TestRemoveOnFailTrue(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{RemoveOnFail: bullmq.RemoveAll()}))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, bullmq.NewUnrecoverableError("intentional")
	}, nil)

	w1WaitEvents(t, worker, 5*time.Second, bullmq.EventFailed, 1, "waiting for failed event")

	time.Sleep(100 * time.Millisecond)

	if fetched := must(queue.GetJob(ctx, job.ID)); fetched != nil {
		t.Fatal("Job should have been removed after failure")
	}
	if counts := w1Counts(t, queue); counts.Failed != 0 {
		t.Fatalf("failed = %d, want 0", counts.Failed)
	}

	w1Close(t, worker, 5*time.Second)
}

func TestRemoveOnCompleteKeepsSpecifiedCount(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	for i := range 5 {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, &bullmq.JobOptions{RemoveOnComplete: bullmq.KeepLast(3)}))
	}

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	}, nil)

	w1WaitEvents(t, worker, 10*time.Second, bullmq.EventCompleted, 5, "waiting for 5 completed events")

	time.Sleep(100 * time.Millisecond)

	if counts := w1Counts(t, queue); counts.Completed > 3 {
		t.Fatalf("Expected at most 3 completed jobs, got %d", counts.Completed)
	}

	w1Close(t, worker, 5*time.Second)
}

func TestWorkerLevelRemoveOnComplete(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	}, &bullmq.WorkerOptions{RemoveOnComplete: bullmq.RemoveAll()})

	w1WaitEvents(t, worker, 5*time.Second, bullmq.EventCompleted, 1, "waiting for completed event")

	time.Sleep(100 * time.Millisecond)

	if fetched := must(queue.GetJob(ctx, job.ID)); fetched != nil {
		t.Fatal("Job should be removed by worker-level removeOnComplete")
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Process several jobs serially (Node.js: 'process several jobs serially')
// ═══════════════════════════════════════════════════════════════════════════

func TestProcessSeveralJobsSeriallyWithDataCheck(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const maxJobs = 35
	for i := 1; i <= maxJobs; i++ {
		must(queue.Add(ctx, "test", map[string]any{"foo": "bar", "num": i}, nil))
	}

	var counter atomic.Int32
	counter.Store(1)
	tx := make(chan struct{}, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		expected := counter.Add(1) - 1
		data := w1Data(job)
		if data["foo"] != "bar" {
			t.Errorf("data.foo = %v, want bar", data["foo"])
		}
		if num, _ := data["num"].(float64); int32(num) != expected {
			t.Errorf("data.num = %v, want %d", data["num"], expected)
		}
		if expected == maxJobs {
			w1Send(ctx, tx, struct{}{})
		}
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: 1})

	w1Recv(t, tx, 15*time.Second, "waiting for all 35 jobs")

	if !worker.IsRunning() {
		t.Fatal("worker should be running")
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Process job that returns data (Node.js: 'process a job that returns data
// in the process handler')
// ═══════════════════════════════════════════════════════════════════════════

func TestProcessJobReturnsData(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if got := w1Data(job)["foo"]; got != "bar" {
			t.Errorf("data.foo = %v, want bar", got)
		}
		return 37, nil
	}, nil)

	ev := w1WaitEvents(t, worker, 5*time.Second, bullmq.EventCompleted, 1, "waiting for completed event")[0]
	if !w1JSONEqual(ev.Result, 37) {
		t.Fatalf("result = %s, want 37", ev.Result)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Process a job that throws an exception (Node.js: 'process a job that
// throws an exception')
// ═══════════════════════════════════════════════════════════════════════════

func TestProcessJobThrowsException(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if got := w1Data(job)["foo"]; got != "bar" {
			t.Errorf("data.foo = %v, want bar", got)
		}
		return nil, bullmq.NewUnrecoverableError("Job Failed")
	}, nil)

	ev := w1WaitEvents(t, worker, 5*time.Second, bullmq.EventFailed, 1, "waiting for failed event")[0]
	if ev.JobID != job.ID {
		t.Fatalf("failed job id = %q, want %q", ev.JobID, job.ID)
	}
	if ev.Err == nil || !strings.Contains(ev.Err.Error(), "Job Failed") {
		t.Fatalf("failed error = %v, want to contain 'Job Failed'", ev.Err)
	}

	// Verify finishedOn is set
	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched == nil {
		t.Fatal("job not found")
	}
	if fetched.FinishedOn == 0 {
		t.Fatal("finishedOn not set")
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Process a job that returns a rejected promise (Node.js: 'process a job
// that returns a rejected promise')
// ═══════════════════════════════════════════════════════════════════════════

func TestProcessJobReturnsError(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if got := w1Data(job)["foo"]; got != "bar" {
			t.Errorf("data.foo = %v, want bar", got)
		}
		return nil, bullmq.NewUnrecoverableError("rejected promise")
	}, nil)

	ev := w1WaitEvents(t, worker, 5*time.Second, bullmq.EventFailed, 1, "waiting for failed event")[0]
	if ev.Err == nil || !strings.Contains(ev.Err.Error(), "rejected promise") {
		t.Fatalf("failed error = %v, want to contain 'rejected promise'", ev.Err)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Concurrency respecting (Node.js: 'should process job respecting the
// concurrency set')
// ═══════════════════════════════════════════════════════════════════════════

func TestProcessJobRespectingConcurrency(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const numJobs = 8
	const concurrency = 4

	for range numJobs {
		must(queue.Add(ctx, "test", map[string]any{}, nil))
	}

	var currentProcessing, maxConcurrent, completedCount atomic.Int32
	tx := make(chan struct{}, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		current := currentProcessing.Add(1)
		// Update max observed concurrency
		for {
			m := maxConcurrent.Load()
			if current <= m || maxConcurrent.CompareAndSwap(m, current) {
				break
			}
		}

		time.Sleep(100 * time.Millisecond)

		currentProcessing.Add(-1)
		if done := completedCount.Add(1); done == numJobs {
			w1Send(ctx, tx, struct{}{})
		}
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: concurrency})

	w1Recv(t, tx, 10*time.Second, "waiting for all jobs")

	m := maxConcurrent.Load()
	if m > concurrency {
		t.Fatalf("Max concurrency %d exceeded limit %d", m, concurrency)
	}
	if m <= 1 {
		t.Fatal("Should have processed multiple jobs concurrently")
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Run job in sequence with concurrency 1 (Node.js: 'should run job in
// sequence if I specify a concurrency of 1')
// ═══════════════════════════════════════════════════════════════════════════

func TestRunJobInSequenceConcurrency1(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{}, nil))
	must(queue.Add(ctx, "test", map[string]any{}, nil))

	var processing atomic.Bool
	var completed atomic.Int32
	tx := make(chan struct{}, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		// Assert no other job is being processed
		if processing.Swap(true) {
			t.Error("Two jobs processed concurrently with concurrency=1")
		}
		time.Sleep(50 * time.Millisecond)
		processing.Store(false)
		if completed.Add(1) == 2 {
			w1Send(ctx, tx, struct{}{})
		}
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: 1})

	w1Recv(t, tx, 5*time.Second, "waiting for both jobs")

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Should not keep active jobs after completion (Node.js: 'should not keep
// active jobs')
// ═══════════════════════════════════════════════════════════════════════════

func TestShouldNotKeepActiveJobs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	for range 10 {
		must(queue.Add(ctx, "test", map[string]any{}, nil))
	}

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		time.Sleep(10 * time.Millisecond)
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: 3})

	// Wait for all to complete
	w1WaitEvents(t, worker, 10*time.Second, bullmq.EventCompleted, 10, "waiting for 10 completed events")

	time.Sleep(100 * time.Millisecond)

	if counts := w1Counts(t, queue); counts.Active != 0 {
		t.Fatalf("active = %d, want 0", counts.Active)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Automatically retry a failed job (Node.js: 'should automatically retry a
// failed job if attempts is bigger than 1')
// ═══════════════════════════════════════════════════════════════════════════

func TestAutoRetryFailedJobWithAttempts(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{Attempts: 3}))

	var tries atomic.Int32
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if attempt := tries.Add(1) - 1; attempt < 1 {
			return nil, errors.New("Not yet!")
		}
		return nil, nil
	}, nil)

	w1WaitEvents(t, worker, 10*time.Second, bullmq.EventCompleted, 1, "waiting for completed after retry")

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Should not retry more than attempts times (Node.js: 'should not retry a
// failed job more than the number of given attempts times')
// ═══════════════════════════════════════════════════════════════════════════

func TestNotRetryMoreThanAttempts(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{Attempts: 3}))

	var tries atomic.Int32
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		tries.Add(1)
		return nil, errors.New("Not yet!")
	}, nil)

	// Wait for 3 failed events
	w1WaitEvents(t, worker, 10*time.Second, bullmq.EventFailed, 3, "waiting for 3 failed events")

	time.Sleep(200 * time.Millisecond)

	if state := must(queue.GetJobState(ctx, job.ID)); state != bullmq.StateFailed {
		t.Fatalf("state = %s, want failed", state)
	}
	// Should have tried exactly 3 times
	if got := tries.Load(); got != 3 {
		t.Fatalf("tries = %d, want 3", got)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Retry with exponential backoff (Node.js: 'should retry a job after a
// delay if an exponential backoff is given')
// ═══════════════════════════════════════════════════════════════════════════

func TestRetryExponentialBackoff(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	start := time.Now()
	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.ExponentialBackoff(100 * time.Millisecond),
	}))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if job.AttemptsMade < 2 {
			return nil, errors.New("Not yet!")
		}
		return nil, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	w1WaitEvents(t, worker, 10*time.Second, bullmq.EventCompleted, 1, "waiting for completed event")

	// Exponential: 100ms + 200ms = 300ms minimum
	if elapsed := time.Since(start); elapsed < 200*time.Millisecond {
		t.Fatalf("Should have waited for backoff delays, elapsed: %v", elapsed)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Process priority jobs (Node.js: 'should process jobs by priority')
// ═══════════════════════════════════════════════════════════════════════════

func TestProcessJobsByPriority(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add jobs with different priorities (lower number = higher priority)
	// Add them in reverse priority order
	for _, p := range []struct{ idx, prio int }{{1, 10}, {2, 5}, {3, 1}} {
		must(queue.Add(ctx, "test", map[string]any{"idx": p.idx}, &bullmq.JobOptions{Priority: p.prio}))
	}

	tx := make(chan int, 10)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		idx, _ := w1Data(job)["idx"].(float64)
		w1Send(ctx, tx, int(idx))
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: 1})

	var order []int
	deadline := time.After(5 * time.Second)
	for len(order) < 3 {
		select {
		case idx := <-tx:
			order = append(order, idx)
		case <-deadline:
			t.Fatal("timeout")
		}
	}

	// Priority 1 (idx=3) should be first, then 5 (idx=2), then 10 (idx=1)
	if !slices.Equal(order, []int{3, 2, 1}) {
		t.Fatalf("order = %v, want [3 2 1]", order)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Pick standard job without delay (Node.js: 'pick standard job without
// delay')
// ═══════════════════════════════════════════════════════════════════════════

func TestPickStandardJobWithoutDelay(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add a delayed job, then a standard job
	must(queue.Add(ctx, "delayed", map[string]any{"type": "delayed"}, &bullmq.JobOptions{Delay: 2000 * time.Millisecond}))
	must(queue.Add(ctx, "standard", map[string]any{"type": "standard"}, nil))

	tx := make(chan string, 10)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		typ, _ := w1Data(job)["type"].(string)
		w1Send(ctx, tx, typ)
		return nil, nil
	}, nil)

	// Standard job should be picked first
	if first := w1Recv(t, tx, 2*time.Second, "waiting for first job"); first != "standard" {
		t.Fatalf("first = %q, want standard", first)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Remove job after completed if removeOnComplete (worker default)
// (Node.js: 'should remove a job after completed if the default job options
// specify removeOnComplete')
// ═══════════════════════════════════════════════════════════════════════════

func TestRemoveOnCompleteFromWorkerDefault(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if got := w1Data(job)["foo"]; got != "bar" {
			t.Errorf("data.foo = %v, want bar", got)
		}
		return nil, nil
	}, &bullmq.WorkerOptions{RemoveOnComplete: bullmq.RemoveAll()})

	w1WaitEvents(t, worker, 5*time.Second, bullmq.EventCompleted, 1, "waiting for completed event")

	time.Sleep(100 * time.Millisecond)

	if fetched := must(queue.GetJob(ctx, job.ID)); fetched != nil {
		t.Fatal("job should have been removed")
	}
	if counts := w1Counts(t, queue); counts.Completed != 0 {
		t.Fatalf("completed = %d, want 0", counts.Completed)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Remove on fail from worker default (Node.js: 'should remove a job after
// fail if the default job options specify removeOnFail')
// ═══════════════════════════════════════════════════════════════════════════

func TestRemoveOnFailFromWorkerDefault(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, bullmq.NewUnrecoverableError("error")
	}, &bullmq.WorkerOptions{RemoveOnFail: bullmq.RemoveAll()})

	w1WaitEvents(t, worker, 5*time.Second, bullmq.EventFailed, 1, "waiting for failed event")

	time.Sleep(100 * time.Millisecond)

	if fetched := must(queue.GetJob(ctx, job.ID)); fetched != nil {
		t.Fatal("job should have been removed")
	}
	if counts := w1Counts(t, queue); counts.Failed != 0 {
		t.Fatalf("failed = %d, want 0", counts.Failed)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Keep specified number of jobs with removeOnFail (Node.js: 'should keep
// specified number of jobs after completed with removeOnFail')
// ═══════════════════════════════════════════════════════════════════════════

func TestKeepSpecifiedCountRemoveOnFail(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const keepJobs = 3
	const totalJobs = 6

	for i := range totalJobs {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, &bullmq.JobOptions{RemoveOnFail: bullmq.KeepLast(keepJobs)}))
	}

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, bullmq.NewUnrecoverableError("fail")
	}, nil)

	w1WaitEvents(t, worker, 10*time.Second, bullmq.EventFailed, totalJobs, "waiting for failed events")

	time.Sleep(100 * time.Millisecond)

	if counts := w1Counts(t, queue); counts.Failed > keepJobs {
		t.Fatalf("Expected at most %d failed jobs, got %d", keepJobs, counts.Failed)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Remove all jobs when count is 0 with removeOnFail (Node.js: 'should
// remove all jobs when count is 0 with removeOnFail')
// ═══════════════════════════════════════════════════════════════════════════

func TestRemoveAllJobsCount0RemoveOnFail(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	for i := range 5 {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, &bullmq.JobOptions{RemoveOnFail: bullmq.RemoveAll()}))
	}

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, bullmq.NewUnrecoverableError("fail")
	}, nil)

	w1WaitEvents(t, worker, 10*time.Second, bullmq.EventFailed, 5, "waiting for failed events")

	time.Sleep(100 * time.Millisecond)

	if counts := w1Counts(t, queue); counts.Failed != 0 {
		t.Fatalf("failed = %d, want 0", counts.Failed)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Saves failedReason (Node.js: 'saves failedReason')
// ═══════════════════════════════════════════════════════════════════════════

func TestSavesFailedReason(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{Attempts: 3}))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, errors.New("my error message")
	}, nil)

	// Wait for all 3 attempts to fail
	w1WaitEvents(t, worker, 10*time.Second, bullmq.EventFailed, 3, "waiting for 3 failed events")

	time.Sleep(200 * time.Millisecond)

	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched == nil {
		t.Fatal("job not found")
	}
	if !strings.Contains(fetched.FailedReason, "my error message") {
		t.Fatalf("Expected failedReason to contain 'my error message', got: '%s'", fetched.FailedReason)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// UnrecoverableError moves to failed immediately (Node.js: 'moves job to
// failed' in UnrecoverableError describe)
// ═══════════════════════════════════════════════════════════════════════════

func TestUnrecoverableErrorMovesToFailedImmediately(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Even with attempts=3, unrecoverable should not retry
	job := must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{Attempts: 3}))

	var attemptCount atomic.Int32
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		attemptCount.Add(1)
		return nil, bullmq.NewUnrecoverableError("unrecoverable error")
	}, nil)

	w1WaitEvents(t, worker, 5*time.Second, bullmq.EventFailed, 1, "waiting for failed event")

	time.Sleep(200 * time.Millisecond)

	// Should have only processed once (no retries)
	if got := attemptCount.Load(); got != 1 {
		t.Fatalf("UnrecoverableError should not retry: attempts = %d", got)
	}
	if state := must(queue.GetJobState(ctx, job.ID)); state != bullmq.StateFailed {
		t.Fatalf("state = %s, want failed", state)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Lock extender continues to run (Node.js: 'lock extender continues to run
// until all active jobs are completed when closing a worker')
// ═══════════════════════════════════════════════════════════════════════════

func TestLockExtenderRunsUntilJobsComplete(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{}, nil))

	tx := make(chan struct{}, 1)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		// Processing takes longer than lock duration
		time.Sleep(1500 * time.Millisecond)
		w1Send(ctx, tx, struct{}{})
		return nil, nil
	}, &bullmq.WorkerOptions{LockDuration: 500 * time.Millisecond})

	// Wait a moment then close with generous timeout
	time.Sleep(200 * time.Millisecond)
	w1Close(t, worker, 5*time.Second)

	// Job should have completed despite lock duration < processing time
	w1Recv(t, tx, 100*time.Millisecond, "Job should have completed before close returned")

	// Verify job is completed, not stalled/failed
	counts := w1Counts(t, queue)
	if counts.Completed != 1 || counts.Failed != 0 {
		t.Fatalf("counts = %+v, want completed=1 failed=0", counts)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Clear job from stalled set when completed (Node.js: 'should clear job
// from stalled set when job completed')
// ═══════════════════════════════════════════════════════════════════════════

func TestClearJobFromStalledSetOnComplete(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		time.Sleep(100 * time.Millisecond)
		return nil, nil
	}, &bullmq.WorkerOptions{StalledInterval: 10 * time.Millisecond})

	w1WaitEvents(t, worker, 5*time.Second, bullmq.EventCompleted, 1, "waiting for completed event")

	// After completion, stalled set should be empty
	counts := w1Counts(t, queue)
	if counts.Completed != 1 || counts.Active != 0 {
		t.Fatalf("counts = %+v, want completed=1 active=0", counts)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Multiple workers don't process same job (Node.js: 'does not process a job
// that is being processed when a new queue starts')
// ═══════════════════════════════════════════════════════════════════════════

func TestDoesNotProcessJobBeingProcessedByAnother(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{}, nil))

	var (
		mu           sync.Mutex
		processedIDs []string
	)
	record := func(s string) {
		mu.Lock()
		processedIDs = append(processedIDs, s)
		mu.Unlock()
	}

	worker1 := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		record("w1-" + job.ID)
		time.Sleep(500 * time.Millisecond)
		return nil, nil
	}, nil)

	// Start second worker after first has picked up the job
	time.Sleep(200 * time.Millisecond)

	worker2 := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		record("w2-" + job.ID)
		return nil, nil
	}, nil)

	time.Sleep(2 * time.Second)

	mu.Lock()
	ids := slices.Clone(processedIDs)
	mu.Unlock()
	// Only one worker should have processed the job
	if len(ids) != 1 {
		t.Fatalf("Job should only be processed once, got: %v", ids)
	}

	w1Close(t, worker1, 5*time.Second)
	w1Close(t, worker2, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Worker is_running check (Node.js: verifies isRunning in various tests)
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkerIsRunning(t *testing.T) {
	name := testQueueName()

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	}, nil)

	if !worker.IsRunning() {
		t.Fatal("worker should be running")
	}

	w1Close(t, worker, 5*time.Second)

	if worker.IsRunning() {
		t.Fatal("worker should not be running after close")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Keep jobs newer than specified age with removeOnComplete
// (Node.js: 'should keep of jobs newer than specified after completed with
// removeOnComplete')
// ═══════════════════════════════════════════════════════════════════════════

func TestKeepJobsNewerThanAgeRemoveOnComplete(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Keep jobs younger than 10 seconds — all should be kept in this fast test
	opts := &bullmq.JobOptions{RemoveOnComplete: bullmq.KeepJobsPolicy(bullmq.KeepJobs{Age: bullmq.Ptr[int64](10)})}
	for i := range 5 {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, opts))
	}

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	}, nil)

	w1WaitEvents(t, worker, 10*time.Second, bullmq.EventCompleted, 5, "waiting for 5 completed events")

	time.Sleep(100 * time.Millisecond)

	// All jobs are younger than 10s so all should be kept
	if counts := w1Counts(t, queue); counts.Completed != 5 {
		t.Fatalf("completed = %d, want 5", counts.Completed)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Keep jobs with age and count limit (Node.js: 'should keep of jobs newer
// than specified and up to a count completed with removeOnComplete')
// ═══════════════════════════════════════════════════════════════════════════

func TestKeepJobsWithAgeAndCountLimit(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// age=10s, count=3 — since all are < 10s old, count takes precedence
	opts := &bullmq.JobOptions{RemoveOnComplete: bullmq.KeepJobsPolicy(bullmq.KeepJobs{
		Age:   bullmq.Ptr[int64](10),
		Count: bullmq.Ptr[int64](3),
	})}
	for i := range 6 {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, opts))
	}

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	}, nil)

	w1WaitEvents(t, worker, 10*time.Second, bullmq.EventCompleted, 6, "waiting for 6 completed events")

	time.Sleep(100 * time.Millisecond)

	if counts := w1Counts(t, queue); counts.Completed > 3 {
		t.Fatalf("Expected at most 3 completed jobs, got %d", counts.Completed)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Global removeOnFail from worker options (Node.js: 'should keep specified
// number of jobs after completed with global removeOnFail')
// ═══════════════════════════════════════════════════════════════════════════

func TestGlobalRemoveOnFailFromWorker(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	for i := range 6 {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, nil))
	}

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, bullmq.NewUnrecoverableError("fail")
	}, &bullmq.WorkerOptions{RemoveOnFail: bullmq.KeepLast(3)})

	w1WaitEvents(t, worker, 10*time.Second, bullmq.EventFailed, 6, "waiting for 6 failed events")

	time.Sleep(100 * time.Millisecond)

	if counts := w1Counts(t, queue); counts.Failed > 3 {
		t.Fatalf("Expected at most 3 failed jobs, got %d", counts.Failed)
	}

	w1Close(t, worker, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Continues processing after worker has stalled (Node.js: 'continues
// processing after a worker has stalled')
// ═══════════════════════════════════════════════════════════════════════════

func TestContinuesProcessingAfterStall(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "test", map[string]any{"bar": "baz"}, nil))

	// Worker 1: stalls on the job. Like the Rust processor it ignores its
	// cancellation token; it is only released when the test ends so that no
	// goroutine outlives the test.
	release := make(chan struct{})
	processor1Done := make(chan struct{})
	worker1 := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		defer close(processor1Done)
		select {
		case <-time.After(60 * time.Second):
		case <-release:
		}
		return nil, nil
	}, &bullmq.WorkerOptions{
		LockDuration:    500 * time.Millisecond,
		StalledInterval: 100 * time.Millisecond,
	})
	t.Cleanup(func() {
		close(release)
		select {
		case <-processor1Done:
		case <-time.After(5 * time.Second):
		}
	})

	// Wait long enough for the job to be picked up, then force close
	time.Sleep(300 * time.Millisecond)
	w1Close(t, worker1, 0)

	// Worker 2: processes the stalled job
	tx := make(chan struct{}, 1)
	worker2 := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		w1Send(ctx, tx, struct{}{})
		return nil, nil
	}, &bullmq.WorkerOptions{
		LockDuration:    500 * time.Millisecond,
		StalledInterval: 100 * time.Millisecond,
	})

	w1Recv(t, tx, 10*time.Second, "waiting for stalled job to be reprocessed")

	w1Close(t, worker2, 5*time.Second)
}

// ═══════════════════════════════════════════════════════════════════════════
// Retry with fixed backoff timing (Node.js: 'should retry a job after a
// delay if a fixed backoff is given')
// ═══════════════════════════════════════════════════════════════════════════

func TestRetryFixedBackoffTiming(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	start := time.Now()
	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.FixedBackoff(500 * time.Millisecond),
	}))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if job.AttemptsMade < 2 {
			return nil, errors.New("Not yet!")
		}
		return nil, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	w1WaitEvents(t, worker, 10*time.Second, bullmq.EventCompleted, 1, "waiting for completed event")

	// 2 retries * 500ms backoff = at least 1000ms
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("Should have waited for backoff delays, elapsed: %v", elapsed)
	}

	w1Close(t, worker, 5*time.Second)
}
