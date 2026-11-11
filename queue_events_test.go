package bullmq_test

// QueueEvents integration tests — cross-process stream event listening.
//
// These mirror the Node.js `tests/events.test.ts` scenarios. They require a
// running Redis instance at `redis://127.0.0.1:6379`.
// Ported from bullmq_rust/tests/queue_events_test.rs.

import (
	"bullmq"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// evMakeEvents builds QueueEvents that reads the full stream history ("0"),
// which removes the start-up race for freshly-created, uniquely-named test
// queues. The listener is also closed at cleanup (Close is idempotent).
func evMakeEvents(t testing.TB, name string) *bullmq.QueueEvents {
	t.Helper()
	return evNewEvents(t, name, &bullmq.QueueEventsOptions{
		Connection:      testConnection(),
		LastEventID:     "0",
		BlockingTimeout: 1000 * time.Millisecond,
	})
}

// evNewEvents creates a QueueEvents with opts, closed at cleanup.
func evNewEvents(t testing.TB, name string, opts *bullmq.QueueEventsOptions) *bullmq.QueueEvents {
	t.Helper()
	ev, err := bullmq.NewQueueEvents(context.Background(), name, opts)
	if err != nil {
		t.Fatalf("create QueueEvents: %v", err)
	}
	t.Cleanup(func() { _ = ev.Close() })
	return ev
}

// evWaitFor waits until an event matching pred is observed, or fails the
// test after timeout.
func evWaitFor(t testing.TB, events *bullmq.QueueEvents, timeout time.Duration, pred func(bullmq.QueueEvent) bool) bullmq.QueueEvent {
	t.Helper()
	return evWaitForEntry(t, events, timeout, pred).Event
}

// evWaitForEntry is like evWaitFor but returns the whole stream entry.
func evWaitForEntry(t testing.TB, events *bullmq.QueueEvents, timeout time.Duration, pred func(bullmq.QueueEvent) bool) bullmq.QueueEventEntry {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		entry, ok := events.NextEvent(ctx)
		if !ok {
			if ctx.Err() != nil {
				t.Fatalf("timed out waiting for matching queue event")
			}
			t.Fatalf("QueueEvents closed before matching event")
		}
		if pred(entry.Event) {
			return entry
		}
	}
}

// evIs matches events of type typ.
func evIs(typ bullmq.QueueEventType) func(bullmq.QueueEvent) bool {
	return func(e bullmq.QueueEvent) bool { return e.Type == typ }
}

func TestEmitsAddedEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: testConnection()})

	events := evMakeEvents(t, name)

	job := must(queue.Add(ctx, "my-job", map[string]any{"foo": "bar"}, nil))

	event := evWaitFor(t, events, 10*time.Second, evIs(bullmq.QueueEventAdded))
	if event.JobID != job.ID {
		t.Fatalf("job id = %q, want %q", event.JobID, job.ID)
	}
	if event.JobName != "my-job" {
		t.Fatalf("job name = %q, want %q", event.JobName, "my-job")
	}

	noErr(t, events.Close())
}

func TestIdleQueueEmitsNoSpuriousErrorEvents(t *testing.T) {
	name := testQueueName()
	_ = newTestQueue(t, name, &bullmq.QueueOptions{Connection: testConnection()})

	// blocking_timeout is deliberately larger than redis-rs's 500ms default
	// response timeout: the dedicated stream connection must let XREAD BLOCK
	// run to completion instead of timing out client-side.
	events := evNewEvents(t, name, &bullmq.QueueEventsOptions{
		Connection:      testConnection(),
		LastEventID:     "0",
		BlockingTimeout: 2000 * time.Millisecond,
	})

	// No jobs are added, so the queue stays idle across several 500ms windows.
	time.Sleep(2500 * time.Millisecond)

	for {
		entry, ok := events.TryNextEvent()
		if !ok {
			break
		}
		if entry.Event.Type == bullmq.QueueEventError {
			t.Fatalf("idle queue emitted a spurious error event: %s", entry.Event.Message)
		}
	}

	noErr(t, events.Close())
}

func TestEmitsWaitingEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: testConnection()})

	events := evMakeEvents(t, name)

	job := must(queue.Add(ctx, "waiting-job", map[string]any{}, nil))

	event := evWaitFor(t, events, 10*time.Second, evIs(bullmq.QueueEventWaiting))
	if event.JobID != job.ID {
		t.Fatalf("job id = %q, want %q", event.JobID, job.ID)
	}

	noErr(t, events.Close())
}

// TestRunThrowsWhenAlreadyRunningEvents ports queue_events_test.rs
// test_run_throws_when_already_running (the name collides with the worker
// test of the same name).
func TestRunThrowsWhenAlreadyRunningEvents(t *testing.T) {
	name := testQueueName()
	events := evNewEvents(t, name, &bullmq.QueueEventsOptions{
		Connection: testConnection(),
		// autorun: true
	})

	if !events.IsRunning() {
		t.Fatalf("expected listener to be running")
	}
	if err := events.Run(); err == nil {
		t.Fatalf("second run() should fail")
	}

	noErr(t, events.Close())
}

func TestCloseMakesNextEventReturnNone(t *testing.T) {
	name := testQueueName()
	events := evNewEvents(t, name, &bullmq.QueueEventsOptions{
		Connection:      testConnection(),
		BlockingTimeout: 1000 * time.Millisecond,
		// autorun: true
	})

	noErr(t, events.Close())

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, ok := events.NextEvent(ctx)
	if ctx.Err() != nil {
		t.Fatalf("next_event should not hang after close")
	}
	if ok {
		t.Fatalf("closed listener should yield None")
	}
}

func TestRunAfterCloseResetsRunning(t *testing.T) {
	name := testQueueName()
	events := evNewEvents(t, name, &bullmq.QueueEventsOptions{
		Connection:  testConnection(),
		ManualStart: true,
	})

	if events.IsRunning() {
		t.Fatalf("expected listener not running")
	}
	noErr(t, events.Close())
	if events.IsRunning() {
		t.Fatalf("expected listener not running after close")
	}

	if err := events.Run(); err == nil {
		t.Fatalf("run() after close should fail")
	}
	if events.IsRunning() {
		t.Fatalf("failed run() should reset running")
	}

	if err := events.Run(); err == nil {
		t.Fatalf("subsequent run() after close should still fail")
	}
	if events.IsRunning() {
		t.Fatalf("running should stay false after failed run()")
	}
}

func TestAutorunFalseThenRun(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: testConnection()})

	events := evNewEvents(t, name, &bullmq.QueueEventsOptions{
		Connection:      testConnection(),
		LastEventID:     "0",
		BlockingTimeout: 1000 * time.Millisecond,
		ManualStart:     true,
	})

	if events.IsRunning() {
		t.Fatalf("expected listener not running")
	}
	noErr(t, events.Run())
	if !events.IsRunning() {
		t.Fatalf("expected listener running")
	}

	must(queue.Add(ctx, "late-job", map[string]any{}, nil))

	evWaitFor(t, events, 10*time.Second, evIs(bullmq.QueueEventAdded))

	noErr(t, events.Close())
}

func TestEmitsCompletedEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	conn := testConnection()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: conn})

	events := evMakeEvents(t, name)

	must(queue.Add(ctx, "done", map[string]any{"x": 1}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return map[string]any{"ok": true}, nil
	}, &bullmq.WorkerOptions{Connection: conn})

	event := evWaitFor(t, events, 15*time.Second, evIs(bullmq.QueueEventCompleted))
	var parsed map[string]any
	noErr(t, event.DecodeReturnValue(&parsed))
	if len(parsed) != 1 || parsed["ok"] != true {
		t.Fatalf("return value = %s, want {\"ok\":true}", event.ReturnValue)
	}

	noErr(t, worker.Close(5*time.Second))
	noErr(t, events.Close())
}

func TestEmitsFailedEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	conn := testConnection()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: conn})

	events := evMakeEvents(t, name)

	must(queue.Add(ctx, "boom", map[string]any{}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, errors.New("intentional failure")
	}, &bullmq.WorkerOptions{Connection: conn})

	event := evWaitFor(t, events, 15*time.Second, evIs(bullmq.QueueEventFailed))
	if !strings.Contains(event.FailedReason, "intentional failure") {
		t.Fatalf("failed reason = %q", event.FailedReason)
	}

	noErr(t, worker.Close(5*time.Second))
	noErr(t, events.Close())
}

func TestEmitsProgressEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	conn := testConnection()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: conn})

	events := evMakeEvents(t, name)

	must(queue.Add(ctx, "progress-job", map[string]any{}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if err := job.UpdateProgress(ctx, 42.0); err != nil {
			return nil, err
		}
		return nil, nil
	}, &bullmq.WorkerOptions{Connection: conn})

	event := evWaitFor(t, events, 15*time.Second, evIs(bullmq.QueueEventProgress))
	var data float64
	noErr(t, event.DecodeProgress(&data))
	if data != 42.0 {
		t.Fatalf("progress = %s, want 42", event.Progress)
	}

	noErr(t, worker.Close(5*time.Second))
	noErr(t, events.Close())
}

func TestEmitsDelayedEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: testConnection()})

	events := evMakeEvents(t, name)

	job := must(queue.Add(ctx, "delayed-job", map[string]any{}, &bullmq.JobOptions{Delay: 60 * time.Second}))

	event := evWaitFor(t, events, 10*time.Second, evIs(bullmq.QueueEventDelayed))
	if event.JobID != job.ID {
		t.Fatalf("job id = %q, want %q", event.JobID, job.ID)
	}
	if event.Delay <= 0 {
		t.Fatalf("delay = %d, want > 0", event.Delay)
	}

	noErr(t, events.Close())
}

// TestQueueEventsRecoversAfterConnectionIsKilled is a regression test:
// QueueEvents must recover on its own after its dedicated stream connection
// is dropped (network blip, Redis restart, failover).
//
// QueueEvents runs its own consume loop, completely separate from the
// worker's equivalent recovery path. The worker regression test
// (test_worker_recovers_after_connection_is_killed) only exercises the
// worker's path, so it can pass while this one is broken. CLIENT KILL
// simulates the dropped socket deterministically. The test runs in its own
// logical database so the kill does not drop the connections of other tests
// running in parallel against the same server.
func TestQueueEventsRecoversAfterConnectionIsKilled(t *testing.T) {
	const killDB = 13
	ctx := context.Background()
	name := testQueueName()
	connOpts := isolatedConnection(killDB)
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: connOpts})

	events := evNewEvents(t, name, &bullmq.QueueEventsOptions{
		Connection:      connOpts,
		LastEventID:     "0",
		BlockingTimeout: 1000 * time.Millisecond,
	})

	before := must(queue.Add(ctx, "before", map[string]any{}, nil))
	event := evWaitFor(t, events, 10*time.Second, evIs(bullmq.QueueEventAdded))
	if event.JobID != before.ID {
		t.Fatalf("job id = %q, want %q", event.JobID, before.ID)
	}

	// Drop every connection of this test's database, as a Redis restart would.
	killed := killConnectionsInDB(t, killDB)
	if killed <= 0 {
		t.Fatalf("expected CLIENT KILL to drop some connections")
	}

	// Wait for the queue's own connection to reconnect before using it to add
	// the next job.
	deadline := time.Now().Add(20 * time.Second)
	for {
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := queue.Connection().Ping(pctx)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("queue did not reconnect")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// QueueEvents must reconnect by itself and keep delivering later stream
	// events.
	after := must(queue.Add(ctx, "after", map[string]any{}, nil))
	wctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var got bullmq.QueueEvent
	for {
		entry, ok := events.NextEvent(wctx)
		if !ok {
			if wctx.Err() != nil {
				t.Fatalf("QueueEvents did not recover after its connection was killed")
			}
			t.Fatalf("QueueEvents closed before matching event")
		}
		if entry.Event.Type == bullmq.QueueEventAdded && entry.Event.JobID == after.ID {
			got = entry.Event
			break
		}
	}
	if got.Type != bullmq.QueueEventAdded || got.JobID != after.ID {
		t.Fatalf("expected Added for %q, got %+v", after.ID, got)
	}

	noErr(t, events.Close())
}

func TestEmitsDrainedEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	conn := testConnection()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: conn})

	events := evMakeEvents(t, name)

	must(queue.Add(ctx, "drain-me", map[string]any{}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	}, &bullmq.WorkerOptions{Connection: conn})

	evWaitFor(t, events, 15*time.Second, evIs(bullmq.QueueEventDrained))

	noErr(t, worker.Close(5*time.Second))
	noErr(t, events.Close())
}

func TestEmitsRemovedEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: testConnection()})

	events := evMakeEvents(t, name)

	job := must(queue.Add(ctx, "remove-me", map[string]any{}, nil))

	removed := must(queue.Remove(ctx, job.ID))
	if !removed {
		t.Fatalf("expected job to be removed")
	}

	event := evWaitFor(t, events, 10*time.Second, evIs(bullmq.QueueEventRemoved))
	if event.JobID != job.ID {
		t.Fatalf("job id = %q, want %q", event.JobID, job.ID)
	}

	noErr(t, events.Close())
}

func TestResumeFromLastEventId(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: testConnection()})

	// First listener captures the 'added' event and records its stream id.
	events := evMakeEvents(t, name)
	must(queue.Add(ctx, "first", map[string]any{}, nil))

	firstID := evWaitForEntry(t, events, 10*time.Second, evIs(bullmq.QueueEventAdded)).ID
	noErr(t, events.Close())

	// Second listener resumes after the first event and should only see newer ones.
	resumed := evNewEvents(t, name, &bullmq.QueueEventsOptions{
		Connection:      testConnection(),
		LastEventID:     firstID,
		BlockingTimeout: 1000 * time.Millisecond,
	})

	job2 := must(queue.Add(ctx, "second", map[string]any{}, nil))

	event := evWaitFor(t, resumed, 10*time.Second, evIs(bullmq.QueueEventAdded))
	if event.JobID != job2.ID {
		t.Fatalf("job id = %q, want %q", event.JobID, job2.ID)
	}
	if event.JobName != "second" {
		t.Fatalf("job name = %q, want %q", event.JobName, "second")
	}

	noErr(t, resumed.Close())
}

func TestEmitsActiveEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	conn := testConnection()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: conn})

	events := evMakeEvents(t, name)

	job := must(queue.Add(ctx, "act", map[string]any{}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	}, &bullmq.WorkerOptions{Connection: conn})

	event := evWaitFor(t, events, 15*time.Second, evIs(bullmq.QueueEventActive))
	if event.JobID != job.ID {
		t.Fatalf("job id = %q, want %q", event.JobID, job.ID)
	}
	// Jobs become active from the waiting state.
	if event.Prev != "waiting" {
		t.Fatalf("prev = %q, want %q", event.Prev, "waiting")
	}

	noErr(t, worker.Close(5*time.Second))
	noErr(t, events.Close())
}

func TestEmitsDuplicatedEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: testConnection()})

	events := evMakeEvents(t, name)

	opts := func() *bullmq.JobOptions { return &bullmq.JobOptions{JobID: "dup-1"} }
	// The first add creates the job; the second with the same id is a duplicate.
	must(queue.Add(ctx, "test", map[string]any{}, opts()))
	must(queue.Add(ctx, "test", map[string]any{}, opts()))

	event := evWaitFor(t, events, 10*time.Second, evIs(bullmq.QueueEventDuplicated))
	if event.JobID != "dup-1" {
		t.Fatalf("job id = %q, want %q", event.JobID, "dup-1")
	}

	noErr(t, events.Close())
}

func TestEmitsCleanedEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	conn := testConnection()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: conn})

	const numJobs = 5
	for i := 0; i < numJobs; i++ {
		must(queue.Add(ctx, "test", map[string]any{"i": i}, nil))
	}

	// Process all jobs so they land in the completed set.
	var processed atomic.Int32
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		processed.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{Connection: conn})

	deadline := time.Now().Add(15 * time.Second)
	for processed.Load() < numJobs {
		if time.Now().After(deadline) {
			t.Fatalf("timed out processing")
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)

	// Start the listener AFTER processing so we only capture the clean event.
	events := evMakeEvents(t, name)
	removed := must(queue.Clean(ctx, 0, 0, "completed"))
	if len(removed) != numJobs {
		t.Fatalf("cleaned %d jobs, want %d", len(removed), numJobs)
	}

	event := evWaitFor(t, events, 10*time.Second, evIs(bullmq.QueueEventCleaned))
	if event.Count != strconv.Itoa(numJobs) {
		t.Fatalf("count = %q, want %q", event.Count, strconv.Itoa(numJobs))
	}

	noErr(t, worker.Close(5*time.Second))
	noErr(t, events.Close())
}

func TestEmitsRetriesExhaustedEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	conn := testConnection()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: conn})

	events := evMakeEvents(t, name)

	// Default attempts (1): a single failure exhausts retries.
	must(queue.Add(ctx, "boom", map[string]any{}, nil))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, errors.New("nope")
	}, &bullmq.WorkerOptions{Connection: conn})

	event := evWaitFor(t, events, 15*time.Second, evIs(bullmq.QueueEventRetriesExhausted))
	if event.AttemptsMade != "1" {
		t.Fatalf("attempts made = %q, want %q", event.AttemptsMade, "1")
	}

	noErr(t, worker.Close(5*time.Second))
	noErr(t, events.Close())
}

func TestEmitsWaitingChildrenEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	conn := testConnection()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: conn})

	events := evMakeEvents(t, name)

	// A parent with one child is placed in the waiting-children state.
	flow := must(bullmq.NewFlowProducerWithConnection(queue.Connection(), ""))
	defer flow.Close()
	node := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent",
		QueueName: name,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{{
			Name:      "child",
			QueueName: name,
			Data:      map[string]any{},
		}},
	}))
	parentID := node.Job.ID

	event := evWaitFor(t, events, 10*time.Second, evIs(bullmq.QueueEventWaitingChildren))
	if event.JobID != parentID {
		t.Fatalf("job id = %q, want %q", event.JobID, parentID)
	}

	noErr(t, events.Close())
}

func TestNoRemovedEventForNonexistentJob(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: testConnection()})

	events := evMakeEvents(t, name)

	// Removing a non-existent job must not publish a `removed` event.
	_ = must(queue.Remove(ctx, "does-not-exist"))

	// Add a real job as a marker; we should observe its Added event without ever
	// seeing a Removed event for the missing id.
	marker := must(queue.Add(ctx, "marker", map[string]any{}, nil))

	sawRemoved := false
	reachedMarker := false
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	for !reachedMarker {
		entry, ok := events.NextEvent(wctx)
		if !ok {
			if wctx.Err() != nil {
				t.Fatalf("timed out")
			}
			break
		}
		switch {
		case entry.Event.Type == bullmq.QueueEventRemoved:
			sawRemoved = true
		case entry.Event.Type == bullmq.QueueEventAdded && entry.Event.JobID == marker.ID:
			reachedMarker = true
		}
	}

	if !reachedMarker {
		t.Fatalf("did not reach the marker event")
	}
	if sawRemoved {
		t.Fatalf("no removed event should be published for a non-existent job")
	}

	noErr(t, events.Close())
}

func TestDrainedEmittedOncePerIdleBatch(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	conn := testConnection()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: conn})

	events := evMakeEvents(t, name)

	// Count drained events seen across the whole test in a background goroutine.
	var drained atomic.Int32
	var collector sync.WaitGroup
	collector.Add(1)
	go func() {
		defer collector.Done()
		for {
			entry, ok := events.NextEvent(context.Background())
			if !ok {
				return
			}
			if entry.Event.Type == bullmq.QueueEventDrained {
				drained.Add(1)
			}
		}
	}()
	// Make sure the collector exits even if the test fails early.
	t.Cleanup(func() {
		_ = events.Close()
		collector.Wait()
	})

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		time.Sleep(20 * time.Millisecond)
		return nil, nil
	}, &bullmq.WorkerOptions{Connection: conn, DrainDelay: time.Second})

	// First idle batch.
	must(queue.Add(ctx, "a", map[string]any{}, nil))
	must(queue.Add(ctx, "b", map[string]any{}, nil))
	time.Sleep(1200 * time.Millisecond)

	// Second idle batch.
	must(queue.Add(ctx, "c", map[string]any{}, nil))
	must(queue.Add(ctx, "d", map[string]any{}, nil))
	time.Sleep(1200 * time.Millisecond)

	if count := drained.Load(); count != 2 {
		t.Fatalf("expected exactly one drained event per idle batch: got %d, want 2", count)
	}

	noErr(t, worker.Close(5*time.Second))
	noErr(t, events.Close())
	collector.Wait()
}

func TestEventEntryExposesIdAndJobId(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: testConnection()})

	events := evMakeEvents(t, name)
	job := must(queue.Add(ctx, "idx", map[string]any{}, nil))

	entry := evWaitForEntry(t, events, 10*time.Second, evIs(bullmq.QueueEventAdded))

	// Stream ids look like "<millis>-<seq>".
	if !strings.Contains(entry.ID, "-") {
		t.Fatalf("unexpected stream id: %s", entry.ID)
	}
	if id, ok := entry.Event.LookupJobID(); !ok || id != job.ID {
		t.Fatalf("job_id() = (%q, %v), want (%q, true)", id, ok, job.ID)
	}
	if got := entry.Event.Name(); got != "added" {
		t.Fatalf("name = %q, want %q", got, "added")
	}

	noErr(t, events.Close())
}
