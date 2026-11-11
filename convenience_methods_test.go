package bullmq_test

// Convenience-method tests — Queue.UpdateJobProgress, Job.MoveToWait,
// Queue.RecordJobCountsMetric.
//
// Mirror the corresponding Node.js `Queue.updateJobProgress`, `Job.moveToWait`
// and `Queue.recordJobCountsMetric`. Require a running Redis instance at
// `redis://127.0.0.1:6379`.
// Ported from bullmq_rust/tests/convenience_methods_test.rs.

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/imdat99/bullmq"
)

// evNoopProcessor returns a processor that completes every job with null.
func evNoopProcessor() bullmq.Processor {
	return func(ctx context.Context, job *bullmq.Job) (any, error) { return nil, nil }
}

// evNewQueue creates a queue on the test connection (obliterated and closed
// at cleanup).
func evNewQueue(t testing.TB, name string) *bullmq.Queue {
	t.Helper()
	return newTestQueue(t, name, &bullmq.QueueOptions{Connection: testConnection()})
}

// ── Queue.UpdateJobProgress ─────────────────────────────────────────────────

func TestUpdateJobProgressNumber(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := evNewQueue(t, name)

	job := must(queue.Add(ctx, "test", map[string]any{}, nil))
	noErr(t, queue.UpdateJobProgress(ctx, job.ID, 42.0))

	reloaded := must(queue.GetJob(ctx, job.ID))
	if reloaded == nil {
		t.Fatalf("job not found")
	}
	var n float64
	if err := json.Unmarshal(reloaded.Progress, &n); err != nil {
		t.Fatalf("expected number progress, got %s", reloaded.Progress)
	}
	if n != 42.0 {
		t.Fatalf("progress = %v, want 42", n)
	}
}

func TestUpdateJobProgressObject(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := evNewQueue(t, name)

	job := must(queue.Add(ctx, "test", map[string]any{}, nil))
	noErr(t, queue.UpdateJobProgress(ctx, job.ID, map[string]any{"pct": 10}))

	reloaded := must(queue.GetJob(ctx, job.ID))
	if reloaded == nil {
		t.Fatalf("job not found")
	}
	var v map[string]any
	if err := json.Unmarshal(reloaded.Progress, &v); err != nil {
		t.Fatalf("expected object progress, got %s", reloaded.Progress)
	}
	if !reflect.DeepEqual(v, map[string]any{"pct": float64(10)}) {
		t.Fatalf("progress = %s, want {\"pct\":10}", reloaded.Progress)
	}
}

func TestUpdateJobProgressEmitsEvent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := evNewQueue(t, name)

	events := evNewEvents(t, name, &bullmq.QueueEventsOptions{
		Connection:      testConnection(),
		LastEventID:     "0",
		BlockingTimeout: 1000 * time.Millisecond,
	})

	job := must(queue.Add(ctx, "test", map[string]any{}, nil))
	noErr(t, queue.UpdateJobProgress(ctx, job.ID, 75.0))

	event := evWaitFor(t, events, 10*time.Second, evIs(bullmq.QueueEventProgress))
	if event.JobID != job.ID {
		t.Fatalf("job id = %q, want %q", event.JobID, job.ID)
	}
	var data float64
	if err := event.DecodeProgress(&data); err != nil || data != 75.0 {
		t.Fatalf("progress = %s, want 75 (err %v)", event.Progress, err)
	}

	noErr(t, events.Close())
}

func TestUpdateJobProgressNonexistentErrors(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := evNewQueue(t, name)

	err := queue.UpdateJobProgress(ctx, "does-not-exist", 1.0)
	if err == nil {
		t.Fatalf("expected an error")
	}
	code, ok := bullmq.ScriptErrorCode(err)
	if !ok {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != -1 {
		t.Fatalf("script error code = %d, want -1", code)
	}
}

// ── Job.MoveToWait ──────────────────────────────────────────────────────────

func TestMoveToWaitReturnsActiveJobToWait(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	conn := testConnection()
	queue := evNewQueue(t, name)

	must(queue.Add(ctx, "test", map[string]any{}, nil))

	// A non-autorun worker lets us fetch the job manually and hold it active.
	worker := newTestWorker(t, name, evNoopProcessor(), &bullmq.WorkerOptions{
		Connection:  conn,
		ManualStart: true,
	})

	job := must(worker.GetNextJob(ctx, "token-1"))
	if job == nil {
		t.Fatalf("a job should be available")
	}
	if n := must(queue.GetActiveCount(ctx)); n != 1 {
		t.Fatalf("active count = %d, want 1", n)
	}

	// Move it back to wait using the job's lock token (ctx default).
	pttl := must(job.MoveToWait(ctx, ""))
	if pttl != 0 {
		t.Fatalf("no rate limit configured: pttl = %v", pttl)
	}

	if n := must(queue.GetActiveCount(ctx)); n != 0 {
		t.Fatalf("active count = %d, want 0", n)
	}
	if n := must(queue.GetWaitingCount(ctx)); n != 1 {
		t.Fatalf("waiting count = %d, want 1", n)
	}
	if s := must(queue.GetJobState(ctx, job.ID)); s != bullmq.StateWaiting {
		t.Fatalf("state = %v, want waiting", s)
	}

	noErr(t, worker.Close(2*time.Second))
}

func TestMoveToWaitWithWrongTokenErrors(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	conn := testConnection()
	queue := evNewQueue(t, name)

	must(queue.Add(ctx, "test", map[string]any{}, nil))

	worker := newTestWorker(t, name, evNoopProcessor(), &bullmq.WorkerOptions{
		Connection:  conn,
		ManualStart: true,
	})

	job := must(worker.GetNextJob(ctx, "the-real-token"))
	if job == nil {
		t.Fatalf("a job should be available")
	}

	// A token that does not match the lock must be rejected.
	if _, err := job.MoveToWait(ctx, "wrong-token"); err == nil {
		t.Fatalf("expected an error") // any error is acceptable; the job must remain active.
	}
	if n := must(queue.GetActiveCount(ctx)); n != 1 {
		t.Fatalf("active count = %d, want 1", n)
	}

	noErr(t, worker.Close(2*time.Second))
}

// ── Queue.RecordJobCountsMetric ─────────────────────────────────────────────

func TestRecordJobCountsMetricReturnsCounts(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := evNewQueue(t, name)

	for i := 0; i < 3; i++ {
		must(queue.Add(ctx, "test", map[string]any{}, nil))
	}

	counts := must(queue.RecordJobCountsMetric(ctx, []string{"waiting", "completed"}, nil))
	if v, ok := counts["waiting"]; !ok || v != 3 {
		t.Fatalf("waiting = (%d, %v), want 3", v, ok)
	}
	if v, ok := counts["completed"]; !ok || v != 0 {
		t.Fatalf("completed = (%d, %v), want 0", v, ok)
	}
}

func TestRecordJobCountsMetricInvokesRecorder(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := evNewQueue(t, name)

	for i := 0; i < 2; i++ {
		must(queue.Add(ctx, "test", map[string]any{}, nil))
	}

	type rec struct {
		state string
		count int64
	}
	var mu sync.Mutex
	var collected []rec
	recorder := func(state string, count int64) {
		mu.Lock()
		defer mu.Unlock()
		collected = append(collected, rec{state, count})
	}

	counts := must(queue.RecordJobCountsMetric(ctx, []string{"waiting", "active", "failed"}, recorder))

	mu.Lock()
	recorded := append([]rec(nil), collected...)
	mu.Unlock()
	// The recorder is invoked once per requested state.
	if len(recorded) != len(counts) {
		t.Fatalf("recorded %d entries, want %d", len(recorded), len(counts))
	}
	found := false
	for _, r := range recorded {
		if r.state == "waiting" {
			found = true
			if r.count != 2 {
				t.Fatalf("waiting = %d, want 2", r.count)
			}
			break
		}
	}
	if !found {
		t.Fatalf("waiting not recorded")
	}
	for _, r := range recorded {
		if v, ok := counts[r.state]; !ok || v != r.count {
			t.Fatalf("counts[%q] = (%d, %v), want %d", r.state, v, ok, r.count)
		}
	}
}
