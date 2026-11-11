package bullmq

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func fields(pairs ...[2]string) map[string]string {
	m := make(map[string]string, len(pairs))
	for _, p := range pairs {
		m[p[0]] = p[1]
	}
	return m
}

func kv(k, v string) [2]string { return [2]string{k, v} }

func assertEvent(t *testing.T, got, want QueueEvent) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("event mismatch\n got: %+v\nwant: %+v", got, want)
	}
}

func assertJSON(t *testing.T, raw json.RawMessage, want any) {
	t.Helper()
	var got any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("invalid JSON %q: %v", raw, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("JSON mismatch: got %#v, want %#v", got, want)
	}
}

func TestParsesCompletedEvent(t *testing.T) {
	event := parseEvent(fields(
		kv("event", "completed"),
		kv("jobId", "42"),
		kv("returnvalue", `{"ok":true}`),
		kv("prev", "active"),
	))
	assertEvent(t, event, QueueEvent{
		Type:        QueueEventCompleted,
		JobID:       "42",
		ReturnValue: json.RawMessage(`{"ok":true}`),
		Prev:        "active",
	})
	if id, ok := event.LookupJobID(); !ok || id != "42" {
		t.Fatalf("LookupJobID = %q, %v", id, ok)
	}
	if event.Name() != "completed" {
		t.Fatalf("Name = %q", event.Name())
	}
}

func TestParsesProgressJSON(t *testing.T) {
	event := parseEvent(fields(kv("event", "progress"), kv("jobId", "7"), kv("data", `{"pct":50}`)))
	if event.Type != QueueEventProgress {
		t.Fatalf("expected progress, got %+v", event)
	}
	if event.JobID != "7" {
		t.Fatalf("JobID = %q", event.JobID)
	}
	assertJSON(t, event.Progress, map[string]any{"pct": 50.0})
}

func TestParsesProgressStringFallback(t *testing.T) {
	event := parseEvent(fields(kv("event", "progress"), kv("jobId", "7"), kv("data", "not-json")))
	if event.Type != QueueEventProgress {
		t.Fatalf("expected progress, got %+v", event)
	}
	assertJSON(t, event.Progress, "not-json")
}

func TestParsesDelayedNumber(t *testing.T) {
	event := parseEvent(fields(kv("event", "delayed"), kv("jobId", "1"), kv("delay", "1700000000000")))
	assertEvent(t, event, QueueEvent{Type: QueueEventDelayed, JobID: "1", Delay: 1_700_000_000_000})
	if got := event.DelayUntil(); !got.Equal(time.UnixMilli(1_700_000_000_000)) {
		t.Fatalf("DelayUntil = %v", got)
	}
}

func TestParsesDrained(t *testing.T) {
	event := parseEvent(fields(kv("event", "drained")))
	assertEvent(t, event, QueueEvent{Type: QueueEventDrained})
	if _, ok := event.LookupJobID(); ok {
		t.Fatal("drained must not carry a job id")
	}
}

func TestUnknownEventIsOther(t *testing.T) {
	event := parseEvent(fields(kv("event", "future-event"), kv("jobId", "9")))
	if event.Type != QueueEventOther {
		t.Fatalf("expected other, got %+v", event)
	}
	if event.Event != "future-event" {
		t.Fatalf("Event = %q", event.Event)
	}
	if event.Fields["jobId"] != "9" {
		t.Fatalf("Fields = %v", event.Fields)
	}
	if event.Name() != "future-event" {
		t.Fatalf("Name = %q", event.Name())
	}
}

func TestParsesFailedEvent(t *testing.T) {
	event := parseEvent(fields(
		kv("event", "failed"), kv("jobId", "5"), kv("failedReason", "boom"), kv("prev", "active"),
	))
	assertEvent(t, event, QueueEvent{Type: QueueEventFailed, JobID: "5", FailedReason: "boom", Prev: "active"})
	if event.Name() != "failed" {
		t.Fatalf("Name = %q", event.Name())
	}
}

func TestParsesActiveAndWaitingWithPrev(t *testing.T) {
	active := parseEvent(fields(kv("event", "active"), kv("jobId", "1"), kv("prev", "waiting")))
	assertEvent(t, active, QueueEvent{Type: QueueEventActive, JobID: "1", Prev: "waiting"})

	waiting := parseEvent(fields(kv("event", "waiting"), kv("jobId", "2")))
	assertEvent(t, waiting, QueueEvent{Type: QueueEventWaiting, JobID: "2"})
}

func TestParsesAddedCleanedAndRemoved(t *testing.T) {
	assertEvent(t,
		parseEvent(fields(kv("event", "added"), kv("jobId", "1"), kv("name", "job"))),
		QueueEvent{Type: QueueEventAdded, JobID: "1", JobName: "job"})
	assertEvent(t,
		parseEvent(fields(kv("event", "cleaned"), kv("count", "50"))),
		QueueEvent{Type: QueueEventCleaned, Count: "50"})
	assertEvent(t,
		parseEvent(fields(kv("event", "removed"), kv("jobId", "3"), kv("prev", "delayed"))),
		QueueEvent{Type: QueueEventRemoved, JobID: "3", Prev: "delayed"})
}

func TestParsesRetriesExhaustedAndWaitingChildren(t *testing.T) {
	assertEvent(t,
		parseEvent(fields(kv("event", "retries-exhausted"), kv("jobId", "7"), kv("attemptsMade", "3"))),
		QueueEvent{Type: QueueEventRetriesExhausted, JobID: "7", AttemptsMade: "3"})
	wc := parseEvent(fields(kv("event", "waiting-children"), kv("jobId", "8")))
	assertEvent(t, wc, QueueEvent{Type: QueueEventWaitingChildren, JobID: "8"})
	if wc.Name() != "waiting-children" {
		t.Fatalf("Name = %q", wc.Name())
	}
}

func TestParsesDeduplicatedAndDuplicated(t *testing.T) {
	assertEvent(t,
		parseEvent(fields(
			kv("event", "deduplicated"), kv("jobId", "1"),
			kv("deduplicationId", "d1"), kv("deduplicatedJobId", "2"),
		)),
		QueueEvent{Type: QueueEventDeduplicated, JobID: "1", DeduplicationID: "d1", DeduplicatedJobID: "2"})
	assertEvent(t,
		parseEvent(fields(kv("event", "duplicated"), kv("jobId", "9"))),
		QueueEvent{Type: QueueEventDuplicated, JobID: "9"})
}

func TestParsesPausedResumedAndError(t *testing.T) {
	assertEvent(t, parseEvent(fields(kv("event", "paused"))), QueueEvent{Type: QueueEventPaused})
	assertEvent(t, parseEvent(fields(kv("event", "resumed"))), QueueEvent{Type: QueueEventResumed})
	assertEvent(t,
		parseEvent(fields(kv("event", "error"), kv("message", "bad"))),
		QueueEvent{Type: QueueEventError, Message: "bad"})
}

func TestJobIDAndNameHelpersAreConsistent(t *testing.T) {
	// Events without a job id report none.
	for _, e := range []QueueEvent{
		{Type: QueueEventDrained},
		{Type: QueueEventPaused},
		{Type: QueueEventResumed},
		{Type: QueueEventCleaned, Count: "1"},
	} {
		if _, ok := e.LookupJobID(); ok {
			t.Fatalf("%v must not carry a job id", e.Type)
		}
	}
	// Name round-trips through parseEvent for every known type.
	for _, ev := range []string{
		"active", "added", "cleaned", "completed", "debounced", "deduplicated", "delayed",
		"drained", "duplicated", "error", "failed", "paused", "progress", "removed", "resumed",
		"retries-exhausted", "stalled", "waiting", "waiting-children",
	} {
		if got := parseEvent(fields(kv("event", ev))).Name(); got != ev {
			t.Fatalf("Name() = %q, want %q", got, ev)
		}
	}
}

// --- Go-specific unit tests -------------------------------------------------

func TestParseDebouncedAndStalled(t *testing.T) {
	assertEvent(t,
		parseEvent(fields(kv("event", "debounced"), kv("jobId", "1"), kv("debounceId", "x"))),
		QueueEvent{Type: QueueEventDebounced, JobID: "1", DebounceID: "x"})
	assertEvent(t,
		parseEvent(fields(kv("event", "stalled"), kv("jobId", "4"))),
		QueueEvent{Type: QueueEventStalled, JobID: "4"})
}

func TestParseProgressMissingAndNumber(t *testing.T) {
	// A missing payload falls back to the empty string, like serde_json.
	assertJSON(t, parseEvent(fields(kv("event", "progress"), kv("jobId", "1"))).Progress, "")
	assertJSON(t, parseEvent(fields(kv("event", "progress"), kv("jobId", "1"), kv("data", "42"))).Progress, 42.0)
}

func TestParseDelayedInvalid(t *testing.T) {
	e := parseEvent(fields(kv("event", "delayed"), kv("jobId", "1"), kv("delay", "soon")))
	if e.Delay != 0 || !e.DelayUntil().IsZero() {
		t.Fatalf("invalid delay must parse as 0, got %d", e.Delay)
	}
}

func TestQueueEventTypeString(t *testing.T) {
	if QueueEventRetriesExhausted.String() != "retries-exhausted" {
		t.Fatal(QueueEventRetriesExhausted.String())
	}
	if QueueEventOther.String() != "other" {
		t.Fatal(QueueEventOther.String())
	}
	if QueueEventType(0).String() != "unknown(0)" || QueueEventType(200).String() != "unknown(200)" {
		t.Fatal("unexpected String for unknown types")
	}
	if _, ok := parseEvent(fields(kv("event", "other"))).LookupJobID(); ok {
		t.Fatal(`the literal "other" event name must stay an unknown event`)
	}
}

// --- Integration sanity tests (Redis required) -------------------------------

func qeTestConnection() RedisConnectionOptions {
	if u := os.Getenv("REDIS_URL"); u != "" {
		return RedisConnectionOptions{URL: u}
	}
	return RedisConnectionOptions{URL: "redis://127.0.0.1:6379"}
}

func qeTestQueue(t *testing.T) *Queue {
	t.Helper()
	name := "test-qe-" + strings.Split(uuid.NewString(), "-")[0]
	q, err := NewQueue(context.Background(), name, &QueueOptions{Connection: qeTestConnection()})
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = q.Obliterate(ctx, true, 1000)
		_ = q.Close()
	})
	return q
}

func qeNewEvents(t *testing.T, name string, opts QueueEventsOptions) *QueueEvents {
	t.Helper()
	opts.Connection = qeTestConnection()
	qe, err := NewQueueEvents(context.Background(), name, &opts)
	if err != nil {
		t.Fatalf("NewQueueEvents: %v", err)
	}
	t.Cleanup(func() { _ = qe.Close() })
	return qe
}

func qeWaitFor(t *testing.T, qe *QueueEvents, timeout time.Duration, match func(QueueEvent) bool) QueueEventEntry {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		entry, ok := qe.NextEvent(ctx)
		if !ok {
			t.Fatalf("no matching queue event within %v", timeout)
		}
		if match(entry.Event) {
			return entry
		}
	}
}

func ofType(typ QueueEventType) func(QueueEvent) bool {
	return func(e QueueEvent) bool { return e.Type == typ }
}

func TestQueueEventsJobLifecycle(t *testing.T) {
	q := qeTestQueue(t)
	// Default LastEventID ("$"): events emitted right after creation must
	// not be lost.
	qe := qeNewEvents(t, q.Keys().Name(), QueueEventsOptions{BlockingTimeout: time.Second})
	if !qe.IsRunning() {
		t.Fatal("listener should autorun")
	}
	if err := qe.Run(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("second Run: %v", err)
	}

	ctx := context.Background()
	job, err := q.Add(ctx, "my-job", map[string]any{"foo": "bar"}, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	added := qeWaitFor(t, qe, 10*time.Second, ofType(QueueEventAdded))
	if added.ID == "" || added.Event.JobID != job.ID || added.Event.JobName != "my-job" {
		t.Fatalf("unexpected added event: %+v", added)
	}
	qeWaitFor(t, qe, 10*time.Second, ofType(QueueEventWaiting))

	w, err := NewWorker(ctx, q.Keys().Name(), func(ctx context.Context, job *Job) (any, error) {
		return map[string]any{"ok": true}, nil
	}, &WorkerOptions{Connection: qeTestConnection()})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}
	defer func() { _ = w.Close(5 * time.Second) }()

	active := qeWaitFor(t, qe, 15*time.Second, ofType(QueueEventActive))
	if active.Event.JobID != job.ID || active.Event.Prev != "waiting" {
		t.Fatalf("unexpected active event: %+v", active.Event)
	}
	completed := qeWaitFor(t, qe, 15*time.Second, ofType(QueueEventCompleted))
	if completed.Event.JobID != job.ID {
		t.Fatalf("unexpected completed event: %+v", completed.Event)
	}
	var rv map[string]bool
	if err := completed.Event.DecodeReturnValue(&rv); err != nil || !rv["ok"] {
		t.Fatalf("return value %s: %v", completed.Event.ReturnValue, err)
	}
}

func TestQueueEventsResumeFromLastEventID(t *testing.T) {
	q := qeTestQueue(t)
	ctx := context.Background()
	first, err := q.Add(ctx, "first", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	qe := qeNewEvents(t, q.Keys().Name(), QueueEventsOptions{LastEventID: "0", BlockingTimeout: time.Second, ManualStart: true})
	if qe.IsRunning() {
		t.Fatal("ManualStart listener must not run")
	}
	if err := qe.Run(); err != nil {
		t.Fatal(err)
	}
	entry := qeWaitFor(t, qe, 10*time.Second, ofType(QueueEventAdded))
	if entry.Event.JobID != first.ID {
		t.Fatalf("got %+v", entry.Event)
	}
	if err := qe.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := q.Add(ctx, "second", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	resumed := qeNewEvents(t, q.Keys().Name(), QueueEventsOptions{LastEventID: entry.ID, BlockingTimeout: time.Second})
	got := qeWaitFor(t, resumed, 10*time.Second, ofType(QueueEventAdded))
	if got.Event.JobID != second.ID || got.Event.JobName != "second" {
		t.Fatalf("resumed listener got %+v", got.Event)
	}
}

func TestQueueEventsCloseInterruptsBlockingRead(t *testing.T) {
	name := "test-qe-" + strings.Split(uuid.NewString(), "-")[0]
	for _, block := range []time.Duration{30 * time.Second, -1} { // long and infinite blocks
		qe := qeNewEvents(t, name, QueueEventsOptions{BlockingTimeout: block})
		time.Sleep(200 * time.Millisecond) // let the XREAD BLOCK start

		start := time.Now()
		if err := qe.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("Close took %v with block %v", d, block)
		}
		if qe.IsRunning() {
			t.Fatal("closed listener reports running")
		}
		if err := qe.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		for {
			entry, ok := qe.NextEvent(ctx)
			if !ok {
				break
			}
			if entry.Event.Type == QueueEventError {
				t.Fatalf("Close produced an error event: %s", entry.Event.Message)
			}
		}
		if ctx.Err() != nil {
			t.Fatal("NextEvent should return false once closed, not wait")
		}
		cancel()
		if err := qe.Run(); !errors.Is(err, ErrInvalidConfig) || qe.IsRunning() {
			t.Fatalf("Run after Close: %v, running=%v", err, qe.IsRunning())
		}
	}
}

// TestQueueEventsRecoversAfterConnectionKilled checks that the consume loop
// re-dials after its dedicated connection is dropped. It uses logical
// database 12 (unused by other tests) so the kill does not disturb tests
// running in parallel.
func TestQueueEventsRecoversAfterConnectionKilled(t *testing.T) {
	const killDB = 12
	ctx := context.Background()
	opts, err := redis.ParseURL(qeTestConnection().URL)
	if err != nil {
		t.Fatal(err)
	}
	isolated := *opts
	isolated.DB = killDB
	conn := NewRedisConnectionFromClient(redis.NewClient(&isolated))
	defer func() { _ = conn.Client().Close() }()

	name := "test-qe-" + strings.Split(uuid.NewString(), "-")[0]
	q, err := NewQueueWithConnection(ctx, name, conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Obliterate(ctx, true, 1000) }()
	qe, err := NewQueueEventsWithConnection(ctx, name, conn, &QueueEventsOptions{BlockingTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = qe.Close() }()

	before, err := q.Add(ctx, "before", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if e := qeWaitFor(t, qe, 10*time.Second, ofType(QueueEventAdded)); e.Event.JobID != before.ID {
		t.Fatalf("got %+v", e.Event)
	}

	killer := redis.NewClient(opts)
	defer func() { _ = killer.Close() }()
	list, err := killer.ClientList(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	var killed int64
	for _, line := range strings.Split(list, "\n") {
		if !strings.Contains(" "+line+" ", " db=12 ") {
			continue
		}
		for _, f := range strings.Fields(line) {
			if id, ok := strings.CutPrefix(f, "id="); ok {
				n, _ := killer.Do(ctx, "CLIENT", "KILL", "ID", id).Int64()
				killed += n
			}
		}
	}
	if killed == 0 {
		t.Fatal("expected CLIENT KILL to drop some connections")
	}

	after, err := q.Add(ctx, "after", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := qeWaitFor(t, qe, 15*time.Second, func(e QueueEvent) bool {
		return e.Type == QueueEventAdded && e.JobID == after.ID
	})
	if got.Event.JobName != "after" {
		t.Fatalf("got %+v", got.Event)
	}
}
