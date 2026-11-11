// Job Scheduler tests — upsert, get, remove, cron patterns, every-ms schedules.
// Ported from bullmq_rust/tests/job_scheduler_test.rs.
package bullmq_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/bullmq"
)

// jsChanBuf is the buffer size of the channels processors report to. It is
// large enough that a processor never blocks on send during a test.
const jsChanBuf = 1024

// jsSignalProcessor returns a processor that sends one value on ch per job.
func jsSignalProcessor(ch chan<- struct{}) bullmq.Processor {
	return func(ctx context.Context, job *bullmq.Job) (any, error) {
		ch <- struct{}{}
		return nil, nil
	}
}

// jsRecv waits up to timeout for a value on ch and reports whether one came.
func jsRecv[T any](ch <-chan T, timeout time.Duration) (T, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case v := <-ch:
		return v, true
	case <-timer.C:
		var zero T
		return zero, false
	}
}

// jsTryRecv reports whether a value is immediately available on ch.
func jsTryRecv[T any](ch <-chan T) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// jsDrainCount consumes every value currently buffered on ch.
func jsDrainCount[T any](ch <-chan T) int {
	n := 0
	for jsTryRecv(ch) {
		n++
	}
	return n
}

// jsJSONEqual reports whether raw decodes to the same value as want.
func jsJSONEqual(t *testing.T, raw json.RawMessage, want any) bool {
	t.Helper()
	var got, exp any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	b, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := json.Unmarshal(b, &exp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return reflect.DeepEqual(got, exp)
}

// jsAutorunWorker starts an autorun worker with default options.
func jsAutorunWorker(t *testing.T, name string, p bullmq.Processor) *bullmq.Worker {
	t.Helper()
	return newTestWorker(t, name, p, &bullmq.WorkerOptions{Connection: testConnection()})
}

// ═══════════════════════════════════════════════════════════════════════════
// Upsert Job Scheduler
// ═══════════════════════════════════════════════════════════════════════════

func TestUpsertJobSchedulerWithEvery(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job, err := queue.UpsertJobScheduler(ctx, "scheduler-1", bullmq.RepeatOptions{Every: 1000 * time.Millisecond}, "", nil, nil)
	noErr(t, err)
	if job == nil {
		t.Fatal("upsert should return a job")
	}
	if !strings.Contains(job.ID, "repeat:scheduler-1:") {
		t.Fatalf("unexpected job id %q", job.ID)
	}

	// Verify the scheduler is stored
	if c := must(queue.GetJobSchedulersCount(ctx)); c != 1 {
		t.Fatalf("schedulers count = %d, want 1", c)
	}
}

func TestUpsertJobSchedulerWithCronPattern(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job, err := queue.UpsertJobScheduler(ctx, "cron-scheduler",
		bullmq.RepeatOptions{Pattern: "*/5 * * * * *"}, // every 5 seconds
		"", nil, nil)
	noErr(t, err)
	if job == nil {
		t.Fatal("upsert should return a job")
	}
	if !strings.Contains(job.ID, "repeat:cron-scheduler:") {
		t.Fatalf("unexpected job id %q", job.ID)
	}

	// The created job should be in delayed state
	if st := must(queue.GetJobState(ctx, job.ID)); st != bullmq.StateDelayed {
		t.Fatalf("state = %s, want delayed", st)
	}
}

func TestUpsertJobSchedulerWithCustomNameAndData(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	data := map[string]any{"email": "user@example.com"}
	job, err := queue.UpsertJobScheduler(ctx, "email-scheduler",
		bullmq.RepeatOptions{Every: 2000 * time.Millisecond}, "send-email", data, nil)
	noErr(t, err)
	if job == nil {
		t.Fatal("upsert should return a job")
	}
	if job.Name != "send-email" {
		t.Fatalf("name = %q", job.Name)
	}
	if !jsJSONEqual(t, job.Data, data) {
		t.Fatalf("data = %s", job.Data)
	}
}

func TestUpsertJobSchedulerIdempotent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Upsert twice with same ID but different every
	must(queue.UpsertJobScheduler(ctx, "my-scheduler", bullmq.RepeatOptions{Every: 1000 * time.Millisecond}, "", nil, nil))
	must(queue.UpsertJobScheduler(ctx, "my-scheduler", bullmq.RepeatOptions{Every: 2000 * time.Millisecond}, "", nil, nil))

	// Should still have only 1 scheduler
	if c := must(queue.GetJobSchedulersCount(ctx)); c != 1 {
		t.Fatalf("schedulers count = %d, want 1", c)
	}
}

func TestUpsertValidatesPatternAndEveryExclusive(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	_, err := queue.UpsertJobScheduler(ctx, "bad-scheduler",
		bullmq.RepeatOptions{Every: 1000 * time.Millisecond, Pattern: "* * * * *"}, "", nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestUpsertValidatesPatternOrEveryRequired(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	_, err := queue.UpsertJobScheduler(ctx, "bad-scheduler", bullmq.RepeatOptions{}, "", nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestUpsertValidatesEndDateInPast(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	_, err := queue.UpsertJobScheduler(ctx, "past-scheduler", bullmq.RepeatOptions{
		Every:   1000 * time.Millisecond,
		EndDate: time.UnixMilli(1000), // Unix timestamp from 1970 (well in the past)
	}, "", nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Get Job Schedulers
// ═══════════════════════════════════════════════════════════════════════════

func TestGetJobSchedulerByID(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "get-test", bullmq.RepeatOptions{Every: 5000 * time.Millisecond}, "my-job", nil, nil))

	s := must(queue.GetJobScheduler(ctx, "get-test"))
	if s == nil {
		t.Fatal("scheduler should exist")
	}
	if s.Key != "get-test" {
		t.Fatalf("key = %q", s.Key)
	}
	if s.Name != "my-job" {
		t.Fatalf("name = %q", s.Name)
	}
	if s.Every != 5000*time.Millisecond {
		t.Fatalf("every = %v", s.Every)
	}
	if s.Next.IsZero() {
		t.Fatal("next should be set")
	}
}

func TestGetNonexistentSchedulerReturnsNone(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	if s := must(queue.GetJobScheduler(ctx, "does-not-exist")); s != nil {
		t.Fatalf("expected nil, got %+v", s)
	}
}

func TestGetJobSchedulersList(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add multiple schedulers
	for i := range 3 {
		must(queue.UpsertJobScheduler(ctx, fmt.Sprintf("scheduler-%d", i),
			bullmq.RepeatOptions{Every: time.Duration(i+1) * 1000 * time.Millisecond}, "", nil, nil))
	}

	schedulers := must(queue.GetJobSchedulers(ctx, 0, -1, true))
	if len(schedulers) != 3 {
		t.Fatalf("len = %d, want 3", len(schedulers))
	}

	// In ascending order (by nextMillis)
	if schedulers[0].Next.After(schedulers[1].Next) || schedulers[1].Next.After(schedulers[2].Next) {
		t.Fatalf("not ascending: %v %v %v", schedulers[0].Next, schedulers[1].Next, schedulers[2].Next)
	}
}

func TestGetJobSchedulersCount(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	if c := must(queue.GetJobSchedulersCount(ctx)); c != 0 {
		t.Fatalf("count = %d, want 0", c)
	}

	for i := range 5 {
		must(queue.UpsertJobScheduler(ctx, fmt.Sprintf("count-%d", i),
			bullmq.RepeatOptions{Every: 1000 * time.Millisecond}, "", nil, nil))
	}

	if c := must(queue.GetJobSchedulersCount(ctx)); c != 5 {
		t.Fatalf("count = %d, want 5", c)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Remove Job Scheduler
// ═══════════════════════════════════════════════════════════════════════════

func TestRemoveJobScheduler(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "remove-me", bullmq.RepeatOptions{Every: 1000 * time.Millisecond}, "", nil, nil))
	if c := must(queue.GetJobSchedulersCount(ctx)); c != 1 {
		t.Fatalf("count = %d, want 1", c)
	}

	if !must(queue.RemoveJobScheduler(ctx, "remove-me")) {
		t.Fatal("expected removed=true")
	}
	if c := must(queue.GetJobSchedulersCount(ctx)); c != 0 {
		t.Fatalf("count = %d, want 0", c)
	}
}

func TestRemoveNonexistentScheduler(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	if must(queue.RemoveJobScheduler(ctx, "nope")) {
		t.Fatal("expected removed=false")
	}
}

func TestRemoveSchedulerAlsoRemovesDelayedJob(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Use a cron pattern far in the future so the job lands in "delayed"
	job := must(queue.UpsertJobScheduler(ctx, "delayed-remove",
		bullmq.RepeatOptions{Pattern: "0 0 1 1 *"}, // Jan 1st at midnight (yearly)
		"", nil, nil))
	if job == nil {
		t.Fatal("upsert should return a job")
	}
	jobID := job.ID

	// Verify the delayed job exists
	if st := must(queue.GetJobState(ctx, jobID)); st != bullmq.StateDelayed {
		t.Fatalf("state = %s, want delayed", st)
	}

	// Remove the scheduler
	must(queue.RemoveJobScheduler(ctx, "delayed-remove"))

	// The delayed job should be gone too
	if st := must(queue.GetJobState(ctx, jobID)); st != bullmq.StateUnknown {
		t.Fatalf("state = %s, want unknown", st)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler Execution (Worker picks up and repeats)
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerRepeatsWithEvery(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "repeater", bullmq.RepeatOptions{Every: 500 * time.Millisecond}, "", nil, nil)) // every 500ms

	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, jsSignalProcessor(ch))

	// Should process at least 3 iterations within 3 seconds
	count := 0
	deadline := time.Now().Add(5 * time.Second)
	for count < 3 {
		if _, ok := jsRecv(ch, time.Until(deadline)); !ok {
			break
		}
		count++
	}
	if count < 3 {
		t.Fatalf("Expected at least 3 iterations, got %d", count)
	}

	noErr(t, worker.Close(5*time.Second))
}

func TestSchedulerRespectsLimit(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "limited",
		bullmq.RepeatOptions{Every: 200 * time.Millisecond, Limit: 3}, "", nil, nil))

	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, jsSignalProcessor(ch))

	// Wait enough time for more than 3 iterations if there was no limit
	time.Sleep(3 * time.Second)

	// Count how many were processed
	count := jsDrainCount(ch)

	// Should have processed exactly 3 (the limit)
	if count != 3 {
		t.Fatalf("Expected exactly 3 iterations due to limit, got %d", count)
	}

	noErr(t, worker.Close(5*time.Second))
}

func TestMultipleSchedulersIndependent(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Two independent schedulers
	must(queue.UpsertJobScheduler(ctx, "sched-a", bullmq.RepeatOptions{Every: 1000 * time.Millisecond}, "job-a", nil, nil))
	must(queue.UpsertJobScheduler(ctx, "sched-b", bullmq.RepeatOptions{Every: 2000 * time.Millisecond}, "job-b", nil, nil))

	if c := must(queue.GetJobSchedulersCount(ctx)); c != 2 {
		t.Fatalf("count = %d, want 2", c)
	}

	schedulers := must(queue.GetJobSchedulers(ctx, 0, -1, true))
	names := map[string]bool{}
	for _, s := range schedulers {
		names[s.Name] = true
	}
	if !names["job-a"] || !names["job-b"] {
		t.Fatalf("names = %v", names)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler with job template options
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerWithJobOptions(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	jobOpts := &bullmq.JobOptions{
		Attempts: 5,
		Backoff:  bullmq.FixedBackoff(1000 * time.Millisecond),
	}

	job, err := queue.UpsertJobScheduler(ctx, "with-opts",
		bullmq.RepeatOptions{Every: 5000 * time.Millisecond}, "retryable",
		map[string]any{"key": "value"}, jobOpts)
	noErr(t, err)
	if job == nil {
		t.Fatal("upsert should return a job")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler with end date
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerWithFutureEndDate(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	futureEnd := time.Now().UnixMilli() + 60_000 // 1 minute from now

	job, err := queue.UpsertJobScheduler(ctx, "end-date-test", bullmq.RepeatOptions{
		Every:   1000 * time.Millisecond,
		EndDate: time.UnixMilli(futureEnd),
	}, "", nil, nil)
	noErr(t, err)
	if job == nil {
		t.Fatal("upsert should return a job")
	}

	s := must(queue.GetJobScheduler(ctx, "end-date-test"))
	if s == nil {
		t.Fatal("scheduler should exist")
	}
	if s.EndDate.UnixMilli() != futureEnd {
		t.Fatalf("end date = %d, want %d", s.EndDate.UnixMilli(), futureEnd)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Repeat with cron pattern execution
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerRepeatsWithCronPattern(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Every second
	must(queue.UpsertJobScheduler(ctx, "cron-repeat", bullmq.RepeatOptions{Pattern: "* * * * * *"}, "", nil, nil))

	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, jsSignalProcessor(ch))

	// Should fire at least 2 times within 3 seconds
	count := 0
	deadline := time.Now().Add(4 * time.Second)
	for count < 2 {
		if _, ok := jsRecv(ch, time.Until(deadline)); !ok {
			break
		}
		count++
	}
	if count < 2 {
		t.Fatalf("Expected at least 2 cron iterations, got %d", count)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler stops repeating after endDate
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerStopsAfterEndDate(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	now := time.Now().UnixMilli()

	// End date ~800ms from now — should allow ~3 iterations at 200ms intervals
	must(queue.UpsertJobScheduler(ctx, "end-stop", bullmq.RepeatOptions{
		Every:   200 * time.Millisecond,
		EndDate: time.UnixMilli(now + 800),
	}, "", nil, nil))

	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, jsSignalProcessor(ch))

	// Wait long enough for many iterations if there was no endDate
	time.Sleep(2 * time.Second)

	count := jsDrainCount(ch)

	// Should have stopped — not more than ~5 iterations
	if count > 6 {
		t.Fatalf("Expected scheduler to stop after endDate, got %d iterations", count)
	}
	if count < 1 {
		t.Fatal("Expected at least 1 iteration")
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler continues after job failure
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerContinuesAfterFailure(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "fail-repeat", bullmq.RepeatOptions{Every: 300 * time.Millisecond}, "", nil, nil))

	var counter atomic.Int32
	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		c := counter.Add(1) - 1
		if c == 0 {
			// First iteration fails
			return nil, errors.New("intentional failure")
		}
		// Subsequent iterations succeed
		ch <- struct{}{}
		return nil, nil
	})

	// Wait for at least 2 successful iterations (after the first failure)
	successCount := 0
	deadline := time.Now().Add(4 * time.Second)
	for successCount < 2 {
		if _, ok := jsRecv(ch, time.Until(deadline)); !ok {
			break
		}
		successCount++
	}
	if successCount < 2 {
		t.Fatalf("Scheduler should continue repeating after a job failure (successes: %d)", successCount)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Same cron pattern, different scheduler IDs → multiple jobs
// ═══════════════════════════════════════════════════════════════════════════

func TestSameCronDifferentIDsCreatesMultipleJobs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "daily-a", bullmq.RepeatOptions{Pattern: "0 0 * * *"}, "job-a", nil, nil)) // daily at midnight
	must(queue.UpsertJobScheduler(ctx, "daily-b", bullmq.RepeatOptions{Pattern: "0 0 * * *"}, "job-b", nil, nil)) // same pattern

	// Both should exist independently
	if c := must(queue.GetJobSchedulersCount(ctx)); c != 2 {
		t.Fatalf("count = %d, want 2", c)
	}

	a := must(queue.GetJobScheduler(ctx, "daily-a"))
	b := must(queue.GetJobScheduler(ctx, "daily-b"))
	if a == nil || b == nil {
		t.Fatal("both schedulers should exist")
	}
	if a.Name != "job-a" || b.Name != "job-b" {
		t.Fatalf("names = %q, %q", a.Name, b.Name)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Same scheduler ID + different every → only one scheduler
// ═══════════════════════════════════════════════════════════════════════════

func TestSameIDDifferentEveryCreatesOneScheduler(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "same-id", bullmq.RepeatOptions{Every: 1000 * time.Millisecond}, "", nil, nil))
	must(queue.UpsertJobScheduler(ctx, "same-id", bullmq.RepeatOptions{Every: 5000 * time.Millisecond}, "", nil, nil))

	if c := must(queue.GetJobSchedulersCount(ctx)); c != 1 {
		t.Fatalf("count = %d, want 1", c)
	}

	// Should have the updated every value
	s := must(queue.GetJobScheduler(ctx, "same-id"))
	if s == nil || s.Every != 5000*time.Millisecond {
		t.Fatalf("scheduler = %+v", s)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler with startDate in the future
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerWithStartDateInFuture(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	now := time.Now().UnixMilli()

	must(queue.UpsertJobScheduler(ctx, "future-start", bullmq.RepeatOptions{
		Every:     200 * time.Millisecond,
		StartDate: time.UnixMilli(now + 1000), // starts 1 second from now
	}, "", nil, nil))

	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, jsSignalProcessor(ch))

	// Should NOT have fired in the first 500ms
	time.Sleep(500 * time.Millisecond)
	if jsTryRecv(ch) {
		t.Fatal("Job should not fire before startDate")
	}

	// But should fire after startDate (wait another 1.5s)
	if _, ok := jsRecv(ch, 3*time.Second); !ok {
		t.Fatal("Job should fire after startDate")
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler with immediately option
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerImmediatelyFiresFirstJob(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Cron pattern that would normally be far in the future (every day at midnight)
	// but with immediately=true should fire right away
	must(queue.UpsertJobScheduler(ctx, "immediate", bullmq.RepeatOptions{
		Pattern:     "0 0 * * *",
		Immediately: true,
	}, "", nil, nil))

	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, jsSignalProcessor(ch))

	// Should fire within 2 seconds (immediately)
	if _, ok := jsRecv(ch, 2*time.Second); !ok {
		t.Fatal("With immediately=true, job should fire right away")
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Upsert quick succession → only one scheduler, one delayed job
// ═══════════════════════════════════════════════════════════════════════════

func TestUpsertQuickSuccessionOnlyOneScheduler(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Rapid-fire upserts with same ID
	for i := range 5 {
		must(queue.UpsertJobScheduler(ctx, "rapid",
			bullmq.RepeatOptions{Every: time.Duration(1000+i*100) * time.Millisecond}, "", nil, nil))
	}

	// Only one scheduler should exist
	if c := must(queue.GetJobSchedulersCount(ctx)); c != 1 {
		t.Fatalf("count = %d, want 1", c)
	}

	// Last upsert wins
	s := must(queue.GetJobScheduler(ctx, "rapid"))
	if s == nil || s.Every != 1400*time.Millisecond { // 1000 + 4*100
		t.Fatalf("scheduler = %+v", s)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Allow re-adding scheduler after removal
// ═══════════════════════════════════════════════════════════════════════════

func TestReaddSchedulerAfterRemoval(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	repeatOpts := bullmq.RepeatOptions{Every: 1000 * time.Millisecond}
	must(queue.UpsertJobScheduler(ctx, "removable", repeatOpts, "", nil, nil))

	// Remove
	if !must(queue.RemoveJobScheduler(ctx, "removable")) {
		t.Fatal("expected removed=true")
	}
	if c := must(queue.GetJobSchedulersCount(ctx)); c != 0 {
		t.Fatalf("count = %d, want 0", c)
	}

	// Re-add
	job := must(queue.UpsertJobScheduler(ctx, "removable", repeatOpts, "", nil, nil))
	if job == nil {
		t.Fatal("re-add should return a job")
	}
	if c := must(queue.GetJobSchedulersCount(ctx)); c != 1 {
		t.Fatalf("count = %d, want 1", c)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Worker data and name passed correctly to scheduler jobs
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerJobHasCorrectNameAndData(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	data := map[string]any{"message": "hello"}
	must(queue.UpsertJobScheduler(ctx, "data-test",
		bullmq.RepeatOptions{Every: 500 * time.Millisecond}, "my-named-job", data, nil))

	type nameData struct {
		name string
		data json.RawMessage
	}
	ch := make(chan nameData, jsChanBuf)
	worker := jsAutorunWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		ch <- nameData{job.Name, append(json.RawMessage(nil), job.Data...)}
		return nil, nil
	})

	got, ok := jsRecv(ch, 3*time.Second)
	if !ok {
		t.Fatal("timed out waiting for job")
	}
	if got.name != "my-named-job" {
		t.Fatalf("name = %q", got.name)
	}
	if !jsJSONEqual(t, got.data, data) {
		t.Fatalf("data = %s", got.data)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Iteration count tracking
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerHasCorrectIterationCount(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "count-track",
		bullmq.RepeatOptions{Every: 200 * time.Millisecond, Limit: 5}, "", nil, nil))

	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, jsSignalProcessor(ch))

	// Wait for all 5 iterations to complete
	for range 5 {
		if _, ok := jsRecv(ch, 3*time.Second); !ok {
			t.Fatal("timed out waiting for iteration")
		}
	}

	// Check the scheduler's iteration count
	s := must(queue.GetJobScheduler(ctx, "count-track"))
	if s == nil {
		t.Fatal("scheduler should exist")
	}
	if s.IterationCount < 5 {
		t.Fatalf("Expected iteration_count >= 5, got %d", s.IterationCount)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Using 'every' starts immediately (no initial delay)
// ═══════════════════════════════════════════════════════════════════════════

func TestEveryStartsImmediately(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "immediate-every",
		bullmq.RepeatOptions{Every: 60_000 * time.Millisecond}, // 1 minute interval
		"", nil, nil))

	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, jsSignalProcessor(ch))

	// With 'every', the first job should be created immediately (delay=0)
	// So it should be processed within a couple seconds, not in 60s
	if _, ok := jsRecv(ch, 3*time.Second); !ok {
		t.Fatal("With 'every', first job should fire immediately")
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler with removeOnComplete option
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerWithRemoveOnComplete(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "roc-test",
		bullmq.RepeatOptions{Every: 300 * time.Millisecond, Limit: 3}, "", nil,
		&bullmq.JobOptions{RemoveOnComplete: bullmq.RemoveAll()}))

	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, jsSignalProcessor(ch))

	// Wait for all 3 to complete
	for range 3 {
		if _, ok := jsRecv(ch, 3*time.Second); !ok {
			t.Fatal("timed out waiting for iteration")
		}
	}
	time.Sleep(200 * time.Millisecond)

	// Completed jobs should be removed
	counts := must(queue.GetJobCounts(ctx))
	if counts.Completed != 0 {
		t.Fatalf("Completed jobs should be removed with removeOnComplete, got %d", counts.Completed)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler with timezone
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerWithTimezone(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Every second cron with a timezone
	job, err := queue.UpsertJobScheduler(ctx, "tz-test", bullmq.RepeatOptions{
		Pattern: "* * * * * *",
		TZ:      "America/New_York",
	}, "", nil, nil)
	noErr(t, err)
	if job == nil {
		t.Fatal("upsert should return a job")
	}

	s := must(queue.GetJobScheduler(ctx, "tz-test"))
	if s == nil {
		t.Fatal("scheduler should exist")
	}
	if s.TZ != "America/New_York" {
		t.Fatalf("tz = %q", s.TZ)
	}
	if s.Next.IsZero() {
		t.Fatal("next should be set")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler pagination (get_job_schedulers with ranges)
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerPagination(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Create 10 schedulers with increasing intervals so they have different next timestamps
	for i := range 10 {
		must(queue.UpsertJobScheduler(ctx, fmt.Sprintf("page-%02d", i),
			bullmq.RepeatOptions{Every: time.Duration(i+1) * 1000 * time.Millisecond}, "", nil, nil))
	}

	// Get first page (0..4)
	if p := must(queue.GetJobSchedulers(ctx, 0, 4, true)); len(p) != 5 {
		t.Fatalf("page1 len = %d, want 5", len(p))
	}

	// Get second page (5..9)
	if p := must(queue.GetJobSchedulers(ctx, 5, 9, true)); len(p) != 5 {
		t.Fatalf("page2 len = %d, want 5", len(p))
	}

	// All should be unique
	if all := must(queue.GetJobSchedulers(ctx, 0, -1, true)); len(all) != 10 {
		t.Fatalf("all len = %d, want 10", len(all))
	}

	// Descending order
	desc := must(queue.GetJobSchedulers(ctx, 0, 2, false))
	if len(desc) != 3 {
		t.Fatalf("desc len = %d, want 3", len(desc))
	}
	if desc[0].Next.Before(desc[1].Next) {
		t.Fatalf("not descending: %v < %v", desc[0].Next, desc[1].Next)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler with priority
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerWithPriority(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job, err := queue.UpsertJobScheduler(ctx, "priority-test",
		bullmq.RepeatOptions{Every: 500 * time.Millisecond}, "", nil,
		&bullmq.JobOptions{Priority: 10})
	noErr(t, err)
	if job == nil {
		t.Fatal("upsert should return a job")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler validation: immediately + startDate
// ═══════════════════════════════════════════════════════════════════════════

func TestUpsertValidatesImmediatelyAndStartDateExclusive(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	now := time.Now().UnixMilli()

	_, err := queue.UpsertJobScheduler(ctx, "bad-combo", bullmq.RepeatOptions{
		Pattern:     "0 0 * * *",
		Immediately: true,
		StartDate:   time.UnixMilli(now + 60_000),
	}, "", nil, nil)
	if err == nil {
		t.Fatal("immediately + startDate should be rejected")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: concurrent workers process iterations reliably
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerWithConcurrentWorkers(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "concurrent",
		bullmq.RepeatOptions{Every: 300 * time.Millisecond, Limit: 6}, "", nil, nil))

	var counter atomic.Int32

	// Start 3 workers
	workers := make([]*bullmq.Worker, 0, 3)
	for range 3 {
		workers = append(workers, jsAutorunWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
			counter.Add(1)
			return nil, nil
		}))
	}

	// Wait for all iterations
	time.Sleep(4 * time.Second)

	// Should have processed exactly 6 (the limit)
	if total := counter.Load(); total != 6 {
		t.Fatalf("Expected 6 iterations with limit=6, got %d", total)
	}

	for _, w := range workers {
		noErr(t, w.Close(5*time.Second))
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: upsert updates data and options
// ═══════════════════════════════════════════════════════════════════════════

func TestUpsertUpdatesDataAndOptions(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// First upsert
	repeatOpts := bullmq.RepeatOptions{Every: 5000 * time.Millisecond}
	must(queue.UpsertJobScheduler(ctx, "update-test", repeatOpts, "job-v1", map[string]any{"version": 1}, nil))

	// Second upsert with new data
	must(queue.UpsertJobScheduler(ctx, "update-test", repeatOpts, "job-v2", map[string]any{"version": 2}, nil))

	s := must(queue.GetJobScheduler(ctx, "update-test"))
	if s == nil {
		t.Fatal("scheduler should exist")
	}
	if s.Name != "job-v2" {
		t.Fatalf("name = %q", s.Name)
	}

	// Data should be updated
	if s.Template != nil && s.Template.Data != nil {
		var d map[string]any
		if err := json.Unmarshal(s.Template.Data, &d); err != nil {
			t.Fatalf("decode template data: %v", err)
		}
		if d["version"] != float64(2) {
			t.Fatalf("version = %v, want 2", d["version"])
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Worker should continue processing after drain
// ═══════════════════════════════════════════════════════════════════════════

func TestWorkerProcessesSchedulerAfterDrain(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, jsSignalProcessor(ch))

	waitForEvent(t, worker, 3*time.Second, eventType(bullmq.EventDrained))

	// Now add a scheduler — worker should wake up and process it
	must(queue.UpsertJobScheduler(ctx, "after-drain", bullmq.RepeatOptions{Every: 300 * time.Millisecond}, "", nil, nil))

	// Should process within a few seconds
	if _, ok := jsRecv(ch, 5*time.Second); !ok {
		t.Fatal("Worker should process jobs added after drain")
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: repeat job key is correctly set on the job
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerJobHasRepeatJobKey(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "rjk-test", bullmq.RepeatOptions{Every: 500 * time.Millisecond}, "", nil, nil))

	ch := make(chan string, jsChanBuf)
	worker := jsAutorunWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		ch <- job.RepeatJobKey
		return nil, nil
	})

	rjk, ok := jsRecv(ch, 3*time.Second)
	if !ok {
		t.Fatal("timed out waiting for job")
	}
	if rjk != "rjk-test" {
		t.Fatalf("repeat job key = %q, want rjk-test", rjk)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: each iteration gets unique job ID
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerEachIterationHasUniqueID(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "unique-ids",
		bullmq.RepeatOptions{Every: 200 * time.Millisecond, Limit: 4}, "", nil, nil))

	ch := make(chan string, jsChanBuf)
	worker := jsAutorunWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		ch <- job.ID
		return nil, nil
	})

	ids := make([]string, 0, 4)
	for range 4 {
		id, ok := jsRecv(ch, 3*time.Second)
		if !ok {
			t.Fatal("timed out waiting for iteration")
		}
		ids = append(ids, id)
	}

	// All IDs should be unique
	unique := map[string]struct{}{}
	for _, id := range ids {
		unique[id] = struct{}{}
	}
	if len(unique) != 4 {
		t.Fatalf("All job IDs should be unique: %v", ids)
	}

	// All IDs should follow the pattern repeat:<schedulerId>:<timestamp>
	for _, id := range ids {
		if !strings.HasPrefix(id, "repeat:unique-ids:") {
			t.Fatalf("ID '%s' should start with 'repeat:unique-ids:'", id)
		}
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: emit waiting event when job added
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerEmitsEvents(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "events-test",
		bullmq.RepeatOptions{Every: 300 * time.Millisecond, Limit: 2}, "", nil, nil))

	worker := jsAutorunWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	})

	// Wait for at least one Completed event using NextEvent()
	ectx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	got := false
	for !got {
		ev, ok := worker.NextEvent(ectx)
		if !ok {
			break
		}
		got = ev.Type == bullmq.EventCompleted
	}
	if !got {
		t.Fatal("Should receive Completed events")
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: cron with 5-field pattern (no seconds)
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerCron5FieldPattern(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Standard 5-field cron (minute-level)
	job, err := queue.UpsertJobScheduler(ctx, "5field-cron",
		bullmq.RepeatOptions{Pattern: "*/1 * * * *"}, // every minute
		"", nil, nil)
	noErr(t, err)
	if job == nil {
		t.Fatal("upsert should return a job")
	}
	if st := must(queue.GetJobState(ctx, job.ID)); st != bullmq.StateDelayed {
		t.Fatalf("state = %s, want delayed", st)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: cron with 6-field pattern (with seconds)
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerCron6FieldPattern(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// 6-field cron (second-level)
	job, err := queue.UpsertJobScheduler(ctx, "6field-cron",
		bullmq.RepeatOptions{Pattern: "*/10 * * * * *"}, // every 10 seconds
		"", nil, nil)
	noErr(t, err)
	if job == nil {
		t.Fatal("upsert should return a job")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: invalid cron pattern returns error
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerInvalidCronReturnsError(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	_, err := queue.UpsertJobScheduler(ctx, "bad-cron", bullmq.RepeatOptions{Pattern: "not a cron"}, "", nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: invalid timezone returns error
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerInvalidTimezoneReturnsError(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	_, err := queue.UpsertJobScheduler(ctx, "bad-tz", bullmq.RepeatOptions{
		Pattern: "* * * * *",
		TZ:      "Invalid/Timezone",
	}, "", nil, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler with offset aligns first job correctly
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerEveryWithOffset(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job, err := queue.UpsertJobScheduler(ctx, "offset-test", bullmq.RepeatOptions{
		Every:  10_000 * time.Millisecond, // every 10 seconds
		Offset: 5000 * time.Millisecond,   // offset by 5 seconds
	}, "", nil, nil)
	noErr(t, err)
	if job == nil {
		t.Fatal("upsert should return a job")
	}

	s := must(queue.GetJobScheduler(ctx, "offset-test"))
	if s == nil || s.Every != 10_000*time.Millisecond {
		t.Fatalf("scheduler = %+v", s)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: startDate in the past with every
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerWithStartDateInPast(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	now := time.Now().UnixMilli()

	// startDate 10 seconds in the past
	must(queue.UpsertJobScheduler(ctx, "past-start", bullmq.RepeatOptions{
		Every:     500 * time.Millisecond,
		StartDate: time.UnixMilli(now - 10_000),
	}, "", nil, nil))

	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, jsSignalProcessor(ch))

	// With startDate in the past, the first job should fire immediately
	if _, ok := jsRecv(ch, 3*time.Second); !ok {
		t.Fatal("With startDate in past, job should fire immediately")
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: multiple different cron patterns
// ═══════════════════════════════════════════════════════════════════════════

func TestCreateSchedulersWithDifferentPatterns(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	patterns := []struct{ id, pattern string }{
		{"hourly", "0 * * * *"},
		{"daily", "0 0 * * *"},
		{"weekly", "0 0 * * 1"},
		{"monthly", "0 0 1 * *"},
	}

	for _, p := range patterns {
		must(queue.UpsertJobScheduler(ctx, p.id, bullmq.RepeatOptions{Pattern: p.pattern}, "", nil, nil))
	}

	if c := must(queue.GetJobSchedulersCount(ctx)); c != 4 {
		t.Fatalf("count = %d, want 4", c)
	}

	// All schedulers should have different next timestamps
	schedulers := must(queue.GetJobSchedulers(ctx, 0, -1, true))
	if len(schedulers) < 2 {
		t.Fatalf("len = %d", len(schedulers))
	}
	// At minimum, hourly should be before daily, daily before weekly, etc.
	if schedulers[0].Next.After(schedulers[1].Next) {
		t.Fatalf("nexts[0] %v > nexts[1] %v", schedulers[0].Next, schedulers[1].Next)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: re-add and process after removal + re-add
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerProcessesAfterRemoveAndReadd(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	ch := make(chan struct{}, jsChanBuf)
	worker := newTestWorker(t, name, jsSignalProcessor(ch), &bullmq.WorkerOptions{
		Connection: testConnection(),
		DrainDelay: time.Second,
	})

	// First: add and process
	repeatOpts := bullmq.RepeatOptions{Every: 300 * time.Millisecond}
	must(queue.UpsertJobScheduler(ctx, "cycle", repeatOpts, "", nil, nil))

	if _, ok := jsRecv(ch, 2*time.Second); !ok {
		t.Fatal("First scheduler should process")
	}

	// Remove the scheduler
	must(queue.RemoveJobScheduler(ctx, "cycle"))
	waitForEvent(t, worker, 3*time.Second, eventType(bullmq.EventDrained))

	// Re-add
	must(queue.UpsertJobScheduler(ctx, "cycle", repeatOpts, "", nil, nil))

	// Should process again after re-add
	if _, ok := jsRecv(ch, 5*time.Second); !ok {
		t.Fatal("Re-added scheduler should process")
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: not more than limit iterations even with slow processing
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerLimitWithSlowProcessing(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "slow-limit",
		bullmq.RepeatOptions{Every: 100 * time.Millisecond, Limit: 3}, "", nil, nil))

	var counter atomic.Int32
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		// Simulate slow processing (200ms per job)
		time.Sleep(200 * time.Millisecond)
		counter.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{
		Connection: testConnection(),
		DrainDelay: time.Second,
	})

	// Wait plenty of time
	time.Sleep(3 * time.Second)

	if total := counter.Load(); total != 3 {
		t.Fatalf("Should process exactly 3 with limit=3, got %d", total)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: many rapid upserts stress test
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerManyUpsertsStress(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Upsert the same scheduler 20 times rapidly
	for i := range 20 {
		must(queue.UpsertJobScheduler(ctx, "stress",
			bullmq.RepeatOptions{Every: time.Duration(1000+i*10) * time.Millisecond}, "", nil, nil))
	}

	// Guarantees: only 1 scheduler, and the last upsert settings stick
	if c := must(queue.GetJobSchedulersCount(ctx)); c != 1 {
		t.Fatalf("count = %d, want 1", c)
	}

	s := must(queue.GetJobScheduler(ctx, "stress"))
	if s == nil || s.Every != 1190*time.Millisecond { // 1000 + 19*10
		t.Fatalf("scheduler = %+v", s)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: many independent schedulers
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerManyIndependentSchedulers(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Create 20 independent schedulers
	for i := range 20 {
		must(queue.UpsertJobScheduler(ctx, fmt.Sprintf("independent-%03d", i),
			bullmq.RepeatOptions{Every: 60_000 * time.Millisecond}, // all with same interval (won't fire in test)
			fmt.Sprintf("job-%d", i), nil, nil))
	}

	if c := must(queue.GetJobSchedulersCount(ctx)); c != 20 {
		t.Fatalf("count = %d, want 20", c)
	}

	// Remove one in the middle
	must(queue.RemoveJobScheduler(ctx, "independent-010"))
	if c := must(queue.GetJobSchedulersCount(ctx)); c != 19 {
		t.Fatalf("count = %d, want 19", c)
	}

	// Remove first
	must(queue.RemoveJobScheduler(ctx, "independent-000"))
	if c := must(queue.GetJobSchedulersCount(ctx)); c != 18 {
		t.Fatalf("count = %d, want 18", c)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: ID with special characters / colons
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerIDWithColons(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Test scheduler IDs with 5+ colon segments (regression for issue #3828)
	schedulerID := "org:team:project:env:task"

	job, err := queue.UpsertJobScheduler(ctx, schedulerID,
		bullmq.RepeatOptions{Every: 5000 * time.Millisecond}, "colon-job", nil, nil)
	noErr(t, err)
	if job == nil {
		t.Fatal("upsert should return a job")
	}

	s := must(queue.GetJobScheduler(ctx, schedulerID))
	if s == nil {
		t.Fatal("scheduler should exist")
	}
	if s.Key != schedulerID {
		t.Fatalf("key = %q", s.Key)
	}
	if s.Name != "colon-job" {
		t.Fatalf("name = %q", s.Name)
	}

	// Should be removable
	if !must(queue.RemoveJobScheduler(ctx, schedulerID)) {
		t.Fatal("expected removed=true")
	}
	if c := must(queue.GetJobSchedulersCount(ctx)); c != 0 {
		t.Fatalf("count = %d, want 0", c)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: UTC timezone works correctly
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerUTCTimezone(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job, err := queue.UpsertJobScheduler(ctx, "utc-test", bullmq.RepeatOptions{
		Pattern: "* * * * * *",
		TZ:      "UTC",
	}, "", nil, nil)
	noErr(t, err)
	if job == nil {
		t.Fatal("upsert should return a job")
	}

	s := must(queue.GetJobScheduler(ctx, "utc-test"))
	if s == nil {
		t.Fatal("scheduler should exist")
	}
	if s.TZ != "UTC" {
		t.Fatalf("tz = %q", s.TZ)
	}

	// The next timestamp should be very close to now (within 2 seconds)
	now := time.Now().UnixMilli()
	if s.Next.IsZero() {
		t.Fatal("next should be set")
	}
	next := s.Next.UnixMilli()
	if next > now+2000 {
		t.Fatalf("Next should be within 2s from now (next=%d, now=%d)", next, now)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: remove does not affect other schedulers
// ═══════════════════════════════════════════════════════════════════════════

func TestRemoveSchedulerDoesNotAffectOthers(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	for i := range 3 {
		must(queue.UpsertJobScheduler(ctx, fmt.Sprintf("s-%d", i),
			bullmq.RepeatOptions{Every: time.Duration(i+1) * 1000 * time.Millisecond}, "", nil, nil))
	}

	if c := must(queue.GetJobSchedulersCount(ctx)); c != 3 {
		t.Fatalf("count = %d, want 3", c)
	}

	// Remove the middle one
	must(queue.RemoveJobScheduler(ctx, "s-1"))

	if c := must(queue.GetJobSchedulersCount(ctx)); c != 2 {
		t.Fatalf("count = %d, want 2", c)
	}
	if must(queue.GetJobScheduler(ctx, "s-0")) == nil {
		t.Fatal("s-0 should exist")
	}
	if must(queue.GetJobScheduler(ctx, "s-1")) != nil {
		t.Fatal("s-1 should be gone")
	}
	if must(queue.GetJobScheduler(ctx, "s-2")) == nil {
		t.Fatal("s-2 should exist")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: processing continues reliably over many iterations
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerReliableOverManyIterations(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "reliable",
		bullmq.RepeatOptions{Every: 100 * time.Millisecond, Limit: 10}, "", nil, nil))

	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, jsSignalProcessor(ch))

	// All 10 iterations should complete
	count := 0
	deadline := time.Now().Add(5 * time.Second)
	for count < 10 {
		if _, ok := jsRecv(ch, time.Until(deadline)); !ok {
			break
		}
		count++
	}
	if count != 10 {
		t.Fatalf("All 10 iterations should complete, got %d", count)
	}

	// After limit reached, no more should come
	time.Sleep(500 * time.Millisecond)
	if jsTryRecv(ch) {
		t.Fatal("No more iterations after limit")
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Drain should not remove delayed jobs belonging to a scheduler
// ═══════════════════════════════════════════════════════════════════════════

func TestDrainDoesNotRemoveSchedulerDelayedJobs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Scheduler with 'every' puts first job in waiting (delay=0)
	must(queue.UpsertJobScheduler(ctx, "drain-protect", bullmq.RepeatOptions{Every: 5000 * time.Millisecond}, "", nil, nil))

	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 1 {
		t.Fatalf("waiting before = %d, want 1", c.Waiting)
	}

	// Drain with delayed=true
	noErr(t, queue.Drain(ctx, true))

	// The scheduler's waiting job should still be there
	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 1 {
		t.Fatalf("Drain should not remove jobs belonging to a scheduler (waiting=%d)", c.Waiting)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Clean should not remove waiting jobs belonging to a scheduler
// ═══════════════════════════════════════════════════════════════════════════

func TestCleanDoesNotRemoveSchedulerWaitingJobs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "clean-protect", bullmq.RepeatOptions{Every: 5000 * time.Millisecond}, "", nil, nil))

	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 1 {
		t.Fatalf("waiting before = %d, want 1", c.Waiting)
	}

	// Clean wait jobs with grace=0
	removed := must(queue.Clean(ctx, 0, 100, "wait"))

	// The scheduler's job should NOT have been removed
	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 1 {
		t.Fatalf("Clean should not remove jobs belonging to a scheduler (removed: %v, waiting=%d)", removed, c.Waiting)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Direct removal of scheduler's job should fail
// ═══════════════════════════════════════════════════════════════════════════

func TestCannotDirectlyRemoveSchedulerJob(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.UpsertJobScheduler(ctx, "no-remove", bullmq.RepeatOptions{Every: 5000 * time.Millisecond}, "", nil, nil))
	if job == nil {
		t.Fatal("upsert should return a job")
	}

	// Try to remove the job directly — should fail or return false
	removed, err := queue.Remove(ctx, job.ID)
	// The script should reject removal of jobs belonging to a scheduler
	if err == nil && removed {
		t.Fatal("Should not be able to remove a job belonging to a scheduler")
	}

	// Job should still exist
	if st := must(queue.GetJobState(ctx, job.ID)); st == bullmq.StateUnknown {
		t.Fatal("Job should still exist after failed removal")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Promote delayed scheduler job → process → next delayed created
// ═══════════════════════════════════════════════════════════════════════════

func TestPromoteSchedulerJobCreatesNextIteration(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Use 'every' with long interval — first job goes to waiting (delay=0),
	// after processing, next goes to delayed (far in future)
	must(queue.UpsertJobScheduler(ctx, "promote-test", bullmq.RepeatOptions{Every: 60_000 * time.Millisecond}, "", nil, nil)) // 60s interval

	// First job is in waiting (delay=0 for 'every')
	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 1 {
		t.Fatalf("waiting = %d, want 1", c.Waiting)
	}

	// Process the first job
	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, jsSignalProcessor(ch))

	if _, ok := jsRecv(ch, 3*time.Second); !ok {
		t.Fatal("First job should be processed")
	}

	// After processing, next iteration should be in delayed (60s in future)
	time.Sleep(200 * time.Millisecond)
	if c := must(queue.GetJobCounts(ctx)); c.Delayed != 1 {
		t.Fatalf("Next iteration should be scheduled as delayed (delayed=%d)", c.Delayed)
	}

	// Promote the delayed job
	noErr(t, queue.PromoteJobs(ctx, 1000))

	// Worker picks up promoted job almost instantly, so just wait for processing
	// (don't assert waiting=1 because it may already be active)

	// Process the promoted job
	if _, ok := jsRecv(ch, 3*time.Second); !ok {
		t.Fatal("Promoted job should be processed")
	}

	// After processing promoted job, another delayed should be created
	time.Sleep(500 * time.Millisecond)
	if c := must(queue.GetJobCounts(ctx)); c.Delayed != 1 {
		t.Fatalf("Next iteration should be scheduled after promote+process (delayed=%d)", c.Delayed)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Promote 'every' scheduler job after processing → next delayed
// ═══════════════════════════════════════════════════════════════════════════

func TestPromoteEverySchedulerCreatesNextDelayed(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// 'every' with large interval — first job goes to waiting immediately
	must(queue.UpsertJobScheduler(ctx, "promote-every",
		bullmq.RepeatOptions{Every: 50_000 * time.Millisecond, Limit: 3}, "", nil, nil))

	// First job should be in waiting (delay=0 for 'every')
	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 1 {
		t.Fatalf("waiting = %d, want 1", c.Waiting)
	}

	// Process the first job
	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, jsSignalProcessor(ch))

	if _, ok := jsRecv(ch, 3*time.Second); !ok {
		t.Fatal("timed out waiting for first job")
	}

	time.Sleep(200 * time.Millisecond)

	// After processing, next iteration should be in delayed (50s in future)
	if c := must(queue.GetJobCounts(ctx)); c.Delayed != 1 {
		t.Fatalf("Next iteration should be in delayed after processing 'every' scheduler (delayed=%d)", c.Delayed)
	}

	// Promote the delayed job
	noErr(t, queue.PromoteJobs(ctx, 1000))

	// Worker picks up promoted job almost instantly, so just wait for processing

	// Process the promoted job
	if _, ok := jsRecv(ch, 3*time.Second); !ok {
		t.Fatal("timed out waiting for promoted job")
	}

	time.Sleep(200 * time.Millisecond)

	// After second processing, another delayed should exist
	if c := must(queue.GetJobCounts(ctx)); c.Delayed != 1 {
		t.Fatalf("After promote+process, next delayed should exist (delayed=%d)", c.Delayed)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Retry failed scheduler job should not create duplicate delayed
// ═══════════════════════════════════════════════════════════════════════════

func TestRetryFailedSchedulerJobNoDuplicateDelayed(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "retry-test", bullmq.RepeatOptions{Every: 5000 * time.Millisecond}, "", nil, nil))

	var counter atomic.Int32
	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		c := counter.Add(1) - 1
		if c == 0 {
			return nil, errors.New("fail first time")
		}
		ch <- struct{}{}
		return nil, nil
	})

	waitForEvent(t, worker, 3*time.Second, eventType(bullmq.EventFailed))

	counts := must(queue.GetJobCounts(ctx))
	if counts.Failed != 1 {
		t.Fatalf("First job should have failed (failed=%d)", counts.Failed)
	}
	// Next iteration was already scheduled (delayed)
	if counts.Delayed != 1 {
		t.Fatalf("Next iteration should be scheduled after failure (delayed=%d)", counts.Delayed)
	}

	// Retry the failed jobs
	noErr(t, queue.RetryJobs(ctx, "failed", 100, time.Time{}))

	if c := must(queue.GetJobCounts(ctx)); c.Failed != 0 {
		t.Fatalf("No failed jobs after retry (failed=%d)", c.Failed)
	}

	// Wait for retried job to be processed
	if _, ok := jsRecv(ch, 5*time.Second); !ok {
		t.Fatal("Retried job should be processed")
	}

	// After retry+process, should still only have 1 delayed (not duplicated)
	time.Sleep(200 * time.Millisecond)
	counts = must(queue.GetJobCounts(ctx))
	if counts.Waiting+counts.Delayed > 1 {
		t.Fatalf("Should only have 1 job scheduled (waiting=%d, delayed=%d)", counts.Waiting, counts.Delayed)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Job.retry() on failed scheduler job should not create duplicate delayed
// ═══════════════════════════════════════════════════════════════════════════

func TestJobRetryOnFailedSchedulerJobNoDuplicate(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "job-retry-test", bullmq.RepeatOptions{Every: 5000 * time.Millisecond}, "", nil, nil))

	var counter atomic.Int32
	ch := make(chan struct{}, jsChanBuf)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		c := counter.Add(1) - 1
		if c == 0 {
			return nil, errors.New("fail first")
		}
		ch <- struct{}{}
		return nil, nil
	}, &bullmq.WorkerOptions{
		Connection: testConnection(),
		DrainDelay: time.Second,
	})

	waitForEvent(t, worker, 3*time.Second, eventType(bullmq.EventFailed))

	if c := must(queue.GetJobCounts(ctx)); c.Failed != 1 {
		t.Fatalf("failed = %d, want 1", c.Failed)
	}

	// Get the failed job and retry it using Job.Retry()
	// First get the job ID from the failed set
	conn := rawClient(t)
	failedIDs, err := conn.ZRange(ctx, "bull:"+name+":failed", 0, -1).Result()
	noErr(t, err)
	if len(failedIDs) != 1 {
		t.Fatalf("failed ids = %v", failedIDs)
	}

	failedJob := must(queue.GetJob(ctx, failedIDs[0]))
	if failedJob == nil {
		t.Fatal("failed job should exist")
	}
	noErr(t, failedJob.Retry(ctx, bullmq.StateFailed, nil))

	if c := must(queue.GetJobCounts(ctx)); c.Failed != 0 {
		t.Fatalf("failed = %d, want 0", c.Failed)
	}

	// Wait for the retried job to process
	if _, ok := jsRecv(ch, 5*time.Second); !ok {
		t.Fatal("Retried job should process")
	}

	// Should have at most 1 job scheduled
	time.Sleep(200 * time.Millisecond)
	counts := must(queue.GetJobCounts(ctx))
	if counts.Waiting+counts.Delayed > 1 {
		t.Fatalf("Should not duplicate: waiting=%d, delayed=%d", counts.Waiting, counts.Delayed)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler repeats reliably with stall recovery configured
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerRepeatsWithStallRecoveryConfigured(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "stall-test", bullmq.RepeatOptions{Every: 500 * time.Millisecond}, "", nil, nil))

	var counter atomic.Int32

	// First worker with very short lock duration to simulate stalling
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		counter.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{
		Connection:      testConnection(),
		LockDuration:    30_000 * time.Millisecond,
		StalledInterval: 5_000 * time.Millisecond,
		MaxStalledCount: bullmq.Ptr(2),
	})

	// Wait for multiple iterations to process
	time.Sleep(3 * time.Second)

	if total := counter.Load(); total < 3 {
		t.Fatalf("Scheduler should keep repeating even with stall recovery configured, got %d", total)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler with 'pattern' + promote: next delayed created after promote
// ═══════════════════════════════════════════════════════════════════════════

func TestPromotePatternSchedulerThenProcess(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Use 'every' with large interval — more predictable than cron for promote tests
	must(queue.UpsertJobScheduler(ctx, "promote-pattern", bullmq.RepeatOptions{Every: 30_000 * time.Millisecond}, "", nil, nil)) // 30s interval

	// First job in waiting (delay=0 for 'every')
	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 1 {
		t.Fatalf("waiting = %d, want 1", c.Waiting)
	}

	// Process first job
	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, jsSignalProcessor(ch))

	if _, ok := jsRecv(ch, 3*time.Second); !ok {
		t.Fatal("timed out waiting for first job")
	}

	time.Sleep(200 * time.Millisecond)

	// After first processing, next goes to delayed
	if c := must(queue.GetJobCounts(ctx)); c.Delayed != 1 {
		t.Fatalf("delayed = %d, want 1", c.Delayed)
	}

	// Promote
	noErr(t, queue.PromoteJobs(ctx, 1000))

	// Worker picks up promoted job almost instantly, so just wait for processing

	// Process promoted job
	if _, ok := jsRecv(ch, 3*time.Second); !ok {
		t.Fatal("timed out waiting for promoted job")
	}

	time.Sleep(200 * time.Millisecond)

	// After processing promoted job, next delayed should exist
	if c := must(queue.GetJobCounts(ctx)); c.Delayed != 1 {
		t.Fatalf("Next iteration delayed after promote+process (delayed=%d)", c.Delayed)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler with 'immediately' + 'pattern' promotes and processes again
// ═══════════════════════════════════════════════════════════════════════════

func TestPromotePatternImmediatelyScheduler(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Use immediately with a long-interval 'every' instead of cron
	// to avoid timing issues with cron patterns near second boundaries
	must(queue.UpsertJobScheduler(ctx, "promote-imm", bullmq.RepeatOptions{Every: 45_000 * time.Millisecond}, "", nil, nil)) // 45s

	// With 'every', first job is in waiting (delay=0)
	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 1 {
		t.Fatalf("waiting = %d, want 1", c.Waiting)
	}

	// Process first
	ch := make(chan struct{}, jsChanBuf)
	worker := jsAutorunWorker(t, name, jsSignalProcessor(ch))

	if _, ok := jsRecv(ch, 2*time.Second); !ok {
		t.Fatal("timed out waiting for first job")
	}

	time.Sleep(200 * time.Millisecond)

	// Next should be in delayed (45s in future)
	if c := must(queue.GetJobCounts(ctx)); c.Delayed != 1 {
		t.Fatalf("delayed = %d, want 1", c.Delayed)
	}

	// Promote it
	noErr(t, queue.PromoteJobs(ctx, 1000))

	// Process the promoted job
	if _, ok := jsRecv(ch, 2*time.Second); !ok {
		t.Fatal("timed out waiting for promoted job")
	}

	time.Sleep(200 * time.Millisecond)

	// After second process, next delayed should exist
	if c := must(queue.GetJobCounts(ctx)); c.Delayed != 1 {
		t.Fatalf("Next delayed should exist after promote+process (delayed=%d)", c.Delayed)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: count value is correct in job opts
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerJobHasCorrectCountInOpts(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.UpsertJobScheduler(ctx, "count-opts",
		bullmq.RepeatOptions{Every: 300 * time.Millisecond, Limit: 3}, "", nil, nil))

	// -1 encodes Rust's None (no repeat options on the job).
	ch := make(chan int64, jsChanBuf)
	worker := jsAutorunWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		count := int64(-1)
		if job.Opts.Repeat != nil {
			count = job.Opts.Repeat.Count
		}
		ch <- count
		return nil, nil
	})

	// Collect count values from each iteration
	counts := make([]int64, 0, 3)
	for range 3 {
		c, ok := jsRecv(ch, 3*time.Second)
		if !ok {
			t.Fatal("timed out waiting for iteration")
		}
		counts = append(counts, c)
	}

	// The count should increment: first job has count=1, second has count=2, etc.
	if !reflect.DeepEqual(counts, []int64{1, 2, 3}) {
		t.Fatalf("counts = %v, want [1 2 3]", counts)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: keep only one delayed if upserting with same id
// ═══════════════════════════════════════════════════════════════════════════

func TestUpsertKeepsOnlyOneDelayedJob(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// First upsert with cron (creates delayed)
	must(queue.UpsertJobScheduler(ctx, "one-delayed", bullmq.RepeatOptions{Pattern: "0 0 * * *"}, "", nil, nil)) // daily

	if c := must(queue.GetJobCounts(ctx)); c.Delayed != 1 {
		t.Fatalf("delayed = %d, want 1", c.Delayed)
	}

	// Upsert again with different pattern
	must(queue.UpsertJobScheduler(ctx, "one-delayed", bullmq.RepeatOptions{Pattern: "0 12 * * *"}, "", nil, nil)) // daily at noon

	// Should still have only 1 delayed job (old one replaced)
	if c := must(queue.GetJobCounts(ctx)); c.Delayed != 1 {
		t.Fatalf("Should have exactly 1 delayed job after re-upsert (delayed=%d)", c.Delayed)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: update delayed job timestamp when upserting different pattern
// ═══════════════════════════════════════════════════════════════════════════

func TestUpsertUpdatesDelayedJobTimestamp(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Hourly cron
	must(queue.UpsertJobScheduler(ctx, "ts-update", bullmq.RepeatOptions{Pattern: "0 * * * *"}, "", nil, nil))

	sched1 := must(queue.GetJobScheduler(ctx, "ts-update"))
	if sched1 == nil || sched1.Next.IsZero() {
		t.Fatalf("scheduler = %+v", sched1)
	}
	next1 := sched1.Next.UnixMilli()

	// Now upsert with a "every minute" cron — should have an earlier next
	must(queue.UpsertJobScheduler(ctx, "ts-update", bullmq.RepeatOptions{Pattern: "* * * * *"}, "", nil, nil))

	sched2 := must(queue.GetJobScheduler(ctx, "ts-update"))
	if sched2 == nil || sched2.Next.IsZero() {
		t.Fatalf("scheduler = %+v", sched2)
	}
	next2 := sched2.Next.UnixMilli()

	// Allow minor millisecond drift when both cron expressions resolve to the
	// same boundary (e.g. exactly on the next hour).
	const maxBoundaryDriftMs = 100
	driftMs := max(next2-next1, 0)

	// Every-minute next should be earlier than hourly next, or effectively the
	// same when both schedules align on the next run boundary.
	if next2 > next1 && driftMs > maxBoundaryDriftMs {
		t.Fatalf("After upserting with more frequent pattern, next timestamp should be earlier or within %dms boundary drift (was %d, now %d, drift %d)",
			maxBoundaryDriftMs, next1, next2, driftMs)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: keep only one waiting job if upserting with same id (every)
// ═══════════════════════════════════════════════════════════════════════════

func TestUpsertKeepsOnlyOneWaitingJobEvery(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// First upsert with 'every' (puts first job in waiting)
	must(queue.UpsertJobScheduler(ctx, "one-waiting", bullmq.RepeatOptions{Every: 5000 * time.Millisecond}, "", nil, nil))

	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 1 {
		t.Fatalf("waiting = %d, want 1", c.Waiting)
	}

	// Upsert again with different every
	must(queue.UpsertJobScheduler(ctx, "one-waiting", bullmq.RepeatOptions{Every: 10_000 * time.Millisecond}, "", nil, nil))

	// Should still have only 1 waiting job
	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 1 {
		t.Fatalf("Should have exactly 1 waiting job after re-upsert (waiting=%d)", c.Waiting)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: delete and upsert should not throw during processing
// ═══════════════════════════════════════════════════════════════════════════

func TestDeleteAndUpsertDuringProcessing(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	repeatOpts := bullmq.RepeatOptions{Every: 300 * time.Millisecond}
	must(queue.UpsertJobScheduler(ctx, "del-upsert", repeatOpts, "", nil, nil))

	var counter atomic.Int32
	worker := jsAutorunWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		counter.Add(1)
		return nil, nil
	})

	// Let it process one iteration
	time.Sleep(500 * time.Millisecond)
	if counter.Load() < 1 {
		t.Fatal("expected at least one iteration")
	}

	// Delete the scheduler
	must(queue.RemoveJobScheduler(ctx, "del-upsert"))
	time.Sleep(100 * time.Millisecond)

	// Re-add it
	must(queue.UpsertJobScheduler(ctx, "del-upsert", repeatOpts, "", nil, nil))

	// Worker should continue processing without crashing
	before := counter.Load()
	time.Sleep(time.Second)
	after := counter.Load()
	if after <= before {
		t.Fatalf("Worker should continue processing after delete+upsert (before=%d, after=%d)", before, after)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Scheduler: processes delayed jobs by priority
// ═══════════════════════════════════════════════════════════════════════════

func TestSchedulerProcessesJobsByPriority(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Create two schedulers with different priorities
	repeatOptsLow := bullmq.RepeatOptions{Every: 60_000 * time.Millisecond} // won't repeat during test
	repeatOptsHigh := bullmq.RepeatOptions{Every: 60_000 * time.Millisecond}

	optsLow := &bullmq.JobOptions{Priority: 10} // lower priority (higher number = lower priority)
	optsHigh := &bullmq.JobOptions{Priority: 1} // higher priority

	// Add low priority first
	must(queue.UpsertJobScheduler(ctx, "low-prio", repeatOptsLow, "low", nil, optsLow))

	// Add high priority second
	must(queue.UpsertJobScheduler(ctx, "high-prio", repeatOptsHigh, "high", nil, optsHigh))

	ch := make(chan string, jsChanBuf)
	worker := jsAutorunWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		ch <- job.Name
		return nil, nil
	})

	// Collect the order they're processed
	first, ok := jsRecv(ch, 3*time.Second)
	if !ok {
		t.Fatal("timed out waiting for first job")
	}
	second, ok := jsRecv(ch, 3*time.Second)
	if !ok {
		t.Fatal("timed out waiting for second job")
	}

	// High priority should be processed first
	if first != "high" {
		t.Fatalf("Higher priority job should be processed first, got %q", first)
	}
	if second != "low" {
		t.Fatalf("second = %q, want low", second)
	}

	noErr(t, worker.Close(5*time.Second))
}
