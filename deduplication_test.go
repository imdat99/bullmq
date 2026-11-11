package bullmq_test

// Deduplication tests (port of bullmq_rust/tests/deduplication_test.rs).

import (
	"bullmq"
	"context"
	"testing"
	"time"
)

// mxDedup returns job options deduplicating on id.
func mxDedup(id string) *bullmq.JobOptions {
	return &bullmq.JobOptions{Deduplication: &bullmq.DeduplicationOptions{ID: id}}
}

// ═══════════════════════════════════════════════════════════════════════════
// Basic deduplication
// ═══════════════════════════════════════════════════════════════════════════

func TestDedupReschedulePreservesPayloadAndUpdatesDelay(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)
	original := must(queue.Add(ctx, "original", map[string]any{"color": "white"}, &bullmq.JobOptions{
		Delay:         60000 * time.Millisecond,
		Deduplication: &bullmq.DeduplicationOptions{ID: "reschedule"},
	}))
	for _, delay := range []time.Duration{120000 * time.Millisecond, 30000 * time.Millisecond} {
		duplicate := must(queue.Add(ctx, "incoming", map[string]any{"color": "blue"}, &bullmq.JobOptions{
			Delay:         delay,
			Deduplication: &bullmq.DeduplicationOptions{ID: "reschedule", Reschedule: true},
		}))
		if duplicate.ID != original.ID {
			t.Fatalf("duplicate id %s != original id %s", duplicate.ID, original.ID)
		}
		stored := must(queue.GetJob(ctx, original.ID))
		if stored == nil {
			t.Fatal("original job missing")
		}
		if stored.Name != "original" {
			t.Fatalf("name = %q, want original", stored.Name)
		}
		var data map[string]any
		noErr(t, stored.DecodeData(&data))
		if len(data) != 1 || data["color"] != "white" {
			t.Fatalf("data = %v, want {color: white}", data)
		}
		if stored.Delay != delay {
			t.Fatalf("delay = %v, want %v", stored.Delay, delay)
		}
	}
}

func TestDedupPreventsDuplicateJobs(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	// Add two jobs with the same deduplication ID
	id1 := must(queue.Add(ctx, "job1", map[string]any{"first": true}, mxDedup("dedup-1"))).ID
	id2 := must(queue.Add(ctx, "job2", map[string]any{"second": true}, mxDedup("dedup-1"))).ID

	// Both should return the same job ID (second was deduplicated)
	if id1 != id2 {
		t.Fatalf("ids differ: %s vs %s", id1, id2)
	}

	// Only 1 job should be in the queue
	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 1 {
		t.Fatalf("waiting = %d, want 1", c.Waiting)
	}
}

func TestDedupDifferentIdsCreateSeparateJobs(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	id1 := must(queue.Add(ctx, "job1", map[string]any{"a": 1}, mxDedup("dedup-a"))).ID
	id2 := must(queue.Add(ctx, "job2", map[string]any{"b": 2}, mxDedup("dedup-b"))).ID

	// Different dedup IDs → different jobs
	if id1 == id2 {
		t.Fatalf("ids should differ: %s", id1)
	}
	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 2 {
		t.Fatalf("waiting = %d, want 2", c.Waiting)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Deduplication with TTL
// ═══════════════════════════════════════════════════════════════════════════

func TestDedupWithTtlAllowsAfterExpiry(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)
	ttlOpts := func() *bullmq.JobOptions {
		return &bullmq.JobOptions{Deduplication: &bullmq.DeduplicationOptions{ID: "ttl-dedup", TTL: 500 * time.Millisecond}}
	}

	// First job with 500ms TTL
	must(queue.Add(ctx, "job1", map[string]any{"first": true}, ttlOpts()))
	// Immediately add a duplicate → should be deduplicated
	must(queue.Add(ctx, "job2", map[string]any{"second": true}, ttlOpts()))

	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 1 {
		t.Fatalf("Duplicate should be prevented: waiting = %d", c.Waiting)
	}

	// Wait for TTL to expire
	time.Sleep(600 * time.Millisecond)

	// Now add again → should create a new job
	must(queue.Add(ctx, "job3", map[string]any{"third": true}, ttlOpts()))

	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 2 {
		t.Fatalf("After TTL expiry, new job should be created: waiting = %d", c.Waiting)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Deduplication with replace
// ═══════════════════════════════════════════════════════════════════════════

func TestDedupReplaceUpdatesDelayedJob(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	// First job delayed with dedup
	id1 := must(queue.Add(ctx, "job1", map[string]any{"v": 1}, &bullmq.JobOptions{
		Delay:         60_000 * time.Millisecond,
		Deduplication: &bullmq.DeduplicationOptions{ID: "replace-dedup"},
	})).ID

	// Replace with a new delayed job
	id2 := must(queue.Add(ctx, "job2", map[string]any{"v": 2}, &bullmq.JobOptions{
		Delay:         60_000 * time.Millisecond,
		Deduplication: &bullmq.DeduplicationOptions{ID: "replace-dedup", Replace: true},
	})).ID

	// New job should have a different ID
	if id1 == id2 {
		t.Fatalf("replacement should have a new id, got %s", id1)
	}

	// Only 1 delayed job should exist (the replacement)
	if c := must(queue.GetJobCounts(ctx)); c.Delayed != 1 {
		t.Fatalf("delayed = %d, want 1", c.Delayed)
	}

	// Verify the existing job has the updated data
	job := must(queue.GetJob(ctx, id2))
	if job == nil {
		t.Fatal("replacement job missing")
	}
	var data struct {
		V int `json:"v"`
	}
	noErr(t, job.DecodeData(&data))
	if data.V != 2 {
		t.Fatalf("v = %d, want 2", data.V)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Remove deduplication key
// ═══════════════════════════════════════════════════════════════════════════

func TestRemoveDeduplicationKey(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	// Add a job with dedup (no TTL → key persists indefinitely)
	jobID := must(queue.Add(ctx, "job1", map[string]any{"v": 1}, mxDedup("persistent-dedup"))).ID

	// Verify dedup is active — adding same dedup ID returns same job
	id2 := must(queue.Add(ctx, "job2", map[string]any{"v": 2}, mxDedup("persistent-dedup"))).ID
	if jobID != id2 {
		t.Fatalf("expected deduplicated id %s, got %s", jobID, id2)
	}

	// Remove the dedup key
	if !must(queue.RemoveDeduplicationKey(ctx, "persistent-dedup", jobID)) {
		t.Fatal("RemoveDeduplicationKey should return true")
	}

	// Now a new job should be created
	id3 := must(queue.Add(ctx, "job3", map[string]any{"v": 3}, mxDedup("persistent-dedup"))).ID
	if jobID == id3 {
		t.Fatalf("expected a new job id, got %s", id3)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Deduplication with worker processing
// ═══════════════════════════════════════════════════════════════════════════

func TestDedupKeyRemovedAfterJobCompletes(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add job with dedup (no TTL → cleaned up on completion by Lua moveToFinished)
	first := must(queue.Add(ctx, "job1", map[string]any{"v": 1}, mxDedup("complete-dedup")))

	// Process it
	ch := make(chan struct{}, 5)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		select {
		case ch <- struct{}{}:
		default:
		}
		return nil, nil
	}, nil)

	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for job processing")
	}
	time.Sleep(100 * time.Millisecond)

	// After completion, dedup key should be removed → new job can be added
	job2 := must(queue.Add(ctx, "job2", map[string]any{"v": 2}, mxDedup("complete-dedup")))
	if job2.ID == first.ID {
		t.Fatalf("job2 was deduplicated onto %s", first.ID)
	}

	// Should be a new job (not deduplicated)
	c := must(queue.GetJobCounts(ctx))
	// Either the new job is waiting or already picked up by the worker (the
	// Go worker may already have completed it, so the first job's completion
	// is subtracted from the completed count).
	if c.Waiting+c.Active+(c.Completed-1) < 1 {
		t.Fatalf("New job should have been created after dedup key cleared: %+v", c)
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Deprecated debounce aliases
// ═══════════════════════════════════════════════════════════════════════════

func TestGetDebounceJobIdAlias(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	if id, ok, err := queue.GetDebounceJobID(ctx, "missing"); err != nil || ok {
		t.Fatalf("GetDebounceJobID(missing) = %q, %v, %v; want none", id, ok, err)
	}

	jobID := must(queue.Add(ctx, "job1", map[string]any{"v": 1}, mxDedup("debounce-1"))).ID

	// The deprecated alias returns the same value as GetDeduplicationJobID.
	id, ok, err := queue.GetDebounceJobID(ctx, "debounce-1")
	noErr(t, err)
	if !ok || id != jobID {
		t.Fatalf("GetDebounceJobID = %q, %v; want %q", id, ok, jobID)
	}
	id, ok, err = queue.GetDeduplicationJobID(ctx, "debounce-1")
	noErr(t, err)
	if !ok || id != jobID {
		t.Fatalf("GetDeduplicationJobID = %q, %v; want %q", id, ok, jobID)
	}
}

func TestRemoveDebounceKeyAlias(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	must(queue.Add(ctx, "job1", map[string]any{"v": 1}, mxDedup("debounce-rm")))

	// Plain DEL — returns 1 when the key existed, 0 afterwards.
	if n := must(queue.RemoveDebounceKey(ctx, "debounce-rm")); n != 1 {
		t.Fatalf("first RemoveDebounceKey = %d, want 1", n)
	}
	if n := must(queue.RemoveDebounceKey(ctx, "debounce-rm")); n != 0 {
		t.Fatalf("second RemoveDebounceKey = %d, want 0", n)
	}
	if id, ok, err := queue.GetDebounceJobID(ctx, "debounce-rm"); err != nil || ok {
		t.Fatalf("GetDebounceJobID = %q, %v, %v; want none", id, ok, err)
	}
}
