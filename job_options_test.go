package bullmq_test

// Job option tests — SizeLimit and KeepJobs.Limit
// (port of bullmq_rust/tests/job_options_test.rs).
//
// Mirrors Node.js `tests/job.test.ts` size-limit cases. Requires a running
// Redis instance at redis://127.0.0.1:6379.

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/bullmq"
)

// mxExpectInvalidConfig asserts err is an ErrInvalidConfig containing msg.
func mxExpectInvalidConfig(t *testing.T, err error, msg string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", msg)
	}
	if !errors.Is(err, bullmq.ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
	if !strings.Contains(err.Error(), msg) {
		t.Fatalf("error %q does not contain %q", err.Error(), msg)
	}
}

func TestSizeLimitAllowsSmallPayload(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	// {"foo":"bar"} is 13 bytes; limit 20 → accepted.
	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{SizeLimit: 20}))
	if job.ID == "" {
		t.Fatal("job id should not be empty")
	}
}

func TestSizeLimitRejectsLargePayload(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	// {"foo":"bar"} is 13 bytes; limit 12 → rejected.
	_, err := queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{SizeLimit: 12})
	mxExpectInvalidConfig(t, err, "The size of job test exceeds the limit 12 bytes")
	if got, want := err.Error(), "invalid configuration: The size of job test exceeds the limit 12 bytes"; got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
}

func TestSizeLimitCountsUtf8Bytes(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	// {"foo":"βÅ®"} is 16 UTF-8 bytes; limit 15 → rejected.
	_, err := queue.Add(ctx, "test", map[string]any{"foo": "βÅ®"}, &bullmq.JobOptions{SizeLimit: 15})
	mxExpectInvalidConfig(t, err, "The size of job test exceeds the limit 15 bytes")
}

func TestSizeLimitRejectsEvenWithCustomJobId(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	_, err := queue.Add(ctx, "test", map[string]any{"foo": "bar"}, &bullmq.JobOptions{SizeLimit: 12, JobID: "customJobId"})
	mxExpectInvalidConfig(t, err, "The size of job test exceeds the limit 12 bytes")

	// The rejected job must not have been added.
	if n := must(queue.GetWaitingCount(ctx)); n != 0 {
		t.Fatalf("waiting = %d, want 0", n)
	}
}

func TestSizeLimitEnforcedInAddBulk(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	// One job fits (limit 20), one is too large (limit 12) → AddBulk fails.
	_, err := queue.AddBulk(ctx, []bullmq.BulkJob{
		{Name: "small", Data: map[string]any{"foo": "bar"}, Opts: &bullmq.JobOptions{SizeLimit: 20}},
		{Name: "big", Data: map[string]any{"foo": "bar"}, Opts: &bullmq.JobOptions{SizeLimit: 12}},
	})
	mxExpectInvalidConfig(t, err, "The size of job big exceeds the limit 12 bytes")
	if n := must(queue.GetWaitingCount(ctx)); n != 0 {
		t.Fatalf("waiting = %d, want 0", n)
	}
}

func TestKeepJobsLimitIsAcceptedAndTrims(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const numJobs = 5
	for i := 0; i < numJobs; i++ {
		must(queue.Add(ctx, "test", map[string]any{"i": i}, nil))
	}

	var processed atomic.Int32
	// Keep at most 1 completed job, removing up to 10 excess jobs per finish.
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		processed.Add(1)
		return nil, nil
	}, &bullmq.WorkerOptions{
		Concurrency: 1,
		RemoveOnComplete: bullmq.KeepJobsPolicy(bullmq.KeepJobs{
			Count: bullmq.Ptr[int64](1),
			Limit: bullmq.Ptr[int64](10),
		}),
	})

	// Wait until all jobs have been processed.
	eventually(t, 15*time.Second, func() bool { return processed.Load() >= numJobs }, "timed out processing jobs")

	// Give eviction a moment to settle, then assert the completed set was
	// trimmed toward the keep-count (best-effort eviction, so allow a little lag).
	time.Sleep(200 * time.Millisecond)
	if completed := must(queue.GetCompletedCount(ctx)); completed > 2 {
		t.Fatalf("expected completed set trimmed to ~1, got %d", completed)
	}

	noErr(t, worker.Close(5*time.Second))
}
