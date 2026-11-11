package bullmq_test

// Metrics (time-series) tests (port of bullmq_rust/tests/metrics_test.rs).

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/imdat99/bullmq"
)

// A worker configured with the metrics option records completed-job counts.
func TestGatherMetricsForCompletedJobs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const numJobs = 5
	for i := 0; i < numJobs; i++ {
		must(queue.Add(ctx, "test", map[string]any{"index": i}, nil))
	}

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	}, &bullmq.WorkerOptions{
		DrainDelay: time.Second,
		Metrics:    &bullmq.MetricsOptions{MaxDataPoints: 2 * 60}, // two hours of minute points
	})

	// Wait for all jobs to complete.
	eventually(t, 10*time.Second, func() bool { return must(queue.GetCompletedCount(ctx)) >= numJobs },
		"jobs did not complete")

	metrics := must(queue.GetMetrics(ctx, "completed", 0, -1))
	// The metrics meta `count` is incremented once per completed job.
	if metrics.Meta.Count != numJobs {
		t.Fatalf("meta.count = %d, want %d", metrics.Meta.Count, numJobs)
	}
	// `count` equals the number of recorded data points (== data length).
	if int(metrics.Count) != len(metrics.Data) {
		t.Fatalf("count = %d, len(data) = %d", metrics.Count, len(metrics.Data))
	}

	noErr(t, worker.Close(5*time.Second))
}

// A worker WITHOUT the metrics option records nothing.
func TestNoMetricsWhenDisabled(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	for i := 0; i < 3; i++ {
		must(queue.Add(ctx, "test", map[string]any{"index": i}, nil))
	}

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	}, &bullmq.WorkerOptions{
		DrainDelay: time.Second,
		// No metrics option.
	})

	eventually(t, 10*time.Second, func() bool { return must(queue.GetCompletedCount(ctx)) >= 3 },
		"jobs did not complete")

	metrics := must(queue.GetMetrics(ctx, "completed", 0, -1))
	if metrics.Meta.Count != 0 {
		t.Fatalf("meta.count = %d, want 0", metrics.Meta.Count)
	}
	if metrics.Count != 0 {
		t.Fatalf("count = %d, want 0", metrics.Count)
	}
	if len(metrics.Data) != 0 {
		t.Fatalf("data = %v, want empty", metrics.Data)
	}

	noErr(t, worker.Close(5*time.Second))
}

// Metrics are gathered for failed jobs too.
func TestGatherMetricsForFailedJobs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	const numJobs = 4
	for i := 0; i < numJobs; i++ {
		must(queue.Add(ctx, "test", map[string]any{"index": i}, &bullmq.JobOptions{Attempts: 1}))
	}

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, errors.New("boom")
	}, &bullmq.WorkerOptions{
		DrainDelay: time.Second,
		Metrics:    &bullmq.MetricsOptions{},
	})

	eventually(t, 10*time.Second, func() bool { return must(queue.GetFailedCount(ctx)) >= numJobs },
		"jobs did not fail")

	metrics := must(queue.GetMetrics(ctx, "failed", 0, -1))
	if metrics.Meta.Count != numJobs {
		t.Fatalf("meta.count = %d, want %d", metrics.Meta.Count, numJobs)
	}

	noErr(t, worker.Close(5*time.Second))
}
