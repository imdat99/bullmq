package bullmq_test

import (
	"bullmq"
	"context"
	"errors"
	"testing"
	"time"
)

func TestSmokeProcessAndRetry(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	q := newTestQueue(t, name, nil)
	j := must(q.Add(ctx, "a", map[string]any{"x": 1}, nil))
	f := must(q.Add(ctx, "b", nil, &bullmq.JobOptions{Attempts: 2, Backoff: bullmq.FixedBackoff(50 * time.Millisecond)}))
	d := must(q.Add(ctx, "c", nil, &bullmq.JobOptions{Delay: 300 * time.Millisecond}))
	w := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if job.Name == "b" {
			return nil, errors.New("boom")
		}
		return "ok-" + job.Name, nil
	}, &bullmq.WorkerOptions{Concurrency: 3})
	_ = w
	eventually(t, 5*time.Second, func() bool {
		c := must(q.GetJobCounts(ctx))
		return c.Completed == 2 && c.Failed == 1
	}, "2 completed 1 failed")
	got := must(q.GetJob(ctx, j.ID))
	if got.ReturnValue != `"ok-a"` {
		t.Fatalf("returnvalue %q", got.ReturnValue)
	}
	fj := must(q.GetJob(ctx, f.ID))
	if fj.AttemptsMade != 2 || fj.FailedReason != "boom" {
		t.Fatalf("failed job %+v", fj)
	}
	dj := must(q.GetJobState(ctx, d.ID))
	if dj != bullmq.StateCompleted {
		t.Fatalf("delayed state %s", dj)
	}
}
