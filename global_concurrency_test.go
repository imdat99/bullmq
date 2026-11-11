package bullmq_test

// Global concurrency tests (port of bullmq_rust/tests/global_concurrency_test.rs).

import (
	"bullmq"
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// mxConcurrencyProbe records the maximum number of jobs observed running at
// the same time.
type mxConcurrencyProbe struct {
	current, max atomic.Int32
	done         chan struct{}
}

func mxNewConcurrencyProbe() *mxConcurrencyProbe {
	return &mxConcurrencyProbe{done: make(chan struct{}, 10)}
}

// processor holds each job for hold while tracking concurrency.
func (p *mxConcurrencyProbe) processor(hold time.Duration) bullmq.Processor {
	return func(ctx context.Context, job *bullmq.Job) (any, error) {
		activeNow := p.current.Add(1)
		// Update max if we see more concurrent than before
		for {
			m := p.max.Load()
			if activeNow <= m || p.max.CompareAndSwap(m, activeNow) {
				break
			}
		}
		// Hold the job for a bit to allow overlap detection
		time.Sleep(hold)
		p.current.Add(-1)
		p.done <- struct{}{}
		return nil, nil
	}
}

func (p *mxConcurrencyProbe) waitJobs(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-p.done:
		case <-time.After(timeout):
			t.Fatalf("Timed out waiting for job %d", i+1)
		}
	}
}

func TestGlobalConcurrencyLimitsActiveJobs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Set global concurrency to 1
	noErr(t, queue.SetGlobalConcurrency(ctx, 1))

	// Add 3 jobs
	for i := 0; i < 3; i++ {
		must(queue.Add(ctx, "test", map[string]any{"n": i}, nil))
	}

	probe := mxNewConcurrencyProbe()
	// Worker with local concurrency of 5 (higher than global)
	worker := newTestWorker(t, name, probe.processor(100*time.Millisecond), &bullmq.WorkerOptions{Concurrency: 5})

	// Wait for all 3 jobs to complete
	probe.waitJobs(t, 3, 5*time.Second)

	if observed := probe.max.Load(); observed != 1 {
		t.Fatalf("Global concurrency should limit to 1 active job at a time, but saw %d", observed)
	}

	noErr(t, worker.Close(5*time.Second))
	noErr(t, queue.RemoveGlobalConcurrency(ctx))
}

func TestGlobalConcurrencyAllowsMultipleWhenSetHigher(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Set global concurrency to 3
	noErr(t, queue.SetGlobalConcurrency(ctx, 3))

	// Add 5 jobs
	for i := 0; i < 5; i++ {
		must(queue.Add(ctx, "test", map[string]any{"n": i}, nil))
	}

	probe := mxNewConcurrencyProbe()
	// Worker with concurrency 10 (higher than global limit)
	worker := newTestWorker(t, name, probe.processor(200*time.Millisecond), &bullmq.WorkerOptions{Concurrency: 10})

	// Wait for all 5 jobs
	probe.waitJobs(t, 5, 10*time.Second)

	observed := probe.max.Load()
	if observed > 3 {
		t.Fatalf("Global concurrency should limit to at most 3, but saw %d", observed)
	}
	// Should actually hit the limit (at least 2 concurrent)
	if observed < 2 {
		t.Fatalf("Expected at least 2 concurrent jobs given concurrency=3 and 5 jobs, but saw %d", observed)
	}

	noErr(t, worker.Close(5*time.Second))
	noErr(t, queue.RemoveGlobalConcurrency(ctx))
}

func TestRemoveGlobalConcurrencyRestoresUnlimited(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Set then remove global concurrency
	noErr(t, queue.SetGlobalConcurrency(ctx, 1))
	noErr(t, queue.RemoveGlobalConcurrency(ctx))

	// Add 3 jobs
	for i := 0; i < 3; i++ {
		must(queue.Add(ctx, "test", map[string]any{"n": i}, nil))
	}

	probe := mxNewConcurrencyProbe()
	// Worker with concurrency 5
	worker := newTestWorker(t, name, probe.processor(200*time.Millisecond), &bullmq.WorkerOptions{Concurrency: 5})

	// Wait for all 3 jobs
	probe.waitJobs(t, 3, 5*time.Second)

	// With no global concurrency, all 3 should run concurrently (worker concurrency=5)
	if observed := probe.max.Load(); observed < 2 {
		t.Fatalf("Without global concurrency, expected multiple concurrent jobs, but saw %d", observed)
	}

	noErr(t, worker.Close(5*time.Second))
}
