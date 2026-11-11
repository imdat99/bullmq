package bullmq_test

// Delayed job processing tests (port of bullmq_rust/tests/delay_test.rs).

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/imdat99/bullmq"
)

func TestDelayedJobProcessedAfterDelay(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	delay := 500 * time.Millisecond
	start := time.Now()
	must(queue.Add(ctx, "delayed-test", map[string]any{"delayed": "foobar"}, &bullmq.JobOptions{Delay: delay}))

	counts := must(queue.GetJobCounts(ctx))
	if counts.Delayed != 1 {
		t.Fatalf("delayed = %d, want 1", counts.Delayed)
	}
	if counts.Waiting != 0 {
		t.Fatalf("waiting = %d, want 0", counts.Waiting)
	}

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	waitForEvent(t, worker, 5*time.Second, eventType(bullmq.EventCompleted))

	elapsed := time.Since(start)
	if elapsed < delay {
		t.Fatalf("Job processed too early: %v < %v", elapsed, delay)
	}
	if elapsed >= 3*delay {
		t.Fatalf("Job processed too late: %v", elapsed)
	}
	noErr(t, worker.Close(5*time.Second))
}

func TestDelayedJobCorrectOrder(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// Add jobs with different delays: 300ms, 100ms, 200ms
	// Should be processed in order: 100, 200, 300
	for _, c := range []struct {
		idx   int
		delay time.Duration
	}{{1, 300 * time.Millisecond}, {2, 100 * time.Millisecond}, {3, 200 * time.Millisecond}} {
		must(queue.Add(ctx, "test", map[string]any{"idx": c.idx}, &bullmq.JobOptions{Delay: c.delay}))
	}

	ch := make(chan int, 10)
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		var d struct {
			Idx int `json:"idx"`
		}
		if err := job.DecodeData(&d); err != nil {
			return nil, err
		}
		ch <- d.Idx
		return nil, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	var order []int
	timeout := time.After(5 * time.Second)
	for len(order) < 3 {
		select {
		case idx := <-ch:
			order = append(order, idx)
		case <-timeout:
			t.Fatalf("timeout waiting for all delayed jobs, got %v", order)
		}
	}

	// Should be processed in delay order: 100ms first (idx=2), 200ms (idx=3), 300ms (idx=1)
	if !reflect.DeepEqual(order, []int{2, 3, 1}) {
		t.Fatalf("order = %v, want [2 3 1]", order)
	}
	noErr(t, worker.Close(5*time.Second))
}
