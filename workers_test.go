package bullmq_test

// Worker discovery tests — Queue.GetWorkers / GetWorkersCount
// (port of bullmq_rust/tests/workers_test.rs).
//
// Mirrors Node.js `tests/getters.test.ts` `.getWorkers` cases. Requires a
// running Redis instance at redis://127.0.0.1:6379.

import (
	"bullmq"
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func mxNoopProcessor() bullmq.Processor {
	return func(ctx context.Context, job *bullmq.Job) (any, error) { return nil, nil }
}

func TestGetWorkersForQueueOnly(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	if n := must(queue.GetWorkersCount(ctx)); n != 0 {
		t.Fatalf("workers count = %d, want 0", n)
	}

	worker := newTestWorker(t, name, mxNoopProcessor(), &bullmq.WorkerOptions{ManualStart: true})

	if workers := must(queue.GetWorkers(ctx)); len(workers) != 1 {
		t.Fatalf("workers = %v, want 1", workers)
	}

	worker2 := newTestWorker(t, name, mxNoopProcessor(), &bullmq.WorkerOptions{ManualStart: true})

	nextWorkers := must(queue.GetWorkers(ctx))
	if len(nextWorkers) != 2 {
		t.Fatalf("workers = %v, want 2", nextWorkers)
	}
	if n := must(queue.GetWorkersCount(ctx)); n != 2 {
		t.Fatalf("workers count = %d, want 2", n)
	}

	// Every returned client's name is normalized to the queue name.
	for _, w := range nextWorkers {
		if got, ok := w["name"]; !ok || got != name {
			t.Fatalf("name = %q (present %v), want %q", got, ok, name)
		}
		if _, ok := w["rawname"]; !ok {
			t.Fatalf("rawname missing: %v", w)
		}
	}

	noErr(t, worker.Close(5*time.Second))
	noErr(t, worker2.Close(5*time.Second))
}

func TestGetWorkersIncludingTheirNames(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	worker := newTestWorker(t, name, mxNoopProcessor(), &bullmq.WorkerOptions{ManualStart: true, Name: "worker1"})

	if workers := must(queue.GetWorkers(ctx)); len(workers) != 1 {
		t.Fatalf("workers = %v, want 1", workers)
	}
	if n := must(queue.GetWorkersCount(ctx)); n != 1 {
		t.Fatalf("workers count = %d, want 1", n)
	}

	worker2 := newTestWorker(t, name, mxNoopProcessor(), &bullmq.WorkerOptions{ManualStart: true, Name: "worker2"})

	nextWorkers := must(queue.GetWorkers(ctx))
	if len(nextWorkers) != 2 {
		t.Fatalf("workers = %v, want 2", nextWorkers)
	}

	names := make([]string, 0, len(nextWorkers))
	for _, w := range nextWorkers {
		raw := w["rawname"]
		names = append(names, raw[strings.LastIndex(raw, ":")+1:])
	}
	sort.Strings(names)
	if !reflect.DeepEqual(names, []string{"worker1", "worker2"}) {
		t.Fatalf("names = %v, want [worker1 worker2]", names)
	}

	noErr(t, worker.Close(5*time.Second))
	noErr(t, worker2.Close(5*time.Second))
}

func TestGetWorkersIsolatedBetweenQueues(t *testing.T) {
	ctx := context.Background()
	nameA := testQueueName()
	nameB := testQueueName()
	queueA := newTestQueue(t, nameA, nil)
	queueB := newTestQueue(t, nameB, nil)

	// One worker on each queue.
	workerA := newTestWorker(t, nameA, mxNoopProcessor(), &bullmq.WorkerOptions{ManualStart: true})
	workerB := newTestWorker(t, nameB, mxNoopProcessor(), &bullmq.WorkerOptions{ManualStart: true})

	// Each queue must only see its own worker.
	if n := must(queueA.GetWorkersCount(ctx)); n != 1 {
		t.Fatalf("queue A workers = %d, want 1", n)
	}
	if n := must(queueB.GetWorkersCount(ctx)); n != 1 {
		t.Fatalf("queue B workers = %d, want 1", n)
	}

	aRaw, ok := must(queueA.GetWorkers(ctx))[0]["rawname"]
	if !ok {
		t.Fatal("queue A rawname missing")
	}
	bRaw, ok := must(queueB.GetWorkers(ctx))[0]["rawname"]
	if !ok {
		t.Fatal("queue B rawname missing")
	}
	if aRaw == bRaw {
		t.Fatalf("rawnames should differ: %q", aRaw)
	}

	noErr(t, workerA.Close(5*time.Second))
	noErr(t, workerB.Close(5*time.Second))
}
