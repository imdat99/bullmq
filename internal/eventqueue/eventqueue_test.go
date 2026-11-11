package eventqueue

import (
	"context"
	"testing"
)

func TestEventQueueOrderGrowthAndOverflow(t *testing.T) {
	q := New[int](40)

	for i := range 30 { // forces two grows (16 -> 32 -> 40)
		q.Push(i)
	}

	for i := range 10 {
		if v, ok := q.TryNext(); !ok || v != i {
			t.Fatalf("pop %d: got %d, %v", i, v, ok)
		}
	}

	for i := 30; i < 70; i++ { // wraps and overflows: oldest are dropped
		q.Push(i)
	}

	for want := 30; want < 70; want++ {
		v, ok := q.Next(context.Background())
		if !ok || v != want {
			t.Fatalf("got %d, %v; want %d", v, ok, want)
		}
	}

	if _, ok := q.TryNext(); ok {
		t.Fatal("queue should be empty")
	}

	q.Push(1)
	q.Close()
	q.Push(2) // ignored after close

	if v, ok := q.Next(context.Background()); !ok || v != 1 {
		t.Fatalf("pending event lost after close: %d, %v", v, ok)
	}

	if _, ok := q.Next(context.Background()); ok {
		t.Fatal("closed and drained queue must report false")
	}
}

func TestEventQueueSteadyStateDoesNotAllocate(t *testing.T) {
	q := New[event](64)
	for range 64 {
		q.Push(event{kind: 1})
	}

	allocs := testing.AllocsPerRun(1000, func() {
		q.Push(event{kind: 2, id: "1"})
		_, _ = q.TryNext()
	})
	if allocs != 0 {
		t.Fatalf("push/pop allocates %.1f times", allocs)
	}
}

// event mirrors the shape of the worker events (a small struct with a string).
type event struct {
	kind int
	id   string
}
