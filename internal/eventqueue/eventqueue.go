// Package eventqueue provides a bounded, non-blocking FIFO for delivering
// events to consumers that read at their own pace.
package eventqueue

import (
	"context"
	"sync"
)

// Queue is a non-blocking FIFO used to deliver events to consumers that
// read at their own pace. Producers never block; when more than limit events
// are pending, the oldest are dropped so an unread queue cannot grow without
// bound.
type Queue[T any] struct {
	mu sync.Mutex
	// ring holds the pending events from head, n of them. It grows by
	// doubling up to limit and then overwrites the oldest event, so a steady
	// event flow (one or more events per job) allocates nothing, read or not.
	ring   []T
	head   int
	n      int
	limit  int
	closed bool
	notify chan struct{} // buffered(1): "something changed"
}

// New returns an empty queue holding at most limit pending events (0 = unbounded).
func New[T any](limit int) *Queue[T] {
	return &Queue[T]{limit: limit, notify: make(chan struct{}, 1)}
}

// Push appends v without blocking, dropping the oldest event when full.
func (q *Queue[T]) Push(v T) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}

	if q.n == len(q.ring) && (q.limit <= 0 || len(q.ring) < q.limit) {
		q.grow()
	}

	if q.n == len(q.ring) { // full at the limit: drop the oldest
		q.ring[q.head] = v
		q.head = (q.head + 1) % len(q.ring)
	} else {
		q.ring[(q.head+q.n)%len(q.ring)] = v
		q.n++
	}

	q.mu.Unlock()
	q.signal()
}

// grow doubles the ring (bounded by limit), keeping events in order.
func (q *Queue[T]) grow() {
	size := max(2*len(q.ring), 16)
	if q.limit > 0 {
		size = min(size, q.limit)
	}

	ring := make([]T, size)
	for i := range q.n {
		ring[i] = q.ring[(q.head+i)%len(q.ring)]
	}

	q.ring, q.head = ring, 0
}

func (q *Queue[T]) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// Close stops accepting events; pending ones can still be read.
func (q *Queue[T]) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.signal()
}

// TryNext returns the oldest pending event without waiting.
func (q *Queue[T]) TryNext() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var zero T
	if q.n == 0 {
		return zero, false
	}

	v := q.ring[q.head]
	q.ring[q.head] = zero // release references for the GC
	q.head = (q.head + 1) % len(q.ring)
	q.n--

	if q.n > 0 {
		q.signal() // let another waiting consumer proceed
	}

	return v, true
}

// Next waits for an event. It returns false when ctx is done, or when the
// queue is closed and drained.
func (q *Queue[T]) Next(ctx context.Context) (T, bool) {
	for {
		if v, ok := q.TryNext(); ok {
			return v, true
		}

		q.mu.Lock()
		closed := q.closed
		q.mu.Unlock()

		if closed {
			var zero T

			q.signal() // wake other consumers so they observe the close too

			return zero, false
		}

		select {
		case <-q.notify:
		case <-ctx.Done():
			var zero T
			return zero, false
		}
	}
}
