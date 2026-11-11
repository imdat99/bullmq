// Package bullmq is a Go port of BullMQ, the Redis-backed job queue.
//
// It shares the Lua scripts and the Redis data layout of the Node.js, Python,
// Elixir and Rust implementations, so a queue can be fed and consumed from
// any of them.
//
// The main types are:
//
//   - [Queue] adds jobs and manages queue state (pause, clean, metrics,
//     job schedulers, ...).
//   - [Worker] processes jobs with concurrency, lock renewal, stalled job
//     recovery, retries with backoff and rate limiting.
//   - [Job] is a unit of work; jobs returned by a Queue or handed to a
//     processor are bound to their queue.
//   - [FlowProducer] atomically adds trees of dependent jobs.
//   - [QueueEvents] listens to the queue event stream across processes.
//
// Minimal example:
//
//	ctx := context.Background()
//	q, err := bullmq.NewQueue(ctx, "emails", nil)
//	if err != nil { ... }
//	defer q.Close()
//	_, err = q.Add(ctx, "welcome", map[string]string{"to": "a@example.com"}, nil)
//
//	w, err := bullmq.NewWorker(ctx, "emails", func(ctx context.Context, job *bullmq.Job) (any, error) {
//		var data struct{ To string }
//		if err := job.DecodeData(&data); err != nil {
//			return nil, bullmq.NewUnrecoverableError(err.Error())
//		}
//		return "sent", nil
//	}, &bullmq.WorkerOptions{Concurrency: 10})
//	if err != nil { ... }
//	defer w.Close(30 * time.Second)
package bullmq
