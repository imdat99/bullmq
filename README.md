# bullmq (Go)

A Go port of [BullMQ](https://bullmq.io), the Redis-backed job queue, ported
from the official Rust implementation (the `bullmq-official` crate). It runs the **same Lua
scripts** and uses the **same Redis layout** as the Node.js, Python, Elixir and
Rust libraries, so producers and workers written in different languages can
share a queue.

- Queue: add / bulk add, delay, priority, LIFO, custom ids, deduplication,
  pause / resume / drain / clean / obliterate, counts, metrics, Prometheus
  export, global concurrency and rate limits, job schedulers (cron / every).
- Worker: concurrency (changeable at runtime), lock renewal, stalled-job
  recovery, retries with fixed / exponential / custom backoff, rate limiter,
  unrecoverable errors, manual processing, events, graceful close.
- FlowProducer: atomic trees of parent / child jobs across queues.
- QueueEvents: cross-process event stream listener.
- Redis standalone, TLS / mTLS and Redis Cluster (via go-redis v9).

## Documentation

Full guide (Vietnamese): [docs/](docs/README.md). Covers connecting to an
existing Redis, every job option, workers, retries, flows, job schedulers,
events, production operations, and an error reference.

## Install

```bash
go get bullmq   # module path is `bullmq`; adjust once published under a VCS path
```

Requires Go 1.26+ and Redis 6.2+ (7.x recommended).

## Usage

```go
ctx := context.Background()

q, err := bullmq.NewQueue(ctx, "emails", &bullmq.QueueOptions{
	Connection: bullmq.RedisConnectionOptions{URL: "redis://127.0.0.1:6379"},
})
if err != nil {
	log.Fatal(err)
}
defer q.Close()

_, err = q.Add(ctx, "welcome", map[string]string{"to": "a@example.com"}, &bullmq.JobOptions{
	Attempts: 3,
	Backoff:  bullmq.ExponentialBackoff(time.Second),
})

w, err := bullmq.NewWorker(ctx, "emails", func(ctx context.Context, job *bullmq.Job) (any, error) {
	var in struct{ To string }
	if err := job.DecodeData(&in); err != nil {
		return nil, bullmq.NewUnrecoverableError(err.Error()) // no retries
	}
	_ = job.UpdateProgress(ctx, 50)
	return map[string]any{"sent": true}, nil // stored as the job return value
}, &bullmq.WorkerOptions{Concurrency: 10})
if err != nil {
	log.Fatal(err)
}
defer w.Close(30 * time.Second) // waits for active jobs, then cancels their ctx
```

Flows and events:

```go
fp, _ := bullmq.NewFlowProducer(ctx, nil)
node, _ := fp.Add(ctx, bullmq.FlowJob{
	Name: "render", QueueName: "videos",
	Children: []bullmq.FlowJob{
		{Name: "transcode", QueueName: "transcode", Data: map[string]int{"res": 1080}},
	},
})

events, _ := bullmq.NewQueueEvents(ctx, "videos", nil)
for {
	e, ok := events.NextEvent(ctx)
	if !ok {
		break
	}
	fmt.Println(e.Event.Name(), e.Event.JobID)
}
```

## Design notes (Rust → Go)

| Rust | Go |
| --- | --- |
| `async fn` + tokio | `ctx context.Context` first argument, goroutines |
| `CancellationToken` passed to the processor | the processor `ctx` (`Worker.CancelJob`) |
| `serde_json::Value` data | `json.RawMessage` + `Job.DecodeData` |
| `Error` enum | sentinel errors (`ErrInvalidConfig`, `ErrDelayed`, ...) + `*ScriptError`, `*UnrecoverableError`, `*RateLimitError` |
| ms integers in options | `time.Duration` / `time.Time` (zero = unset) |
| redis-rs `ConnectionManager` | go-redis v9 pool (auto reconnect, RESP2, no blind retries of non-idempotent scripts) |

Reliability details:

- A single fetch driver per worker issues at most one blocking `BZPOPMIN`
  (on a dedicated named connection) regardless of concurrency; finished jobs
  chain the next job in the same round trip (`moveToFinished` fetch-next).
  While a backlog is being drained the driver fetches in parallel bursts
  (1, 2, 4, … 32 `moveToActive` calls per round) so hundreds of slots fill in
  a few round trips; an idle worker falls back to one fetch.
- Blocking reads carry a read deadline of block time + 10s, which detects
  half-open sockets; transient Redis errors are retried after
  `RunRetryDelay`.
- Processor panics are recovered and fail the job; every goroutine has an
  owner and exits on `Close`; worker events are buffered (bounded).

See [PORTING.md](PORTING.md) for the complete API mapping.

## Layout

```
.                        package bullmq: the public API (Queue, Worker, Job,
                         FlowProducer, QueueEvents, options, errors) + tests
├── example_test.go      examples rendered on pkg.go.dev
├── docs/                user guide (Vietnamese)
├── examples/            runnable programs: producer/, worker/
└── internal/            implementation details, not importable by users
    ├── scripts/         embedded BullMQ Lua commands (commands/*.lua) + runner
    ├── msgpack/         allocation-lean MessagePack encoder for script args
    └── eventqueue/      bounded ring-buffer event queue
```

```bash
REDIS_URL=redis://127.0.0.1:6379 go run ./examples/worker    # in one terminal
REDIS_URL=redis://127.0.0.1:6379 go run ./examples/producer  # in another
```

## Performance

Processing throughput is bound by the Redis CPU spent in the BullMQ Lua
scripts (`moveToFinished` with fetch-next ≈ 150µs, `addStandardJob` ≈ 55µs on
the reference machine), not by the Go client, which uses well under one CPU
core. Beyond a modest concurrency, raising `Concurrency` helps only when jobs
wait on I/O. To scale further, spread work over several queues placed on
different Redis Cluster shards (hash-tag the queue names, not a shared
prefix) or run Redis on faster cores.

### Memory / GC

The per-job hot paths are tuned for low allocation (measured with
`-benchmem`, see `benchmark_test.go`):

| Path | allocs/op | B/op |
| --- | --- | --- |
| `Queue.Add` | 49 → 25 | 2.15 KiB → 1.31 KiB |
| Worker, per processed job | 131 → 65 | 7.66 KiB → 4.49 KiB |

How: queue keys are computed once per queue and the hot script calls reuse
pre-boxed key lists (go-redis would otherwise box every key into an
`interface{}`), msgpack maps are encoded into a single buffer, jobs are
decoded straight from the Redis reply, script contexts are shared, and
worker events use a fixed ring buffer (no allocation in steady state, even
when nobody reads them). The remaining allocations are mostly go-redis reply
parsing. In containers, set `GOMEMLIMIT` to ~80–90% of the memory limit.

## Tests

The full Rust test-suite (≈580 integration and unit tests) is ported 1:1.

```bash
make test           # needs Redis on 127.0.0.1:6379 (REDIS_URL to override)
make test-cluster   # plus a Redis Cluster on 127.0.0.1:7000-7002
make lint
go test -run '^$' -bench . -benchtime 5000x   # throughput benchmarks
```
