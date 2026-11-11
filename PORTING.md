# Rust → Go porting conventions (bullmq_rust → package `bullmq`)

Module path `bullmq`, single root package (files in repo root). Lua scripts are
embedded from `internal/scripts/commands/*.lua` (identical to the Rust crate's `src/commands`).
Redis client: `github.com/redis/go-redis/v9` (RESP2, `MaxRetries=-1`).

## General mapping

| Rust | Go |
| --- | --- |
| `async fn foo(&self, ..)` | `func (x *T) Foo(ctx context.Context, ..)` — ctx always first |
| `Result<T, Error>` | `(T, error)` |
| `Option<T>` return | `(T, bool, error)` or nil pointer, see each method |
| `serde_json::Value` job data | `json.RawMessage` (`Job.Data`); decode with `job.DecodeData(&v)` |
| snake_case methods | MixedCaps (`get_job_counts` → `GetJobCounts`) |
| ms `u64` options | `time.Duration` for durations, `time.Time` for timestamps (zero = unset) |
| `Option<u32>` options | plain values, zero = unset (e.g. `Attempts`, `Priority`, `KeepLogs`) |

## Errors (`errors.go`)

- `Error::InvalidConfig(msg)` → `fmt.Errorf("%w: msg", ErrInvalidConfig)`; test with
  `errors.Is(err, bullmq.ErrInvalidConfig)` and `strings.Contains(err.Error(), msg)`.
- `Error::Script{code,..}` → `*bullmq.ScriptError`; `bullmq.ScriptErrorCode(err)`.
- `Error::Unrecoverable(m)` → `bullmq.NewUnrecoverableError(m)`, `bullmq.IsUnrecoverable(err)`.
- `Error::Delayed` / `Error::WaitingChildren` → `bullmq.ErrDelayed` / `bullmq.ErrWaitingChildren`.
- `Error::RateLimited{..}` → `bullmq.NewRateLimitError(d)`.
- `Error::JobNotFound` → `bullmq.ErrJobNotFound` (wrapped), `JobLockNotExist` → `ErrJobLockNotExist`,
  `JobNotInState` → `ErrJobNotInState`, `JobLocked` → `ErrJobLocked`.
- `Error::ProcessingError(m)` → any plain `errors.New(m)` (retryable).

## Types

- `JobState`: `bullmq.StateWaiting|StateActive|StateDelayed|StatePrioritized|StateCompleted|StateFailed|StateWaitingChildren|StateUnknown`.
- `RemoveOnFinish::Bool(true)` → `bullmq.RemoveAll()`, `Bool(false)` → `bullmq.KeepAll()`,
  `Count(n)` → `bullmq.KeepLast(n)`, `Options(KeepJobs{..})` → `bullmq.KeepJobsPolicy(bullmq.KeepJobs{Age: bullmq.Ptr[int64](..), Count: .., Limit: ..})`.
- `BackoffStrategy::Fixed(ms)` → `bullmq.FixedBackoff(d)`, `Exponential(ms)` → `bullmq.ExponentialBackoff(d)`, `Custom(name)` → `bullmq.CustomBackoff(name)`.
- `JobProgress::Number(n)` / `Object(v)` → any JSON-able value passed to `UpdateProgress(ctx, v)`;
  `Job.Progress` / `WorkerEvent.Progress` are `json.RawMessage`.
- `JobCounts` fields: `Waiting, Active, Delayed, Prioritized, Completed, Failed, WaitingChildren, Paused` (int64).

## Options

- `QueueOptions{Connection, Prefix, DefaultJobOptions, SkipVersionCheck}`; `NewQueue(ctx, name, *QueueOptions)`.
- `WorkerOptions`: zero fields = defaults. `autorun: false` → `ManualStart: true`.
  `max_stalled_count: Some(n)` / `n` → `MaxStalledCount: bullmq.Ptr(n)`. `drain_delay: 1` (secs) → `DrainDelay: time.Second`.
  `lock_duration: 5000` → `LockDuration: 5*time.Second`. `limiter` → `Limiter: &bullmq.RateLimiterOptions{Max, Duration}`.
  `backoff_strategy` → `BackoffStrategy: func(attemptsMade int, typ string, err error, job *bullmq.Job) time.Duration` (negative = no retry).
  `metrics: Some(MetricsOptions{max_data_points})` → `Metrics: &bullmq.MetricsOptions{MaxDataPoints: n}`.
  `max_started_attempts: Some(n)` → `MaxStartedAttempts: bullmq.Ptr(n)`.
- `JobOptions{Delay, Priority, Attempts, Backoff, LIFO, RemoveOnComplete, RemoveOnFail, KeepLogs, JobID, Timestamp, StackTraceLimit, Parent *ParentOptions{Queue, ID}, Deduplication *DeduplicationOptions{ID, TTL, Extend, Replace, Reschedule, KeepLastIfActive}, Repeat *RepeatOptions, FailParentOnFailure, IgnoreDependencyOnFailure, RemoveDependencyOnFailure, ContinueParentOnFailure, SizeLimit}`.
- `RedisConnectionOptions{URL, MaxConnections, Host, Port, Username, Password, DB *int, TLS, TLSCerts *TLSCerts{RootCert, ClientCert, ClientKey}, ClusterNodes}` with `EffectiveURL()`, `IsCluster()`.
- `RepeatOptions{Pattern, Every time.Duration, Limit, Immediately, Offset time.Duration, StartDate, EndDate time.Time, TZ, Count}`.

## Queue (`queue.go`, `scheduler.go`)

`q.Add(ctx, name, data any, *JobOptions) (*Job, error)` — Rust builder `.add(..).delay(d).priority(p).options(o).await`
becomes one `JobOptions` literal. `q.AddBulk(ctx, []bullmq.BulkJob{{Name, Data, Opts}})`.
`Pause, Resume, IsPaused, GetJob (nil,nil if missing), GetJobCounts, GetRanges(ctx, types []string, start, end int64, asc)`,
`GetJobs(ctx, types, start, end, asc)`, `GetWaiting/GetActive/GetDelayed/GetPrioritized/GetCompleted/GetFailed/GetWaitingChildren(ctx, start, end)`,
`GetJobCountsByTypes(ctx, types...) map[string]int64`, `GetJobCountByTypes(ctx, types...) int64`, `RecordJobCountsMetric(ctx, types, func(state string, n int64))`,
`Count`, `GetCountsPerPriority(ctx, prios ...int) map[int]int64`, `GetCompletedCount/GetFailedCount/GetDelayedCount/GetActiveCount/GetPrioritizedCount/GetWaitingCount/GetWaitingChildrenCount`,
`GetWorkers(ctx) []map[string]string`, `GetWorkersCount(ctx) (int, error)`, `GetMeta(ctx) QueueMeta{Concurrency, Max, Duration, MaxLenEvents *int64; Paused bool; Other map}`,
`GetVersion(ctx) (string, error)`, `IsMaxed`, `ExportPrometheusMetrics(ctx, ...bullmq.PrometheusLabel{Name, Value})`,
`GetMetrics(ctx, "completed"|"failed", start, end) Metrics{Meta{Count, PrevTS, PrevCount}, Data []int64, Count}`,
`GetDependencies(ctx, parentID, "pending"|"processed", start, end) QueueDependencies{Items []DependencyItem{ID, Value any, Err string}, Jobs, Total}`,
`GetChildrenValues(ctx, id) map[string]any`, `GetFailedChildrenValues / GetIgnoredChildrenFailures(ctx, id) map[string]string`,
`GetDependenciesCount(ctx, id) DependenciesCount{Processed, Unprocessed, Ignored, Failed}`, `GetUnprocessedDependencies`,
`RemoveChildDependency(ctx, jobID, parentKey) (bool, error)`, `GetJobState(ctx, id) (JobState, error)`,
`Remove / RemoveWithoutChildren (bool, error)`, `RemoveUnprocessedChildren`, `Clean(ctx, grace time.Duration, limit int, state string) ([]string, error)`,
`Drain(ctx, delayed bool)`, `RetryJobs(ctx, state string, count int, ts time.Time)`, `PromoteJobs(ctx, count int)`,
`RateLimit(ctx, d)`, `RemoveRateLimitKey(ctx) (bool, error)`, `SetGlobalConcurrency(ctx, n int)`, `RemoveGlobalConcurrency`,
`GetGlobalConcurrency(ctx) (int64, bool, error)`, `SetGlobalRateLimit(ctx, max int64, d)`, `RemoveGlobalRateLimit`,
`GetGlobalRateLimit(ctx) (max int64, d time.Duration, ok bool, err error)`, `GetRateLimitTTL(ctx, maxJobs int64) (int64, error)` (Rust `None` → 0),
`RemoveDeduplicationKey(ctx, dedupID, jobID) (bool, error)`, `GetDeduplicationJobID(ctx, id) (string, bool, error)`,
`GetDebounceJobID`, `RemoveDebounceKey(ctx, id) (int64, error)`, `GetJobLogs(ctx, id, start, end int64, asc) ([]string, int64, error)`,
`TrimEvents(ctx, maxLen int64) (int64, error)`, `UpdateJobProgress(ctx, id, progress any)`, `Obliterate(ctx, force bool, count int)`,
`UpsertJobScheduler(ctx, id, RepeatOptions, jobName string, data any, *JobOptions) (*Job, error)` (Rust `Option<&str>` name None → "", data None → nil),
`GetJobScheduler(ctx, id) (*JobScheduler, error)`, `GetJobSchedulers(ctx, start, end int64, asc) ([]JobScheduler, error)`,
`GetJobSchedulersCount`, `RemoveJobScheduler(ctx, id) (bool, error)`, `Keys() QueueKeys`, `Client() redis.UniversalClient` (raw commands),
`Connection() *RedisConnection`, `Close() error`. `NextCronTime(pattern, now, tz, startDate) (time.Time, error)`.

## Job (`job.go`)

Fields: `ID, Name, Data, Opts, Progress, AttemptsMade, AttemptsStarted, Timestamp(ms), ProcessedOn(ms, 0=unset), FinishedOn, FailedReason,
Stacktrace, ReturnValue (raw JSON string), Parent *ParentKeys{QueueKey, ID}, ParentKey string, ProcessedBy, StalledCounter, Delay, RepeatJobKey, DeferredFailure, QueueName`.
Methods (all take ctx): `GetChildrenValues, GetIgnoredChildrenFailures, GetFailedChildrenValues, GetDependenciesCount, GetDependencies(ctx, pc, uc uint64, count int64),
GetUnprocessedDependencies, RemoveChildDependency, MoveToWaitingChildren(ctx, childKey string) (bool, error), UpdateProgress(ctx, any), UpdateData(ctx, any),
Log(ctx, row) (int64, error), ClearLogs(ctx, keep int) (Rust None → 0), Retry(ctx, bullmq.StateFailed|StateCompleted, *RetryOptions), GetState, IsCompleted/IsFailed/IsActive/IsWaiting/IsDelayed/IsWaitingChildren,
ExtendLock(ctx, token, d), Promote, ChangeDelay(ctx, d), ChangePriority(ctx, prio int, lifo bool), MoveToCompleted(ctx, any), MoveToFailed(ctx, reason string),
MoveToDelayed(ctx, time.Time), MoveToWait(ctx, token string) (time.Duration, error)`. Non-ctx: `Token(), QueueQualifiedName(), Key(), Discard(), IsDiscarded(), Priority(), DecodeData(&v), DecodeReturnValue(&v)`.

## Worker (`worker.go`)

`bullmq.NewWorker(ctx, queueName, processor, *WorkerOptions)`; processor is
`func(ctx context.Context, job *bullmq.Job) (any, error)` — the Rust `CancellationToken` is the ctx (cancelled by `CancelJob`/`CancelAllJobs`).
`w.Close(timeout time.Duration) error` (Rust `close(5000)` → `Close(5*time.Second)`), `Run() error`, `Pause()`, `Resume()`, `IsRunning()`, `IsPaused()`,
`ActiveCount()`, `Concurrency()`, `SetConcurrency(n)`, `CancelJob(id) bool`, `CancelAllJobs()`, `ID()`,
`GetNextJob(ctx, token)` / `GetNextJobNonBlocking(ctx, token)` (nil, nil when none).
Events: `w.NextEvent(ctx) (WorkerEvent, bool)` / `w.TryNextEvent()`; `WorkerEvent{Type, JobID, Result json.RawMessage, Err error, Progress json.RawMessage}`;
types `EventCompleted, EventFailed, EventError, EventDrained, EventReady, EventClosed, EventStalled, EventActive, EventProgress, EventPaused, EventResumed`.
`tokio::time::timeout(d, loop { next_event })` → `ctx, cancel := context.WithTimeout(...)` + `NextEvent(ctx)`.

## Tests

- Integration tests live in package `bullmq_test`; shared helpers in `helpers_test.go`:
  `testConnection()`, `testQueueName()`, `newTestQueue(t, name, *QueueOptions)` (auto-cleanup),
  `newTestWorker(t, name, proc, *WorkerOptions)` (auto-close), `cleanupQueue(q)`, `waitForEvent(t, w, timeout, match)`,
  `eventType(typ)`, `eventually(t, timeout, cond, msg)`, `must(v, err)` (panics), `noErr(t, err)`.
- Rust unit tests in `src/*.rs` `#[cfg(test)]` → package `bullmq` internal tests (`*_internal_test.go`).
- Keep the Rust test names as `TestXxx` (CamelCase of the Rust fn name) and keep the Node.js reference comments.
- Use the standard library `testing` only (no testify). Run with `go test -race`.
- Each test must be independent (unique queue names) and must not leak goroutines (always close workers).
- `tokio::spawn` → goroutine + channel/WaitGroup; `AtomicU32` → `atomic.Int32`; `mpsc` → buffered chan.
- Extra helpers: `isolatedConnection(db int)`, `withDatabase(url, db)`, `killConnectionsInDB(t, db) int64` (Rust `common::kill_connections_in_db`),
  `rawClient(t) *redis.Client` (a raw go-redis client for direct Redis assertions; `queue.Client()` works too).
- Do NOT edit library (non-test) files or `helpers_test.go` from a test-porting task. If a faithfully ported test fails, find the root
  cause; if it is a library bug, report it (file:line, diagnosis, proposed patch) instead of weakening or skipping the test.
  Only skip (with `t.Skip("reason")`) tests that need infrastructure that is absent (e.g. a Redis Cluster, TLS server) — mirror Rust's own skip logic.

## FlowProducer (`flow_producer.go`)

`FlowJob{Name, QueueName string; Data any; Opts *JobOptions; Prefix string; Children []FlowJob}`,
`GetFlowOptions{QueueName, ID, Prefix string; Depth, MaxChildren int}` (0 = defaults 10/20), `JobNode{Job *Job; Children []JobNode}`,
`FlowQueueOptions{DefaultJobOptions}`, `FlowOptions{QueuesOptions map[string]FlowQueueOptions}`, `FlowProducerOptions{Connection, Prefix}`.
`NewFlowProducer(ctx, *FlowProducerOptions)`, `NewFlowProducerWithConnection(conn, prefix)`, `Add(ctx, FlowJob) (JobNode, error)`,
`AddWithOptions(ctx, FlowJob, *FlowOptions)`, `AddBulk(ctx, []FlowJob) ([]JobNode, error)`, `AddBulkWithOptions`, `GetFlow(ctx, GetFlowOptions) (JobNode, error)`
(missing root → error wrapping ErrJobNotFound), `Prefix()`, `Connection()`, `Close() error`.
Rust `FlowJob::new(name, queue, data).add_child(c).options(o)` → struct literal.

## QueueEvents (`queue_events.go`)

`QueueEventsOptions{Connection, Prefix, BlockingTimeout time.Duration, LastEventID string, ManualStart bool}`;
`NewQueueEvents(ctx, name, *QueueEventsOptions)`, `NewQueueEventsWithConnection(ctx, name, conn, opts)`, `NewQueueEventsFromQueueOptions(ctx, name, *QueueOptions)`;
methods `Name(), Keys(), IsRunning(), Run() error, NextEvent(ctx) (QueueEventEntry, bool), TryNextEvent(), Close() error`.
`QueueEventEntry{ID string; Event QueueEvent}`; `QueueEvent{Type QueueEventType; JobID, Prev, JobName, Count string; ReturnValue json.RawMessage;
DebounceID, DeduplicationID, DeduplicatedJobID string; Delay int64 (ms); Message, FailedReason string; Progress json.RawMessage; AttemptsMade string; Event string; Fields map[string]string}`
with `Name()`, `LookupJobID() (string, bool)` (Rust `job_id()`), `DelayUntil()`, `DecodeReturnValue(&v)`, `DecodeProgress(&v)`.
Types: `QueueEventActive, QueueEventAdded, QueueEventCleaned, QueueEventCompleted, QueueEventDebounced, QueueEventDeduplicated, QueueEventDelayed, QueueEventDrained,
QueueEventDuplicated, QueueEventError, QueueEventFailed, QueueEventPaused, QueueEventProgress, QueueEventRemoved, QueueEventResumed, QueueEventRetriesExhausted,
QueueEventStalled, QueueEventWaiting, QueueEventWaitingChildren, QueueEventOther`. Rust `QueueEvent::Completed { job_id, return_value, .. }` match → check `Type` and fields.
