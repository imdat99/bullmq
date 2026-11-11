package bullmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/imdat99/bullmq/internal/eventqueue"
	"github.com/imdat99/bullmq/internal/scripts"
)

// Processor processes a job and returns its result, which is stored
// JSON-encoded as the job's return value.
//
// ctx is cancelled when the job is cancelled with [Worker.CancelJob] or
// [Worker.CancelAllJobs], or when [Worker.Close] times out. Returning
// [ErrDelayed] / [ErrWaitingChildren] after moving the job yourself leaves it
// alone; an [UnrecoverableError] fails it without retries; a
// [RateLimitError] moves it back to wait. A panic fails the job.
type Processor func(ctx context.Context, job *Job) (any, error)

// WorkerEventType identifies a [WorkerEvent].
type WorkerEventType uint8

// Worker event types.
const (
	EventCompleted WorkerEventType = iota + 1
	EventFailed
	EventError
	EventDrained
	EventReady
	EventClosed
	EventStalled
	EventActive
	EventProgress
	EventPaused
	EventResumed
)

var workerEventNames = [...]string{
	EventCompleted: "completed", EventFailed: "failed", EventError: "error", EventDrained: "drained",
	EventReady: "ready", EventClosed: "closed", EventStalled: "stalled", EventActive: "active",
	EventProgress: "progress", EventPaused: "paused", EventResumed: "resumed",
}

func (t WorkerEventType) String() string {
	if int(t) < len(workerEventNames) && workerEventNames[t] != "" {
		return workerEventNames[t]
	}

	return "unknown(" + strconv.Itoa(int(t)) + ")"
}

// WorkerEvent is an event emitted by a [Worker].
type WorkerEvent struct {
	Type WorkerEventType
	// JobID is set for job events.
	JobID string
	// Result is the JSON-encoded return value (EventCompleted).
	Result json.RawMessage
	// Err is the failure (EventFailed) or the worker error (EventError).
	Err error
	// Progress is the JSON-encoded progress (EventProgress).
	Progress json.RawMessage
}

// maxBufferedEvents bounds the unread worker events; older ones are dropped.
const maxBufferedEvents = 100_000

// markerWaitCap caps a blocking wait for a delayed job so the worker
// re-evaluates its state regularly.
const markerWaitCap = 5 * time.Second

type activeJob struct {
	token  string
	cancel context.CancelFunc
}

// Worker fetches jobs from a queue and runs a [Processor] on them, with
// concurrency, lock renewal, stalled-job recovery, retries with backoff and
// rate limiting. It is safe for concurrent use.
type Worker struct {
	keys      QueueKeys
	opts      WorkerOptions
	id        string
	conn      *RedisConnection
	ownsConn  bool
	blocking  redis.UniversalClient
	processor Processor
	events    *eventqueue.Queue[WorkerEvent]
	tokenSeq  atomic.Uint64

	running atomic.Bool
	paused  atomic.Bool
	closing atomic.Bool

	mu     sync.Mutex
	active map[string]*activeJob

	// Dynamic concurrency gate: slots counts jobs being fetched or processed.
	slotMu      sync.Mutex
	slots       int
	concurrency atomic.Int64
	slotFreed   chan struct{} // buffered(1)

	// lifeCtx is cancelled on Close to stop the background loops.
	lifeCtx    context.Context //nolint:containedctx // lifetime of the background loops, cancelled by Close
	lifeCancel context.CancelFunc
	loops      sync.WaitGroup // driver, stalled checker, lock renewer
	jobs       sync.WaitGroup // processing goroutines
	driverDone chan struct{}
	closeCh    chan struct{} // closed when Close starts
	runCalled  atomic.Bool
	closeOnce  sync.Once
}

// NewWorker creates a worker for queueName and, unless
// [WorkerOptions.ManualStart] is set, starts processing. A nil opts selects
// the defaults.
func NewWorker(ctx context.Context, queueName string, processor Processor, opts *WorkerOptions) (*Worker, error) {
	var o WorkerOptions
	if opts != nil {
		o = *opts
	}

	if err := o.validate(); err != nil {
		return nil, err
	}

	o = o.withDefaults()

	if err := validateQueueName(queueName); err != nil {
		return nil, err
	}

	if err := validatePrefix(o.Prefix); err != nil {
		return nil, err
	}

	conn, err := NewRedisConnection(ctx, o.Connection)
	if err != nil {
		return nil, err
	}

	w, err := newWorker(ctx, queueName, processor, o, conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	w.ownsConn = true
	if !o.ManualStart {
		if err := w.Run(); err != nil {
			_ = w.Close(0)
			return nil, err
		}
	}

	return w, nil
}

// NewWorkerWithConnection is like [NewWorker] but shares conn (which must
// wrap a *redis.Client or *redis.ClusterClient). Close does not close conn.
func NewWorkerWithConnection(ctx context.Context, queueName string, processor Processor, conn *RedisConnection, opts *WorkerOptions) (*Worker, error) {
	var o WorkerOptions
	if opts != nil {
		o = *opts
	}

	if err := o.validate(); err != nil {
		return nil, err
	}

	o = o.withDefaults()

	if err := validateQueueName(queueName); err != nil {
		return nil, err
	}

	if err := validatePrefix(o.Prefix); err != nil {
		return nil, err
	}

	w, err := newWorker(ctx, queueName, processor, o, conn)
	if err != nil {
		return nil, err
	}

	if !o.ManualStart {
		if err := w.Run(); err != nil {
			_ = w.Close(0)
			return nil, err
		}
	}

	return w, nil
}

func newWorker(ctx context.Context, queueName string, processor Processor, o WorkerOptions, conn *RedisConnection) (*Worker, error) {
	if processor == nil {
		return nil, invalidConfig("processor must not be nil")
	}

	keys := NewQueueKeys(queueName, o.Prefix)

	suffix := ""
	if o.Name != "" {
		suffix = ":w:" + o.Name
	}

	blocking, err := conn.newDedicatedClient(keys.ClientName(suffix))
	if err != nil {
		return nil, err
	}

	setBlockingReadTimeout(blocking, o)
	// Open the named connection now so Queue.GetWorkers sees the worker
	// immediately (best effort: some managed Redis reject CLIENT SETNAME).
	_ = blocking.Ping(ctx).Err()

	lifeCtx, cancel := context.WithCancel(context.Background())
	w := &Worker{
		keys:       keys,
		opts:       o,
		id:         uuid.NewString(),
		conn:       conn,
		blocking:   blocking,
		processor:  processor,
		events:     eventqueue.New[WorkerEvent](maxBufferedEvents),
		active:     map[string]*activeJob{},
		slotFreed:  make(chan struct{}, 1),
		lifeCtx:    lifeCtx,
		lifeCancel: cancel,
		driverDone: make(chan struct{}),
		closeCh:    make(chan struct{}),
	}
	w.concurrency.Store(int64(o.Concurrency))

	return w, nil
}

// setBlockingReadTimeout sizes the read deadline of the blocking client to
// the longest block the worker issues, plus a margin acting as a watchdog for
// half-open sockets.
func setBlockingReadTimeout(c redis.UniversalClient, o WorkerOptions) {
	longest := max(o.DrainDelay, o.MaximumRateLimitDelay, markerWaitCap) + 10*time.Second

	switch cl := c.(type) {
	case *redis.Client:
		cl.Options().ReadTimeout = longest
	case *redis.ClusterClient:
		cl.Options().ReadTimeout = longest
	}
}

// ID returns the worker's unique id.
func (w *Worker) ID() string { return w.id }

// Name returns the worker name.
func (w *Worker) Name() string { return w.opts.Name }

// Concurrency returns the current concurrency.
func (w *Worker) Concurrency() int { return int(w.concurrency.Load()) }

// SetConcurrency changes the number of jobs processed in parallel. Values
// below 1 are ignored.
func (w *Worker) SetConcurrency(n int) {
	if n < 1 {
		return
	}

	w.concurrency.Store(int64(n))
	w.wakeGate()
}

// IsRunning reports whether the worker is processing jobs.
func (w *Worker) IsRunning() bool { return w.running.Load() }

// IsPaused reports whether the worker is paused.
func (w *Worker) IsPaused() bool { return w.paused.Load() }

// ActiveCount returns the number of jobs being processed.
func (w *Worker) ActiveCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return len(w.active)
}

// NextEvent waits for the next event. It returns false when ctx is done or
// the worker is closed and every event has been read.
func (w *Worker) NextEvent(ctx context.Context) (WorkerEvent, bool) { return w.events.Next(ctx) }

// TryNextEvent returns the next event without waiting.
func (w *Worker) TryNextEvent() (WorkerEvent, bool) { return w.events.TryNext() }

func (w *Worker) emit(e WorkerEvent) { w.events.Push(e) }

func (w *Worker) emitError(err error) {
	w.emit(WorkerEvent{Type: EventError, Err: err})
}

// Run starts processing. It returns an error if the worker already runs or
// was closed.
func (w *Worker) Run() error {
	if w.closing.Load() {
		return ErrWorkerClosed
	}

	if !w.running.CompareAndSwap(false, true) {
		return invalidConfig("Worker is already running")
	}

	if !w.runCalled.CompareAndSwap(false, true) {
		return invalidConfig("Worker can only be run once")
	}

	w.emit(WorkerEvent{Type: EventReady})

	if !w.opts.SkipStalledCheck {
		w.loops.Add(1)
		go w.stalledCheckLoop()
	}

	if !w.opts.SkipLockRenewal {
		w.loops.Add(1)
		go w.lockRenewalLoop()
	}

	w.loops.Add(1)
	go w.driver()

	return nil
}

// Pause stops fetching new jobs; active jobs finish normally.
func (w *Worker) Pause() {
	w.paused.Store(true)
	w.emit(WorkerEvent{Type: EventPaused})
}

// Resume resumes fetching after [Worker.Pause].
func (w *Worker) Resume() {
	w.paused.Store(false)
	w.emit(WorkerEvent{Type: EventResumed})
	w.wakeGate()
}

// CancelJob cancels the context of an active job. It reports whether the job
// was active.
func (w *Worker) CancelJob(jobID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	a, ok := w.active[jobID]
	if ok {
		a.cancel()
	}

	return ok
}

// CancelAllJobs cancels the context of every active job.
func (w *Worker) CancelAllJobs() {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, a := range w.active {
		a.cancel()
	}
}

// Close stops fetching jobs and waits up to timeout for active jobs to
// finish. Jobs still running after the timeout get their context cancelled;
// their locks expire and the stalled check of another worker recovers them.
// Close is idempotent.
func (w *Worker) Close(timeout time.Duration) error {
	w.closeOnce.Do(func() {
		w.closing.Store(true)
		w.running.Store(false)
		close(w.closeCh)
		// Interrupt a blocking BZPOPMIN; fetching stops right away.
		_ = w.blocking.Close()

		deadline, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		// The driver exits promptly (closeCh is closed and its blocking client
		// is gone). It is the only caller of jobs.Add, so it must be finished
		// before jobs.Wait starts (WaitGroup Add/Wait ordering).
		if w.runCalled.Load() {
			<-w.driverDone
		}

		jobsDone := make(chan struct{})

		go func() {
			w.jobs.Wait()
			close(jobsDone)
		}()

		select {
		case <-jobsDone:
		case <-deadline.Done():
			slog.Warn("bullmq: worker close timeout reached with active jobs", "queue", w.keys.Name(), "active", w.ActiveCount())
			w.CancelAllJobs()
		}

		w.lifeCancel()
		w.loops.Wait()
		w.emit(WorkerEvent{Type: EventClosed})
		w.events.Close()

		if w.ownsConn {
			select {
			case <-jobsDone:
				_ = w.conn.Close()
			default:
				// Processors still run; close the connection once they return
				// so their final writes are not lost needlessly.
				go func() {
					<-jobsDone

					_ = w.conn.Close()
				}()
			}
		}
	})

	return nil
}

// ── Manual processing ───────────────────────────────────────────────────────

// GetNextJob fetches the next job for manual processing, locking it with
// token. It blocks up to DrainDelay when the queue is empty and returns
// (nil, nil) when no job is available. Complete the job with
// [Job.MoveToCompleted] or [Job.MoveToFailed].
func (w *Worker) GetNextJob(ctx context.Context, token string) (*Job, error) {
	return w.getNextJob(ctx, token, true)
}

// GetNextJobNonBlocking is like [Worker.GetNextJob] without waiting.
func (w *Worker) GetNextJobNonBlocking(ctx context.Context, token string) (*Job, error) {
	return w.getNextJob(ctx, token, false)
}

func (w *Worker) getNextJob(ctx context.Context, token string, block bool) (*Job, error) {
	if w.closing.Load() {
		return nil, ErrWorkerClosed
	}

	if w.paused.Load() {
		return nil, nil
	}

	res, err := moveToActive(ctx, w.conn, w.keys, token, &w.opts)
	if err != nil {
		return nil, err
	}

	if res.job == nil && block {
		popped, err := w.waitForMarker(ctx, w.opts.DrainDelay)
		if err != nil {
			return nil, err
		}

		if popped {
			if res, err = moveToActive(ctx, w.conn, w.keys, token, &w.opts); err != nil {
				return nil, err
			}
		}
	}

	if res.job == nil {
		return nil, nil
	}

	res.job.bind(w.scriptContext(token))
	w.emit(WorkerEvent{Type: EventActive, JobID: res.job.ID})

	return res.job, nil
}

func (w *Worker) scriptContext(token string) *scriptContext {
	return &scriptContext{
		conn:         w.conn,
		keys:         w.keys,
		token:        token,
		lockDuration: w.opts.LockDuration,
		onProgress: func(jobID string, p json.RawMessage) {
			w.emit(WorkerEvent{Type: EventProgress, JobID: jobID, Progress: p})
		},
	}
}

// ── Main loop ───────────────────────────────────────────────────────────────

// driver is the single fetch-and-dispatch loop (as in Node.js): it fetches
// one job at a time, issues at most one blocking BZPOPMIN when idle, and runs
// each job in its own goroutine bounded by the concurrency gate.
func (w *Worker) driver() {
	defer w.loops.Done()
	defer close(w.driverDone)
	// Commands are not tied to lifeCtx: cancelling a script mid-flight could
	// leave a job active with nobody processing it.
	ctx := context.WithoutCancel(w.lifeCtx)
	// burst is how many moveToActive calls the next round issues in parallel.
	// It doubles while every fetch returns a job (a backlog is being drained
	// or a burst just arrived) so all slots fill in O(log n) round trips
	// instead of one round trip per slot, and drops back to 1 as soon as the
	// queue runs dry, so an idle worker still issues a single fetch and a
	// single blocking BZPOPMIN (no thundering herd).
	burst := 1

	for !w.closing.Load() {
		if w.paused.Load() {
			w.sleep(100 * time.Millisecond)
			continue
		}

		n := w.acquireSlots(burst)
		if n == 0 {
			continue // closing; the loop condition exits
		}

		if w.closing.Load() || w.paused.Load() {
			w.releaseSlots(n)
			continue
		}

		if w.fetchRound(ctx, n) {
			burst = min(burst*2, maxFetchBurst)
		} else {
			burst = 1
		}
	}
}

// maxFetchBurst bounds the parallel moveToActive calls of one fetch round.
const maxFetchBurst = 32

// fetchRound runs n parallel fetches (n slots are held), dispatches the jobs
// it gets and handles the idle outcome. It reports whether every fetch
// returned a job.
func (w *Worker) fetchRound(ctx context.Context, n int) bool {
	tokens := make([]string, n)
	results := make([]fetchResult, n)
	errs := make([]error, n)

	for i := range n {
		tokens[i] = w.nextToken()
	}

	if n == 1 {
		results[0], errs[0] = moveToActive(ctx, w.conn, w.keys, tokens[0], &w.opts)
	} else {
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() {
				results[i], errs[i] = moveToActive(ctx, w.conn, w.keys, tokens[i], &w.opts)
			})
		}

		wg.Wait()
	}

	var (
		fetchErr error
		idle     fetchResult
		gotAll   = true
	)

	for i := range n {
		switch {
		case errs[i] != nil:
			w.releaseSlot()

			fetchErr, gotAll = errs[i], false
		case results[i].job != nil:
			w.jobs.Add(1) // only the driver adds, before Close waits

			go w.processChain(results[i].job, tokens[i])
		default:
			w.releaseSlot()

			gotAll = false
			idle = mostInformative(idle, results[i])
		}
	}

	switch {
	case gotAll:
	case fetchErr != nil:
		if !w.closing.Load() {
			slog.Warn("bullmq: failed to fetch job, retrying", "queue", w.keys.Name(), "error", fetchErr)
			w.emitError(fetchErr)
			w.sleep(w.opts.RunRetryDelay)
		}
	case idle.rateLimited > 0:
		_, _ = w.waitForMarkerLogged(ctx, min(idle.rateLimited, w.opts.MaximumRateLimitDelay))
	case idle.nextDelayed > 0:
		if wait := time.Duration(idle.nextDelayed-nowMillis()) * time.Millisecond; wait > 0 {
			_, _ = w.waitForMarkerLogged(ctx, min(wait, markerWaitCap))
		}
	case n > 1 && n > countNil(results):
		// Some fetches of a burst got jobs: the queue just ran dry. Loop back
		// to a single fetch instead of blocking, in case more arrived.
	default:
		w.emit(WorkerEvent{Type: EventDrained})
		_, _ = w.waitForMarkerLogged(ctx, w.opts.DrainDelay)
	}

	return gotAll
}

// mostInformative keeps the idle outcome that dictates the wait: a rate
// limit wins over a delayed job, which wins over an empty queue.
func mostInformative(a, b fetchResult) fetchResult {
	switch {
	case a.rateLimited > 0:
		return a
	case b.rateLimited > 0:
		return b
	case a.nextDelayed > 0 && (b.nextDelayed == 0 || a.nextDelayed <= b.nextDelayed):
		return a
	default:
		return b
	}
}

// countNil returns how many results carry no job.
func countNil(rs []fetchResult) int {
	n := 0

	for _, r := range rs {
		if r.job == nil {
			n++
		}
	}

	return n
}

func (w *Worker) nextToken() string {
	return w.id + ":" + strconv.FormatUint(w.tokenSeq.Add(1)-1, 10)
}

// sleep waits for d or until the worker closes.
func (w *Worker) sleep(d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-t.C:
	case <-w.closeCh:
	}
}

// waitForMarker blocks on the marker key until a job arrives or timeout
// elapses. It reports whether a marker was popped.
func (w *Worker) waitForMarker(ctx context.Context, timeout time.Duration) (bool, error) {
	secs := math.Max(float64(timeout.Milliseconds()), 1) / 1000 // 0 would block forever

	err := w.blocking.Do(ctx, "BZPOPMIN", w.keys.Marker(), strconv.FormatFloat(secs, 'f', -1, 64)).Err()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func (w *Worker) waitForMarkerLogged(ctx context.Context, timeout time.Duration) (bool, error) {
	popped, err := w.waitForMarker(ctx, timeout)
	if err != nil && !w.closing.Load() {
		slog.Warn("bullmq: blocking wait failed, retrying", "queue", w.keys.Name(), "error", err)
		w.emitError(err)
		w.sleep(100 * time.Millisecond)
	}

	return popped, err
}

// acquireSlot waits for a free concurrency slot. It returns false when the
// worker is closing.
func (w *Worker) acquireSlot() bool {
	for {
		w.slotMu.Lock()
		if w.closing.Load() {
			w.slotMu.Unlock()
			return false
		}

		if int64(w.slots) < w.concurrency.Load() {
			w.slots++
			w.slotMu.Unlock()

			return true
		}
		w.slotMu.Unlock()

		select {
		case <-w.slotFreed:
		case <-w.closeCh:
			return false
		}
	}
}

// acquireSlots waits for one free slot, then takes up to want-1 more that
// are free right now. It returns 0 when the worker is closing.
func (w *Worker) acquireSlots(want int) int {
	if !w.acquireSlot() {
		return 0
	}

	w.slotMu.Lock()
	defer w.slotMu.Unlock()

	got := 1
	for got < want && int64(w.slots) < w.concurrency.Load() {
		w.slots++
		got++
	}

	return got
}

func (w *Worker) releaseSlots(n int) {
	w.slotMu.Lock()
	w.slots -= n
	w.slotMu.Unlock()
	w.wakeGate()
}

func (w *Worker) releaseSlot() {
	w.slotMu.Lock()
	w.slots--
	w.slotMu.Unlock()
	w.wakeGate()
}

func (w *Worker) wakeGate() {
	select {
	case w.slotFreed <- struct{}{}:
	default:
	}
}

// shouldFetchNext mirrors Node.js fetchNextCallback: chain the next job into
// the finishing script only while running within the concurrency budget.
func (w *Worker) shouldFetchNext() bool {
	if w.closing.Load() || w.paused.Load() {
		return false
	}

	w.slotMu.Lock()
	defer w.slotMu.Unlock()

	return int64(w.slots) <= w.concurrency.Load()
}

// processChain processes job and any job chained by the finishing script,
// then frees the slot.
func (w *Worker) processChain(job *Job, token string) {
	defer w.jobs.Done()
	defer w.releaseSlot()

	// Every job of a chain is locked with the same token: share one context.
	sc := w.scriptContext(token)
	for job != nil {
		job = w.processJob(job, sc)
	}
}

// processJob runs the processor on job and moves it to its final state. It
// returns the next job when the finishing script fetched one.
func (w *Worker) processJob(job *Job, sc *scriptContext) *Job {
	token := sc.token
	ctx := context.Background()

	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	job.bind(sc)

	w.mu.Lock()
	w.active[job.ID] = &activeJob{token: token, cancel: cancel}

	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		delete(w.active, job.ID)
		w.mu.Unlock()
	}()

	w.emit(WorkerEvent{Type: EventActive, JobID: job.ID})

	// A deferred failure (a child failed with failParentOnFailure) or too many
	// starts fails the job before processing (Node getUnrecoverableErrorMessage).
	reason := job.DeferredFailure
	if reason == "" && w.opts.MaxStartedAttempts != nil && *w.opts.MaxStartedAttempts < job.AttemptsStarted {
		reason = "job started more than allowable limit"
	}

	if reason != "" {
		return w.handleFailed(ctx, job, token, NewUnrecoverableError(reason), w.shouldFetchNext())
	}

	result, err := w.runProcessor(jobCtx, job)

	var raw []byte
	if err == nil {
		if raw, err = json.Marshal(result); err != nil {
			err = fmt.Errorf("bullmq: marshal return value: %w", err)
		}
	}

	fetchNext := w.shouldFetchNext()

	var next *Job
	if err == nil {
		next = w.handleCompleted(ctx, job, token, raw, fetchNext)
	} else {
		if job.IsDiscarded() && shouldConvertDiscardedError(err) {
			err = NewUnrecoverableError(err.Error())
		}

		next = w.handleFailed(ctx, job, token, err, fetchNext)
	}

	if job.RepeatJobKey != "" {
		if err := updateJobScheduler(ctx, w.conn, w.keys, job.RepeatJobKey, job.ID, job.Data, &job.Opts); err != nil {
			slog.Error("bullmq: failed to schedule next iteration", "job", job.ID, "scheduler", job.RepeatJobKey, "error", err)
			w.emitError(fmt.Errorf("Failed to add repeatable job for next iteration: %w", err))
		}
	}

	return next
}

// runProcessor calls the processor, converting a panic into an error.
func (w *Worker) runProcessor(ctx context.Context, job *Job) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("bullmq: processor panicked", "job", job.ID, "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("processor panic: %v", r)
		}
	}()

	return w.processor(ctx, job)
}

// shouldConvertDiscardedError reports whether a failure of a discarded job
// must become unrecoverable (control-flow errors are left alone).
func shouldConvertDiscardedError(err error) bool {
	var rl *RateLimitError
	return !IsUnrecoverable(err) && !errors.Is(err, ErrDelayed) && !errors.Is(err, ErrWaitingChildren) && !errors.As(err, &rl)
}

func (w *Worker) finishOpts(job *Job, token string, target JobState) *finishOpts {
	keep := job.Opts.RemoveOnComplete
	if target == StateFailed {
		keep = job.Opts.RemoveOnFail
	}

	if keep == nil {
		if target == StateFailed {
			keep = w.opts.RemoveOnFail
		} else {
			keep = w.opts.RemoveOnComplete
		}
	}

	o := &finishOpts{
		token:        token,
		keep:         keep,
		lockDuration: w.opts.LockDuration,
		attempts:     job.Opts.Attempts,
		name:         w.opts.Name,
		limiter:      w.opts.Limiter,
	}
	if w.opts.Metrics != nil {
		o.maxMetricsSize = w.opts.Metrics.MaxDataPoints
	}

	return o
}

func (w *Worker) handleCompleted(ctx context.Context, job *Job, token string, raw []byte, fetchNext bool) *Job {
	res, err := moveToFinished(ctx, w.conn, w.keys, job.ID, StateCompleted, string(raw), w.finishOpts(job, token, StateCompleted), fetchNext)
	if err != nil {
		if code, ok := ScriptErrorCode(err); ok {
			// e.g. pending or failed children: fail the job like Node.js.
			return w.handleFailed(ctx, job, token, finishedError(code, job.ID, "moveToFinished", "active"), fetchNext)
		}

		slog.Error("bullmq: failed to move job to completed", "job", job.ID, "error", err)
		w.emitError(err)

		return nil
	}

	job.ReturnValue = string(raw)
	w.emit(WorkerEvent{Type: EventCompleted, JobID: job.ID, Result: raw})

	return w.chainedJob(res, fetchNext)
}

func (w *Worker) chainedJob(res any, fetchNext bool) *Job {
	if !fetchNext {
		return nil
	}

	if _, ok := res.([]any); !ok {
		return nil
	}

	fr, err := parseMoveToActive(res)
	if err != nil {
		return nil
	}

	return fr.job
}

func (w *Worker) handleFailed(ctx context.Context, job *Job, token string, procErr error, fetchNext bool) *Job {
	if errors.Is(procErr, ErrDelayed) || errors.Is(procErr, ErrWaitingChildren) {
		return nil // the processor already moved the job
	}

	if _, ok := errors.AsType[*RateLimitError](procErr); ok {
		if _, err := moveJobFromActiveToWait(ctx, w.conn, w.keys, job.ID, token); err != nil {
			slog.Error("bullmq: failed to move rate-limited job back to wait", "job", job.ID, "error", err)
			w.emitError(err)
		}

		return nil
	}

	attempts := job.Opts.Attempts
	if !IsUnrecoverable(procErr) && attempts > 0 && job.AttemptsMade+1 < attempts {
		w.retry(ctx, job, token, procErr)
		return nil
	}

	return w.failPermanently(ctx, job, token, procErr, fetchNext)
}

func (w *Worker) retry(ctx context.Context, job *Job, token string, procErr error) {
	reason := procErr.Error()

	var err error

	delay := time.Duration(0)
	if b := job.Opts.Backoff; b != nil {
		delay = w.backoffDelay(b, job.AttemptsMade+1, procErr, job)
	}

	switch {
	case delay < 0:
		// The custom strategy refused a retry. Never chain a job here: the
		// retry path has no consumer for it.
		w.failPermanently(ctx, job, token, procErr, false)
		return
	case delay > 0:
		err = moveToDelayed(ctx, w.conn, w.keys, job.ID, token, delay, false, w.opts.LockDuration, reason)
	default:
		err = retryJob(ctx, w.conn, w.keys, job.ID, token, job.Opts.LIFO, reason)
	}

	if err != nil {
		slog.Error("bullmq: failed to retry job", "job", job.ID, "error", err)
		w.emitError(err)
	}

	job.FailedReason = reason
	// A failed event is emitted for every failed attempt, as in Node.js.
	w.emit(WorkerEvent{Type: EventFailed, JobID: job.ID, Err: procErr})
}

func (w *Worker) backoffDelay(b *BackoffStrategy, attempt int, procErr error, job *Job) time.Duration {
	switch b.Type {
	case BackoffFixed:
		return b.Delay
	case BackoffExponential:
		if attempt > 62 {
			return time.Duration(math.MaxInt64)
		}

		return b.Delay * time.Duration(int64(1)<<(attempt-1))
	default:
		if w.opts.BackoffStrategy == nil {
			return 0
		}

		return w.opts.BackoffStrategy(attempt, b.Type, procErr, job)
	}
}

func (w *Worker) failPermanently(ctx context.Context, job *Job, token string, procErr error, fetchNext bool) *Job {
	reason := procErr.Error()
	res, err := moveToFinished(ctx, w.conn, w.keys, job.ID, StateFailed, reason, w.finishOpts(job, token, StateFailed), fetchNext)

	var next *Job

	if err != nil {
		slog.Error("bullmq: failed to move job to failed", "job", job.ID, "error", err)
		w.emitError(err)
	} else {
		next = w.chainedJob(res, fetchNext)
	}

	job.FailedReason = reason
	w.emit(WorkerEvent{Type: EventFailed, JobID: job.ID, Err: procErr})

	return next
}

// ── Background loops ────────────────────────────────────────────────────────

func (w *Worker) stalledCheckLoop() {
	defer w.loops.Done()

	ticker := time.NewTicker(w.opts.StalledInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.lifeCtx.Done():
			return
		case <-ticker.C:
		}

		if w.closing.Load() {
			return
		}

		if err := w.checkStalledJobs(w.lifeCtx); err != nil && !isClosedErr(err) {
			slog.Warn("bullmq: stalled check failed", "queue", w.keys.Name(), "error", err)
		}
	}
}

func (w *Worker) checkStalledJobs(ctx context.Context) error {
	k := w.keys

	res, err := scripts.Get("moveStalledJobsToWait").Run(ctx, w.conn.client,
		[]string{k.Stalled(), k.Wait(), k.Active(), k.StalledCheck(), k.Meta(), k.Paused(), k.Marker(), k.Events(), k.Repeat()},
		*w.opts.MaxStalledCount, k.KeyPrefix(), nowMillis(), w.opts.StalledInterval.Milliseconds())
	if err != nil {
		return err
	}

	for _, id := range stalledIDs(res) {
		w.emit(WorkerEvent{Type: EventStalled, JobID: id})
	}

	return nil
}

// stalledIDs accepts both a flat id list and a nested [[ids...]] reply.
func stalledIDs(res any) []string {
	arr, _ := replyArray(res)

	var ids []string

	for _, v := range arr {
		if s, ok := replyString(v); ok {
			ids = append(ids, s)
		} else if inner, ok := replyArray(v); ok {
			for _, x := range inner {
				if s, ok := replyString(x); ok {
					ids = append(ids, s)
				}
			}
		}
	}

	return ids
}

func (w *Worker) lockRenewalLoop() {
	defer w.loops.Done()

	ticker := time.NewTicker(w.opts.LockRenewTime)
	defer ticker.Stop()

	for {
		select {
		case <-w.lifeCtx.Done():
			return
		case <-ticker.C:
		}

		w.mu.Lock()

		jobs := make(map[string]string, len(w.active))
		for id, a := range w.active {
			jobs[id] = a.token
		}
		w.mu.Unlock()

		for id, token := range jobs {
			ok, err := extendLock(w.lifeCtx, w.conn, w.keys, id, token, w.opts.LockDuration)
			if err != nil && !isClosedErr(err) {
				slog.Warn("bullmq: lock extension failed", "job", id, "error", err)
			} else if err == nil && !ok {
				slog.Warn("bullmq: lock lost", "job", id)
			}
		}
	}
}
