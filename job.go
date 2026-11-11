package bullmq

import (
	"bullmq/internal/scripts"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"time"
)

// scriptContext binds a job to the queue it belongs to so that its methods
// can run Redis scripts.
type scriptContext struct {
	conn         *RedisConnection
	keys         QueueKeys
	token        string
	lockDuration time.Duration
	// onProgress forwards progress updates to the owning worker (may be nil).
	onProgress func(jobID string, progress json.RawMessage)
}

// Job is a unit of work stored in Redis as a hash.
//
// Jobs returned by a [Queue] or handed to a processor are bound to their
// queue, so their methods (UpdateProgress, Log, Retry, ...) work directly. A
// job is not safe for concurrent mutation; methods that update Redis also
// update the corresponding fields.
type Job struct {
	// ID is the job id.
	ID string
	// Name is the job name.
	Name string
	// Data is the JSON payload. Use [Job.DecodeData] to decode it.
	Data json.RawMessage
	// Opts are the job options.
	Opts JobOptions
	// Progress is the JSON-encoded progress (a number by default).
	Progress json.RawMessage
	// AttemptsMade is the number of failed or completed attempts.
	AttemptsMade int
	// AttemptsStarted is the number of times processing started.
	AttemptsStarted int
	// Timestamp is the creation time in Unix milliseconds.
	Timestamp int64
	// ProcessedOn is the time processing started, in Unix ms (0 if never).
	ProcessedOn int64
	// FinishedOn is the time the job finished, in Unix ms (0 if not finished).
	FinishedOn int64
	// FailedReason is the error message of the last failure.
	FailedReason string
	// Stacktrace is the raw stored stack trace list (JSON).
	Stacktrace string
	// ReturnValue is the JSON-encoded return value of the processor.
	ReturnValue string
	// Parent identifies the parent job, if any.
	Parent *ParentKeys
	// ParentKey is the parent's fully qualified job key, if any.
	ParentKey string
	// ProcessedBy is the name of the worker that processed the job.
	ProcessedBy string
	// StalledCounter counts stalled detections.
	StalledCounter int
	// Delay is the current delay.
	Delay time.Duration
	// RepeatJobKey is the id of the scheduler that produced the job.
	RepeatJobKey string
	// DeferredFailure is set when a child failed with FailParentOnFailure; the
	// worker fails the job without processing it.
	DeferredFailure string
	// QueueName is the name of the queue the job belongs to.
	QueueName string

	discarded *atomic.Bool
	sc        *scriptContext
}

// newJob builds a job that has not been added yet.
func newJob(name string, data json.RawMessage, opts JobOptions) *Job {
	ts := timeToMillis(opts.Timestamp)
	if ts == 0 {
		ts = nowMillis()
	}

	return &Job{
		Name:      name,
		Data:      data,
		Opts:      opts,
		Progress:  jsonZero,
		Timestamp: ts,
		Delay:     opts.Delay,
		discarded: new(atomic.Bool),
	}
}

// DecodeData unmarshals the job data into v.
func (j *Job) DecodeData(v any) error { return json.Unmarshal(j.Data, v) }

// DecodeReturnValue unmarshals the stored return value into v.
func (j *Job) DecodeReturnValue(v any) error { return json.Unmarshal([]byte(j.ReturnValue), v) }

// Priority returns the job priority.
func (j *Job) Priority() int { return j.Opts.Priority }

// Token returns the lock token of an active job (empty outside a worker).
func (j *Job) Token() string {
	if j.sc == nil {
		return ""
	}

	return j.sc.token
}

// QueueQualifiedName returns `{prefix}:{queueName}`, or "" for an unbound job.
func (j *Job) QueueQualifiedName() string {
	if j.sc == nil {
		return ""
	}

	return j.sc.keys.Base()
}

// Key returns the fully qualified job key, or "" for an unbound job.
func (j *Job) Key() string {
	if j.sc == nil {
		return ""
	}

	return j.sc.keys.JobKey(j.ID)
}

// Discard marks the job so that, if it fails, it is not retried even if
// attempts remain (Node.js Job.discard). Safe to call from the processor.
func (j *Job) Discard() { j.discardFlag().Store(true) }

// IsDiscarded reports whether [Job.Discard] was called.
func (j *Job) IsDiscarded() bool { return j.discardFlag().Load() }

func (j *Job) discardFlag() *atomic.Bool {
	if j.discarded == nil {
		j.discarded = new(atomic.Bool)
	}

	return j.discarded
}

func (j *Job) bind(sc *scriptContext) {
	j.sc = sc
	j.QueueName = sc.keys.Name()
}

func (j *Job) ctx() (*scriptContext, error) {
	if j.sc == nil {
		return nil, ErrNoScriptContext
	}

	return j.sc, nil
}

// ── Flow / dependency methods ───────────────────────────────────────────────

// GetChildrenValues returns the return values of the completed children,
// keyed by child job key. Values that are not valid JSON are kept as strings.
func (j *Job) GetChildrenValues(ctx context.Context) (map[string]any, error) {
	sc, err := j.ctx()
	if err != nil {
		return nil, err
	}

	return childrenValues(ctx, sc.conn, sc.keys.JobKey(j.ID))
}

// GetIgnoredChildrenFailures returns the failure reasons of children that
// failed with IgnoreDependencyOnFailure, keyed by child job key.
func (j *Job) GetIgnoredChildrenFailures(ctx context.Context) (map[string]string, error) {
	sc, err := j.ctx()
	if err != nil {
		return nil, err
	}

	return sc.conn.client.HGetAll(ctx, sc.keys.JobKey(j.ID)+":failed").Result()
}

// GetFailedChildrenValues is a deprecated alias of
// [Job.GetIgnoredChildrenFailures].
func (j *Job) GetFailedChildrenValues(ctx context.Context) (map[string]string, error) {
	return j.GetIgnoredChildrenFailures(ctx)
}

// GetDependenciesCount returns the number of children by type.
func (j *Job) GetDependenciesCount(ctx context.Context) (DependenciesCount, error) {
	sc, err := j.ctx()
	if err != nil {
		return DependenciesCount{}, err
	}

	return dependenciesCount(ctx, sc.conn, sc.keys.JobKey(j.ID))
}

// GetDependencies returns a page of processed and unprocessed children using
// HSCAN/SSCAN cursors (start at 0; a returned cursor of 0 means done).
func (j *Job) GetDependencies(ctx context.Context, processedCursor, unprocessedCursor uint64, count int64) (DependenciesResult, error) {
	sc, err := j.ctx()
	if err != nil {
		return DependenciesResult{}, err
	}

	jobKey := sc.keys.JobKey(j.ID)

	flat, nextProcessed, err := sc.conn.client.HScan(ctx, jobKey+":processed", processedCursor, "", count).Result()
	if err != nil {
		return DependenciesResult{}, err
	}

	processed := make(map[string]any, len(flat)/2)
	for i := 0; i+1 < len(flat); i += 2 {
		processed[flat[i]] = parseJSONOrString(flat[i+1])
	}

	unprocessed, nextUnprocessed, err := sc.conn.client.SScan(ctx, jobKey+":dependencies", unprocessedCursor, "", count).Result()
	if err != nil {
		return DependenciesResult{}, err
	}

	return DependenciesResult{
		Processed:             processed,
		NextProcessedCursor:   nextProcessed,
		Unprocessed:           unprocessed,
		NextUnprocessedCursor: nextUnprocessed,
	}, nil
}

// GetUnprocessedDependencies returns the keys of the children still pending.
func (j *Job) GetUnprocessedDependencies(ctx context.Context) ([]string, error) {
	sc, err := j.ctx()
	if err != nil {
		return nil, err
	}

	return sc.conn.client.SMembers(ctx, sc.keys.JobKey(j.ID)+":dependencies").Result()
}

// RemoveChildDependency removes this child's dependency from its parent. If
// it was the last pending dependency, the parent moves to wait. It reports
// whether the dependency was removed.
func (j *Job) RemoveChildDependency(ctx context.Context) (bool, error) {
	sc, err := j.ctx()
	if err != nil {
		return false, err
	}

	if j.ParentKey == "" {
		return false, invalidConfig("Job has no parent key")
	}

	removed, err := removeChildDependency(ctx, sc.conn, sc.keys, j.ID, j.ParentKey)
	if err == nil && removed {
		j.Parent = nil
		j.ParentKey = ""
	}

	return removed, err
}

// MoveToWaitingChildren moves an active job to waiting-children. childKey
// optionally names the child (`{prefix}:{queue}:{id}`) to wait for. It
// reports false when there are no pending dependencies. After a true result
// the processor must return [ErrWaitingChildren].
func (j *Job) MoveToWaitingChildren(ctx context.Context, childKey string) (bool, error) {
	sc, err := j.ctx()
	if err != nil {
		return false, err
	}

	k := sc.keys
	jobKey := k.JobKey(j.ID)

	res, err := scripts.Get("moveToWaitingChildren").Run(ctx, sc.conn.client,
		[]string{
			k.Active(), k.WaitingChildren(), jobKey, jobKey + ":dependencies",
			jobKey + ":unsuccessful", k.Stalled(), k.Events(),
		},
		sc.token, childKey, nowMillis(), j.ID, k.KeyPrefix())
	if err != nil {
		return false, err
	}

	code, _ := replyInt(res)
	switch code {
	case 0:
		return true, nil
	case 1:
		return false, nil
	case CodeJobNotExist:
		return false, fmt.Errorf("%w: %s", ErrJobNotFound, j.ID)
	case CodeJobLockNotExist:
		return false, fmt.Errorf("%w: job %s", ErrJobLockNotExist, j.ID)
	case CodeJobNotInState:
		return false, fmt.Errorf("%w: job %s is not in state active", ErrJobNotInState, j.ID)
	case CodeJobHasFailedChildren:
		return false, NewUnrecoverableError(fmt.Sprintf(
			"Cannot complete job %s because it has at least one failed child. moveToWaitingChildren", j.ID,
		))
	default:
		return false, fmt.Errorf("bullmq: moveToWaitingChildren returned unexpected code %d", code)
	}
}

// ── Script-backed methods ───────────────────────────────────────────────────

// UpdateProgress stores progress (any JSON value, typically a number) and
// emits a progress event.
func (j *Job) UpdateProgress(ctx context.Context, progress any) error {
	sc, err := j.ctx()
	if err != nil {
		return err
	}

	raw, err := marshalProgress(progress)
	if err != nil {
		return err
	}

	if err := updateProgress(ctx, sc.conn, sc.keys, j.ID, raw); err != nil {
		return err
	}

	j.Progress = raw
	if sc.onProgress != nil {
		sc.onProgress(j.ID, raw)
	}

	return nil
}

// UpdateData replaces the job data.
func (j *Job) UpdateData(ctx context.Context, data any) error {
	sc, err := j.ctx()
	if err != nil {
		return err
	}

	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("bullmq: marshal job data: %w", err)
	}

	res, err := scripts.Get("updateData").Run(ctx, sc.conn.client, []string{sc.keys.JobKey(j.ID)}, raw)
	if err != nil {
		return err
	}

	if code, ok := res.(int64); ok && code < 0 {
		return fmt.Errorf("%w: updateData %s", NewScriptError(code), j.ID)
	}

	j.Data = raw

	return nil
}

// Log appends a log row and returns the number of log entries.
func (j *Job) Log(ctx context.Context, row string) (int64, error) {
	sc, err := j.ctx()
	if err != nil {
		return 0, err
	}

	jobKey := sc.keys.JobKey(j.ID)

	var keepLogs string
	if j.Opts.KeepLogs > 0 {
		keepLogs = strconv.Itoa(j.Opts.KeepLogs)
	}

	res, err := scripts.Get("addLog").Run(ctx, sc.conn.client, []string{jobKey, jobKey + ":logs"}, j.ID, row, keepLogs)
	if err != nil {
		return 0, err
	}

	n, _ := replyInt(res)
	if n < 0 {
		return 0, NewScriptError(n)
	}

	return n, nil
}

// ClearLogs removes the job logs, keeping the keepLogs most recent entries
// when keepLogs > 0.
func (j *Job) ClearLogs(ctx context.Context, keepLogs int) error {
	sc, err := j.ctx()
	if err != nil {
		return err
	}

	logsKey := sc.keys.JobKey(j.ID) + ":logs"
	if keepLogs > 0 {
		return sc.conn.client.LTrim(ctx, logsKey, int64(-keepLogs), -1).Err()
	}

	return sc.conn.client.Del(ctx, logsKey).Err()
}

// Retry moves a completed or failed job back to wait. state is
// [StateFailed] or [StateCompleted].
func (j *Job) Retry(ctx context.Context, state JobState, opts *RetryOptions) error {
	sc, err := j.ctx()
	if err != nil {
		return err
	}

	if state != StateFailed && state != StateCompleted {
		return invalidConfig("retry state must be failed or completed, got %q", state)
	}

	var ro RetryOptions
	if opts != nil {
		ro = *opts
	}

	k := sc.keys

	pushCmd := "LPUSH"
	if j.Opts.LIFO {
		pushCmd = "RPUSH"
	}

	prop := "returnvalue"
	if state == StateFailed {
		prop = "failedReason"
	}

	res, err := scripts.Get("reprocessJob").Run(ctx, sc.conn.client,
		[]string{
			k.JobKey(j.ID), k.Events(), k.Get(string(state)), k.Wait(), k.Meta(),
			k.Active(), k.Marker(), k.Prioritized(), k.PC(),
		},
		j.ID, pushCmd, prop, string(state), boolArg(ro.ResetAttemptsMade), boolArg(ro.ResetAttemptsStarted))
	if err != nil {
		return err
	}

	code, _ := replyInt(res)
	if code != 1 {
		return retryError(code, j.ID, state)
	}

	j.FailedReason = ""
	j.FinishedOn = 0
	j.ProcessedOn = 0

	j.ReturnValue = ""
	if ro.ResetAttemptsMade {
		j.AttemptsMade = 0
	}

	if ro.ResetAttemptsStarted {
		j.AttemptsStarted = 0
	}

	return nil
}

func retryError(code int64, jobID string, state JobState) error {
	switch code {
	case CodeJobNotExist:
		return fmt.Errorf("%w: Missing key for job %s. reprocessJob", NewScriptError(code), jobID)
	case CodeJobNotInState:
		return fmt.Errorf("%w: Job %s is not in the %s state. reprocessJob", NewScriptError(code), jobID, state)
	default:
		return fmt.Errorf("%w: reprocessJob %s", NewScriptError(code), jobID)
	}
}

// GetState returns the current state of the job.
func (j *Job) GetState(ctx context.Context) (JobState, error) {
	sc, err := j.ctx()
	if err != nil {
		return StateUnknown, err
	}

	return jobState(ctx, sc.conn, sc.keys, j.ID)
}

// IsCompleted reports whether the job is completed.
func (j *Job) IsCompleted(ctx context.Context) (bool, error) { return j.isState(ctx, StateCompleted) }

// IsFailed reports whether the job failed.
func (j *Job) IsFailed(ctx context.Context) (bool, error) { return j.isState(ctx, StateFailed) }

// IsActive reports whether the job is being processed.
func (j *Job) IsActive(ctx context.Context) (bool, error) { return j.isState(ctx, StateActive) }

// IsWaiting reports whether the job is waiting.
func (j *Job) IsWaiting(ctx context.Context) (bool, error) { return j.isState(ctx, StateWaiting) }

// IsDelayed reports whether the job is delayed.
func (j *Job) IsDelayed(ctx context.Context) (bool, error) { return j.isState(ctx, StateDelayed) }

// IsWaitingChildren reports whether the job waits for its children.
func (j *Job) IsWaitingChildren(ctx context.Context) (bool, error) {
	return j.isState(ctx, StateWaitingChildren)
}

func (j *Job) isState(ctx context.Context, want JobState) (bool, error) {
	s, err := j.GetState(ctx)
	return s == want, err
}

// ExtendLock extends the lock of an active job held with token.
func (j *Job) ExtendLock(ctx context.Context, token string, d time.Duration) error {
	sc, err := j.ctx()
	if err != nil {
		return err
	}

	ok, err := extendLock(ctx, sc.conn, sc.keys, j.ID, token, d)
	if err != nil {
		return err
	}

	if !ok {
		return fmt.Errorf("%w: could not extend lock for job %s", ErrJobLocked, j.ID)
	}

	return nil
}

// Promote moves a delayed job to wait immediately.
func (j *Job) Promote(ctx context.Context) error {
	sc, err := j.ctx()
	if err != nil {
		return err
	}

	k := sc.keys

	res, err := scripts.Get("promote").Run(ctx, sc.conn.client,
		[]string{k.Delayed(), k.Wait(), k.Paused(), k.Meta(), k.Prioritized(), k.Active(), k.PC(), k.Events(), k.Marker()},
		k.KeyPrefix(), j.ID)
	if err != nil {
		return err
	}

	if err := scriptCodeError(res); err != nil {
		return fmt.Errorf("%w: promote %s", err, j.ID)
	}

	j.Delay = 0

	return nil
}

// ChangeDelay sets a new delay (from now) on a delayed job.
func (j *Job) ChangeDelay(ctx context.Context, delay time.Duration) error {
	sc, err := j.ctx()
	if err != nil {
		return err
	}

	k := sc.keys

	res, err := scripts.Get("changeDelay").Run(ctx, sc.conn.client,
		[]string{k.Delayed(), k.Meta(), k.Marker(), k.Events()},
		delay.Milliseconds(), nowMillis(), j.ID, k.JobKey(j.ID))
	if err != nil {
		return err
	}

	if err := scriptCodeError(res); err != nil {
		return fmt.Errorf("%w: changeDelay %s", err, j.ID)
	}

	j.Delay = delay

	return nil
}

// ChangePriority changes the priority of a waiting job (0 = no priority).
func (j *Job) ChangePriority(ctx context.Context, priority int, lifo bool) error {
	sc, err := j.ctx()
	if err != nil {
		return err
	}

	k := sc.keys

	res, err := scripts.Get("changePriority").Run(ctx, sc.conn.client,
		[]string{k.Wait(), k.Paused(), k.Meta(), k.Prioritized(), k.Active(), k.PC(), k.Marker()},
		priority, k.KeyPrefix(), j.ID, boolArg(lifo))
	if err != nil {
		return err
	}

	if err := scriptCodeError(res); err != nil {
		return fmt.Errorf("%w: changePriority %s", err, j.ID)
	}

	j.Opts.Priority = priority

	return nil
}

// MoveToCompleted completes a job fetched manually with [Worker.GetNextJob],
// storing returnValue (JSON-encoded).
func (j *Job) MoveToCompleted(ctx context.Context, returnValue any) error {
	raw, err := json.Marshal(returnValue)
	if err != nil {
		return fmt.Errorf("bullmq: marshal return value: %w", err)
	}

	if err := j.moveToFinished(ctx, StateCompleted, string(raw)); err != nil {
		return err
	}

	j.ReturnValue = string(raw)

	return nil
}

// MoveToFailed fails a job fetched manually with [Worker.GetNextJob].
func (j *Job) MoveToFailed(ctx context.Context, reason string) error {
	if err := j.moveToFinished(ctx, StateFailed, reason); err != nil {
		return err
	}

	j.FailedReason = reason

	return nil
}

func (j *Job) moveToFinished(ctx context.Context, target JobState, value string) error {
	sc, err := j.ctx()
	if err != nil {
		return err
	}

	opts := &finishOpts{
		token:        sc.token,
		keep:         nil, // keep every job
		lockDuration: sc.lockDuration,
		attempts:     j.Opts.Attempts,
	}
	if _, err := moveToFinished(ctx, sc.conn, sc.keys, j.ID, target, value, opts, false); err != nil {
		return err
	}

	j.AttemptsMade++
	j.FinishedOn = nowMillis()

	return nil
}

// MoveToDelayed moves an active job to the delayed set until timestamp. The
// processor must then return [ErrDelayed].
func (j *Job) MoveToDelayed(ctx context.Context, timestamp time.Time) error {
	sc, err := j.ctx()
	if err != nil {
		return err
	}

	delay := max(time.Until(timestamp), 0)

	return moveToDelayed(ctx, sc.conn, sc.keys, j.ID, sc.token, delay, true, sc.lockDuration, "")
}

// MoveToWait moves an active job back to wait (or prioritized). token
// defaults to the job's lock token. It returns the rate limiter TTL (0 when
// not rate limited).
func (j *Job) MoveToWait(ctx context.Context, token string) (time.Duration, error) {
	sc, err := j.ctx()
	if err != nil {
		return 0, err
	}

	if token == "" {
		token = sc.token
	}

	return moveJobFromActiveToWait(ctx, sc.conn, sc.keys, j.ID, token)
}

// ── Serialization ───────────────────────────────────────────────────────────

// Shared read-only defaults: the hot path builds one job per fetch, so the
// constant JSON values are not re-allocated for every job.
var (
	jsonNull = json.RawMessage("null")
	jsonZero = json.RawMessage("0")
)

// jobFromHash builds a job from its Redis hash fields.
func jobFromHash(id string, f map[string]string) *Job {
	j := &Job{ID: id, Data: jsonNull, Progress: jsonZero}

	var hasDelay bool
	for k, v := range f {
		hasDelay = j.setField(k, v) || hasDelay
	}

	j.finishDecode(hasDelay)

	return j
}

// jobFromReply builds a job from a flat [field, value, ...] HGETALL reply
// without an intermediate map (it runs for every fetched job). It returns
// nil when the reply holds no fields.
func jobFromReply(id string, reply any) *Job {
	arr, _ := replyArray(reply)
	if len(arr) < 2 {
		return nil
	}

	j := &Job{ID: id, Data: jsonNull, Progress: jsonZero}

	var hasDelay bool

	for i := 0; i+1 < len(arr); i += 2 {
		k, ok := replyString(arr[i])
		if !ok {
			continue
		}

		v, _ := replyString(arr[i+1])
		hasDelay = j.setField(k, v) || hasDelay
	}

	j.finishDecode(hasDelay)

	return j
}

// setField decodes one hash field. It reports whether the field was "delay".
func (j *Job) setField(k, v string) bool {
	if j.setTextField(k, v) {
		return false
	}

	switch k {
	case "timestamp":
		j.Timestamp = parseInt64(v)
	case "atm":
		j.AttemptsMade = int(parseInt64(v))
	case "ats":
		j.AttemptsStarted = int(parseInt64(v))
	case "processedOn":
		j.ProcessedOn = parseInt64(v)
	case "finishedOn":
		j.FinishedOn = parseInt64(v)
	case "stc":
		j.StalledCounter = int(parseInt64(v))
	case "delay":
		j.Delay = time.Duration(parseInt64(v)) * time.Millisecond

		return true
	}

	return false
}

// setTextField decodes the string and JSON fields; it reports whether k was
// one of them.
func (j *Job) setTextField(k, v string) bool {
	switch k {
	case "name":
		j.Name = v
	case "data":
		if v != "" && json.Valid([]byte(v)) {
			j.Data = json.RawMessage(v)
		}
	case "opts":
		if v != "" {
			_ = json.Unmarshal([]byte(v), &j.Opts) // tolerate options from other implementations
		}
	case "progress":
		if v != "" && json.Valid([]byte(v)) {
			j.Progress = json.RawMessage(v)
		}
	case "parent":
		var pk ParentKeys
		if v != "" && json.Unmarshal([]byte(v), &pk) == nil {
			j.Parent = &pk
		}
	case "failedReason":
		j.FailedReason = v
	case "stacktrace":
		j.Stacktrace = v
	case "returnvalue":
		j.ReturnValue = v
	case "parentKey":
		j.ParentKey = v
	case "pb":
		j.ProcessedBy = v
	case "rjk":
		j.RepeatJobKey = v
	case "defa":
		j.DeferredFailure = v
	default:
		return false
	}

	return true
}

// finishDecode applies the fallbacks that depend on the decoded options.
func (j *Job) finishDecode(hasDelay bool) {
	if !hasDelay {
		j.Delay = j.Opts.Delay
	}

	if j.RepeatJobKey == "" {
		j.RepeatJobKey = j.Opts.RepeatJobKey
	}
}

// ToRedisHash serializes the job into hash fields (as stored by the scripts).
func (j *Job) ToRedisHash() map[string]string {
	opts, _ := json.Marshal(j.Opts)

	f := map[string]string{
		"name":      j.Name,
		"data":      string(j.Data),
		"opts":      string(opts),
		"timestamp": strconv.FormatInt(j.Timestamp, 10),
		"delay":     strconv.FormatInt(j.Delay.Milliseconds(), 10),
		"priority":  strconv.Itoa(j.Opts.Priority),
		"atm":       strconv.Itoa(j.AttemptsMade),
		"ats":       strconv.Itoa(j.AttemptsStarted),
		"stc":       strconv.Itoa(j.StalledCounter),
		"progress":  string(j.Progress),
	}
	if j.Parent != nil {
		p, _ := json.Marshal(j.Parent)
		f["parent"] = string(p)
	}

	if j.ParentKey != "" {
		f["parentKey"] = j.ParentKey
	}

	if j.Opts.Deduplication != nil {
		f["deid"] = j.Opts.Deduplication.ID
	}

	return f
}

// fetchJob loads a job by id; it returns (nil, nil) when it does not exist.
func fetchJob(ctx context.Context, conn *RedisConnection, keys QueueKeys, id string) (*Job, error) {
	f, err := conn.client.HGetAll(ctx, keys.JobKey(id)).Result()
	if err != nil {
		return nil, err
	}

	if len(f) == 0 {
		return nil, nil
	}

	return jobFromHash(id, f), nil
}

func parseInt64(s string) int64 {
	if s == "" {
		return 0
	}

	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}

	if f, err := strconv.ParseFloat(s, 64); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
		return int64(f)
	}

	return 0
}

// parseJSONOrString decodes a JSON value, keeping the raw string when it is
// not valid JSON.
func parseJSONOrString(s string) any {
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return s
	}

	return v
}

// marshalProgress encodes progress like Node.js: non-finite numbers become
// null.
func marshalProgress(progress any) (json.RawMessage, error) {
	switch p := progress.(type) {
	case json.RawMessage:
		return p, nil
	case float64:
		if math.IsNaN(p) || math.IsInf(p, 0) {
			return json.RawMessage("null"), nil
		}
	case float32:
		if math.IsNaN(float64(p)) || math.IsInf(float64(p), 0) {
			return json.RawMessage("null"), nil
		}
	}

	raw, err := json.Marshal(progress)
	if err != nil {
		return nil, fmt.Errorf("bullmq: marshal progress: %w", err)
	}

	return raw, nil
}

func boolArg(b bool) string {
	if b {
		return "1"
	}

	return "0"
}
