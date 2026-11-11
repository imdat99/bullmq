package bullmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/imdat99/bullmq/internal/msgpack"
	"github.com/imdat99/bullmq/internal/scripts"
)

// This file holds the script invocations shared by Job, Queue and Worker.

func childrenValues(ctx context.Context, conn *RedisConnection, jobKey string) (map[string]any, error) {
	raw, err := conn.client.HGetAll(ctx, jobKey+":processed").Result()
	if err != nil {
		return nil, err
	}

	out := make(map[string]any, len(raw))
	for k, v := range raw {
		out[k] = parseJSONOrString(v)
	}

	return out, nil
}

func dependenciesCount(ctx context.Context, conn *RedisConnection, jobKey string) (DependenciesCount, error) {
	pipe := conn.client.Pipeline()
	processed := pipe.HLen(ctx, jobKey+":processed")
	unprocessed := pipe.SCard(ctx, jobKey+":dependencies")
	ignored := pipe.HLen(ctx, jobKey+":failed")

	failed := pipe.ZCard(ctx, jobKey+":unsuccessful")
	if _, err := pipe.Exec(ctx); err != nil {
		return DependenciesCount{}, err
	}

	return DependenciesCount{
		Processed:   processed.Val(),
		Unprocessed: unprocessed.Val(),
		Ignored:     ignored.Val(),
		Failed:      failed.Val(),
	}, nil
}

func removeChildDependency(ctx context.Context, conn *RedisConnection, keys QueueKeys, jobID, parentKey string) (bool, error) {
	res, err := scripts.Get("removeChildDependency").Run(ctx, conn.client,
		[]string{keys.KeyPrefix()}, keys.JobKey(jobID), parentKey)
	if err != nil {
		return false, err
	}

	code, _ := replyInt(res)
	switch code {
	case 0:
		return true, nil
	case CodeJobNotExist:
		return false, fmt.Errorf("%w: Missing key for job %s. removeChildDependency", NewScriptError(code), jobID)
	case CodeParentJobNotExist:
		return false, fmt.Errorf("%w: Missing key for parent job %s. removeChildDependency", NewScriptError(code), parentKey)
	default:
		return false, nil
	}
}

func updateProgress(ctx context.Context, conn *RedisConnection, keys QueueKeys, jobID string, progress json.RawMessage) error {
	res, err := scripts.Get("updateProgress").Run(ctx, conn.client,
		[]string{keys.JobKey(jobID), keys.Events(), keys.Meta()}, jobID, []byte(progress))
	if err != nil {
		return err
	}

	if err := scriptCodeError(res); err != nil {
		return fmt.Errorf("%w: updateProgress %s", err, jobID)
	}

	return nil
}

func jobState(ctx context.Context, conn *RedisConnection, keys QueueKeys, jobID string) (JobState, error) {
	res, err := scripts.Get("getState").Run(ctx, conn.client,
		[]string{
			keys.Completed(), keys.Failed(), keys.Delayed(), keys.Active(), keys.Wait(),
			keys.Paused(), keys.WaitingChildren(), keys.Prioritized(),
		},
		jobID)
	if err != nil {
		return StateUnknown, err
	}

	s, _ := replyString(res)

	return parseJobState(s), nil
}

func extendLock(ctx context.Context, conn *RedisConnection, keys QueueKeys, jobID, token string, d time.Duration) (bool, error) {
	res, err := scripts.Get("extendLock").Run(ctx, conn.client,
		[]string{keys.JobKey(jobID) + ":lock", keys.Stalled()}, token, d.Milliseconds(), jobID)
	if err != nil {
		return false, err
	}

	n, _ := replyInt(res)

	return n == 1, nil
}

// finishOpts are the options packed for moveToFinished (and the next-job
// fetch it may chain).
type finishOpts struct {
	token          string
	keep           *RemoveOnFinish // nil keeps every job
	lockDuration   time.Duration
	attempts       int
	maxMetricsSize int // 0 disables metrics
	name           string
	limiter        *RateLimiterOptions
}

func (o *finishOpts) pack() []byte {
	var m msgpack.Map
	m.PutStr("token", o.token)
	putKeepJobs(&m, "keepJobs", o.keep)
	m.PutInt("lockDuration", o.lockDuration.Milliseconds())
	m.PutInt("attempts", int64(o.attempts))

	if o.maxMetricsSize > 0 {
		m.PutStr("maxMetricsSize", strconv.Itoa(o.maxMetricsSize))
	} else {
		m.PutStr("maxMetricsSize", "")
	}

	for _, k := range []string{"fpof", "cpof", "idof", "rdof"} {
		m.PutBool(k, false)
	}

	if o.name != "" {
		m.PutStr("name", o.name)
	}

	if o.limiter != nil {
		putLimiter(&m, o.limiter)
	}

	return m.Bytes()
}

// putKeepJobs writes the keepJobs map consumed by moveToFinished.
func putKeepJobs(m *msgpack.Map, key string, p *RemoveOnFinish) {
	switch {
	case p == nil, p.kind == removeBool && p.count != 1:
		m.PutMap(key, 1, func(kv msgpack.KV) { kv.Int("count", -1) })
	case p.kind == removeBool:
		m.PutMap(key, 1, func(kv msgpack.KV) { kv.Int("count", 0) })
	case p.kind == removeCount:
		m.PutMap(key, 1, func(kv msgpack.KV) { kv.Int("count", p.count) })
	default:
		k := p.keep
		n := countSet(k.Age, k.Count, k.Limit)
		m.PutMap(key, n, func(kv msgpack.KV) {
			if k.Age != nil {
				kv.Int("age", *k.Age)
			}

			if k.Count != nil {
				kv.Int("count", *k.Count)
			}

			if k.Limit != nil {
				kv.Int("limit", *k.Limit)
			}
		})
	}
}

func countSet(ps ...*int64) int {
	n := 0

	for _, p := range ps {
		if p != nil {
			n++
		}
	}

	return n
}

// putLimiter writes the {max, duration} limiter map.
func putLimiter(m *msgpack.Map, l *RateLimiterOptions) {
	m.PutMap("limiter", 2, func(kv msgpack.KV) {
		kv.Int("max", l.Max)
		kv.Int("duration", l.Duration.Milliseconds())
	})
}

// moveToFinished runs the moveToFinished script. When fetchNext is set the
// raw reply may carry the next job (same shape as moveToActive).
func moveToFinished(ctx context.Context, conn *RedisConnection, keys QueueKeys, jobID string,
	target JobState, value string, opts *finishOpts, fetchNext bool,
) (any, error) {
	// Hot path (one call per job): pre-boxed keys and constants, see keyCache.
	kc := keys.cache()
	targetKey, metricsKey, field, targetArg := kc.boxed.completed, kc.boxed.metricsCompleted, argReturnValue, argCompleted

	if target == StateFailed {
		targetKey, metricsKey, field, targetArg = kc.boxed.failed, kc.boxed.metricsFailed, argFailedReason, argFailed
	}

	s := scripts.Get("moveToFinished")
	args := append(s.CallArgs(len(kc.moveToFinishedBase)+4+9), kc.moveToFinishedBase...)
	args = append(args,
		targetKey, keys.JobKey(jobID), metricsKey, kc.boxed.marker,
		jobID, nowMillis(), field, value, targetArg, scripts.BoxedBool(fetchNext), kc.boxed.keyPrefix, opts.pack(), scripts.ArgEmpty)

	res, err := s.RunArgs(ctx, conn.client, args)
	if err != nil {
		return nil, err
	}

	if err := scriptCodeError(res); err != nil {
		return nil, err
	}

	return res, nil
}

// Pre-boxed moveToFinished constants.
var (
	argReturnValue  any = "returnvalue"
	argFailedReason any = "failedReason"
	argCompleted    any = string(StateCompleted)
	argFailed       any = string(StateFailed)
)

// finishedError maps a moveToFinished code to a descriptive error, mirroring
// Node.js Scripts.finishedErrors.
func finishedError(code int64, jobID, command, state string) error {
	se := NewScriptError(code)
	switch code {
	case CodeJobNotExist:
		return &detailedScriptError{se, fmt.Sprintf("Missing key for job %s. %s", jobID, command)}
	case CodeJobLockNotExist:
		return &detailedScriptError{se, fmt.Sprintf("Missing lock for job %s. %s", jobID, command)}
	case CodeJobNotInState:
		return &detailedScriptError{se, fmt.Sprintf("Job %s is not in the %s state. %s", jobID, state, command)}
	case CodeJobPendingDependencies:
		return &detailedScriptError{se, fmt.Sprintf("Job %s has pending dependencies. %s", jobID, command)}
	case CodeJobLockMismatch:
		return &detailedScriptError{se, fmt.Sprintf("Lock mismatch for job %s. Cmd %s from %s", jobID, command, state)}
	case CodeJobHasFailedChildren:
		return NewUnrecoverableError(fmt.Sprintf(
			"Cannot complete job %s because it has at least one failed child. %s", jobID, command,
		))
	default:
		return se
	}
}

// detailedScriptError carries a descriptive message (stored verbatim as a
// job's failedReason, like Node.js) while still unwrapping to the
// [ScriptError] so [ScriptErrorCode] and errors.As keep working.
type detailedScriptError struct {
	se  *ScriptError
	msg string
}

func (e *detailedScriptError) Error() string { return e.msg }
func (e *detailedScriptError) Unwrap() error { return e.se }

func packTokenOpts(token string, lockDuration time.Duration) []byte {
	var m msgpack.Map
	m.PutStr("token", token)
	m.PutInt("lockDuration", lockDuration.Milliseconds())

	return m.Bytes()
}

// moveToDelayed moves an active job to delayed. skipAttempt leaves
// attemptsMade unchanged (manual MoveToDelayed); retries count the attempt.
func moveToDelayed(ctx context.Context, conn *RedisConnection, keys QueueKeys, jobID, token string,
	delay time.Duration, skipAttempt bool, lockDuration time.Duration, failedReason string,
) error {
	res, err := scripts.Get("moveToDelayed").Run(ctx, conn.client,
		[]string{
			keys.Marker(), keys.Active(), keys.Prioritized(), keys.Delayed(), keys.JobKey(jobID),
			keys.Events(), keys.Meta(), keys.Stalled(), keys.Wait(), keys.Limiter(), keys.PC(),
		},
		keys.KeyPrefix(), nowMillis(), jobID, token, delay.Milliseconds(), boolArg(skipAttempt),
		packFailedReason(failedReason), "0",
		packTokenOpts(token, lockDuration))
	if err != nil {
		return err
	}

	if code, ok := res.(int64); ok && code < 0 {
		return finishedError(code, jobID, "moveToDelayed", "active")
	}

	return nil
}

// packFailedReason packs the optional job fields updated by retryJob and
// moveToDelayed ([field, value, ...]); empty when there is nothing to store.
func packFailedReason(reason string) []byte {
	if reason == "" {
		return nil
	}

	var b msgpack.Buf
	b.ArrayLen(2)
	b.Str("failedReason")
	b.Str(reason)

	return b.Bytes()
}

// retryJob moves a failed active job straight back to wait.
func retryJob(ctx context.Context, conn *RedisConnection, keys QueueKeys, jobID, token string, lifo bool, failedReason string) error {
	pushCmd := "LPUSH"
	if lifo {
		pushCmd = "RPUSH"
	}

	res, err := scripts.Get("retryJob").Run(ctx, conn.client,
		[]string{
			keys.Active(), keys.Wait(), keys.Paused(), keys.JobKey(jobID), keys.Meta(),
			keys.Events(), keys.Delayed(), keys.Prioritized(), keys.PC(), keys.Marker(), keys.Stalled(),
		},
		keys.KeyPrefix(), nowMillis(), pushCmd, jobID, token, packFailedReason(failedReason))
	if err != nil {
		return err
	}

	if code, ok := res.(int64); ok && code < 0 {
		return finishedError(code, jobID, "retryJob", "active")
	}

	return nil
}

func moveJobFromActiveToWait(ctx context.Context, conn *RedisConnection, keys QueueKeys, jobID, token string) (time.Duration, error) {
	res, err := scripts.Get("moveJobFromActiveToWait").Run(ctx, conn.client,
		[]string{
			keys.Active(), keys.Wait(), keys.Stalled(), keys.Paused(), keys.Meta(),
			keys.Limiter(), keys.Prioritized(), keys.Marker(), keys.Events(),
		},
		jobID, token, keys.JobKey(jobID))
	if err != nil {
		return 0, err
	}

	if err := scriptCodeError(res); err != nil {
		return 0, fmt.Errorf("%w: moveJobFromActiveToWait %s", err, jobID)
	}

	pttl, _ := replyInt(res)

	return time.Duration(max(pttl, 0)) * time.Millisecond, nil
}

// fetchResult is the outcome of a moveToActive call.
type fetchResult struct {
	job         *Job
	nextDelayed int64         // timestamp (ms) of the next delayed job
	rateLimited time.Duration // rate limiter TTL
}

// parseMoveToActive parses `[jobHash, jobId, rateLimitTTL, nextTimestamp]`.
func parseMoveToActive(res any) (fetchResult, error) {
	if code, ok := res.(int64); ok && code < 0 {
		return fetchResult{}, NewScriptError(code)
	}

	arr, ok := replyArray(res)
	if !ok || len(arr) < 2 {
		return fetchResult{}, nil
	}

	var out fetchResult

	if len(arr) > 2 {
		if ttl, _ := replyInt(arr[2]); ttl > 0 {
			out.rateLimited = time.Duration(ttl) * time.Millisecond
		}
	}

	if len(arr) > 3 {
		out.nextDelayed, _ = replyInt(arr[3])
	}

	id, _ := replyString(arr[1])

	if id == "" || id == "0" {
		return out, nil
	}

	out.job = jobFromReply(id, arr[0])

	return out, nil
}

func moveToActive(ctx context.Context, conn *RedisConnection, keys QueueKeys, token string, o *WorkerOptions) (fetchResult, error) {
	var m msgpack.Map
	m.PutStr("token", token)
	m.PutInt("lockDuration", o.LockDuration.Milliseconds())

	if o.Name != "" {
		m.PutStr("name", o.Name)
	}

	if o.Limiter != nil {
		putLimiter(&m, o.Limiter)
	}

	// Hot path (one call per fetch): pre-boxed keys, see keyCache.
	kc := keys.cache()
	s := scripts.Get("moveToActive")
	args := append(s.CallArgs(len(kc.moveToActiveKeys)+3), kc.moveToActiveKeys...)
	args = append(args, kc.boxed.keyPrefix, nowMillis(), m.Bytes())

	res, err := s.RunArgs(ctx, conn.client, args)
	if err != nil {
		return fetchResult{}, err
	}

	return parseMoveToActive(res)
}

// ── Option packing shared by Queue and the job scheduler ────────────────────

func packRemoveOnFinish(r *RemoveOnFinish) []byte {
	var b msgpack.Buf

	switch r.kind {
	case removeCount:
		b.Int(r.count)
	case removeOptions:
		var m msgpack.Map
		if r.keep.Age != nil {
			m.PutInt("age", *r.keep.Age)
		}

		if r.keep.Count != nil {
			m.PutInt("count", *r.keep.Count)
		}

		if r.keep.Limit != nil {
			m.PutInt("limit", *r.keep.Limit)
		}

		m.Encode(&b)
	default:
		b.Bool(r.count == 1)
	}

	return b.Bytes()
}

func packBackoff(s *BackoffStrategy) []byte {
	var m msgpack.Map
	m.PutStr("type", s.Type)

	if s.IsCustom() {
		m.PutInt("delay", 0)
	} else {
		m.PutInt("delay", s.Delay.Milliseconds())
	}

	return m.Bytes()
}

func packDeduplication(d *DeduplicationOptions) []byte {
	var m msgpack.Map
	m.PutStr("id", d.ID)

	if d.TTL > 0 {
		m.PutInt("ttl", d.TTL.Milliseconds())
	}

	if d.Extend {
		m.PutBool("extend", true)
	}

	if d.Replace {
		m.PutBool("replace", true)
	}

	if d.Reschedule {
		m.PutBool("reschedule", true)
	}

	if d.KeepLastIfActive {
		m.PutBool("keepLastIfActive", true)
	}

	return m.Bytes()
}

// putTemplateOpts adds the options shared by scheduler templates and
// scheduler-produced jobs.
func putTemplateOpts(m *msgpack.Map, o *JobOptions) {
	if o.Attempts > 0 {
		m.PutInt("attempts", int64(o.Attempts))
	}

	if o.Backoff != nil {
		m.PutRaw("backoff", packBackoff(o.Backoff))
	}

	if o.RemoveOnComplete != nil {
		m.PutRaw("removeOnComplete", packRemoveOnFinish(o.RemoveOnComplete))
	}

	if o.RemoveOnFail != nil {
		m.PutRaw("removeOnFail", packRemoveOnFinish(o.RemoveOnFail))
	}

	if o.Priority > 0 {
		m.PutInt("priority", int64(o.Priority))
	}
}
