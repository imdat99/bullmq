package bullmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/imdat99/bullmq/internal/msgpack"
	"github.com/imdat99/bullmq/internal/scripts"
)

// Version is the library version recorded in the queue meta hash.
const Version = "bullmq-go:0.1.0"

// getJobsMaxBackfillIterations bounds the getJobs script's backfill of ids
// whose hashes are missing.
const getJobsMaxBackfillIterations = 5

// maxSafeInteger is JavaScript's Number.MAX_SAFE_INTEGER.
const maxSafeInteger = "9007199254740991"

// Queue adds jobs to a queue and manages its state. It is safe for
// concurrent use.
type Queue struct {
	name        string
	keys        QueueKeys
	conn        *RedisConnection
	ownsConn    bool
	defaultOpts JobOptions
	sc          *scriptContext // shared by every job the queue returns
}

// NewQueue connects to Redis and returns a queue. A nil opts selects the
// defaults.
func NewQueue(ctx context.Context, name string, opts *QueueOptions) (*Queue, error) {
	var o QueueOptions
	if opts != nil {
		o = *opts
	}

	if err := validateQueueArgs(name, &o); err != nil {
		return nil, err
	}

	conn, err := NewRedisConnection(ctx, o.Connection)
	if err != nil {
		return nil, err
	}

	q, err := newQueue(ctx, name, conn, o)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	q.ownsConn = true

	return q, nil
}

// NewQueueWithConnection returns a queue that shares conn. Closing the queue
// does not close conn.
func NewQueueWithConnection(ctx context.Context, name string, conn *RedisConnection, opts *QueueOptions) (*Queue, error) {
	var o QueueOptions
	if opts != nil {
		o = *opts
	}

	if err := validateQueueArgs(name, &o); err != nil {
		return nil, err
	}

	return newQueue(ctx, name, conn, o)
}

func validateQueueArgs(name string, o *QueueOptions) error {
	if o.Prefix == "" {
		o.Prefix = DefaultPrefix
	}

	if err := validateQueueName(name); err != nil {
		return err
	}

	return validatePrefix(o.Prefix)
}

func newQueue(ctx context.Context, name string, conn *RedisConnection, o QueueOptions) (*Queue, error) {
	q := &Queue{
		name:        name,
		keys:        NewQueueKeys(name, o.Prefix),
		conn:        conn,
		defaultOpts: o.DefaultJobOptions,
	}
	q.sc = q.scriptContextFor(q.keys)

	if err := conn.client.HSet(ctx, q.keys.Meta(), "library", Version).Err(); err != nil {
		return nil, fmt.Errorf("bullmq: update queue meta: %w", err)
	}

	return q, nil
}

// Name returns the queue name.
func (q *Queue) Name() string { return q.name }

// Keys returns the queue key builder.
func (q *Queue) Keys() QueueKeys { return q.keys }

// Connection returns the Redis connection.
func (q *Queue) Connection() *RedisConnection { return q.conn }

// Client returns the underlying go-redis client.
func (q *Queue) Client() redis.UniversalClient { return q.conn.client }

// Close releases the connection when the queue owns it.
func (q *Queue) Close() error {
	if q.ownsConn {
		return q.conn.Close()
	}

	return nil
}

// scriptContext returns the queue's shared, immutable script context.
func (q *Queue) scriptContext() *scriptContext { return q.sc }

func (q *Queue) scriptContextFor(keys QueueKeys) *scriptContext {
	return &scriptContext{conn: q.conn, keys: keys}
}

// ── Adding jobs ─────────────────────────────────────────────────────────────

// Add adds a job. data is JSON-encoded; opts may be nil.
func (q *Queue) Add(ctx context.Context, name string, data any, opts *JobOptions) (*Job, error) {
	var o JobOptions
	if opts != nil {
		o = *opts
	}

	raw, err := marshalData(data)
	if err != nil {
		return nil, err
	}

	job := newJob(name, raw, q.mergeJobOptions(o))
	if err := q.validateJob(job); err != nil {
		return nil, err
	}

	script, args, err := q.addJobCall(job)
	if err != nil {
		return nil, err
	}

	res, err := script.RunArgs(ctx, q.conn.client, args)
	if err != nil {
		return nil, err
	}

	id, err := parseAddedJobID(res)
	if err != nil {
		return nil, err
	}

	job.ID = id
	job.bind(q.scriptContext())

	return job, nil
}

// BulkJob is one job of [Queue.AddBulk].
type BulkJob struct {
	Name string
	Data any
	Opts *JobOptions
}

// AddBulk adds several jobs in a single pipeline round trip. Validation
// happens before anything is sent, so an invalid job adds nothing. Jobs are
// not added atomically: on a Redis error some may have been added.
func (q *Queue) AddBulk(ctx context.Context, jobs []BulkJob) ([]*Job, error) {
	if len(jobs) == 0 {
		return nil, nil
	}

	out := make([]*Job, len(jobs))

	type call struct {
		script *scripts.Script
		args   []any
	}

	calls := make([]call, len(jobs))
	for i, bj := range jobs {
		var o JobOptions
		if bj.Opts != nil {
			o = *bj.Opts
		}

		raw, err := marshalData(bj.Data)
		if err != nil {
			return nil, err
		}

		job := newJob(bj.Name, raw, q.mergeJobOptions(o))
		if err := q.validateJob(job); err != nil {
			return nil, err
		}

		s, args, err := q.addJobCall(job)
		if err != nil {
			return nil, err
		}

		out[i] = job
		calls[i] = call{s, args}
	}

	// EVALSHA in a pipeline has no NOSCRIPT fallback: load the scripts first.
	loaded := map[*scripts.Script]bool{}
	for _, c := range calls {
		if !loaded[c.script] {
			if err := c.script.Load(ctx, q.conn.client); err != nil {
				return nil, fmt.Errorf("bullmq: load script %s: %w", c.script.Name(), err)
			}

			loaded[c.script] = true
		}
	}

	pipe := q.conn.client.Pipeline()

	cmds := make([]*redis.Cmd, len(calls))
	for i, c := range calls {
		cmds[i] = pipe.Do(ctx, c.args...)
	}

	_, _ = pipe.Exec(ctx) // per-command errors are inspected below
	sc := q.scriptContext()

	for i, cmd := range cmds {
		res, err := cmd.Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("bullmq: add bulk job %d: %w", i, err)
		}

		id, err := parseAddedJobID(res)
		if err != nil {
			return nil, err
		}

		out[i].ID = id
		out[i].bind(sc)
	}

	return out, nil
}

func marshalData(data any) (json.RawMessage, error) {
	if raw, ok := data.(json.RawMessage); ok {
		if len(raw) == 0 {
			return json.RawMessage("null"), nil
		}

		return raw, nil
	}

	raw, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("bullmq: marshal job data: %w", err)
	}

	return raw, nil
}

// validateJob mirrors Node.js Job.validateOptions.
func (q *Queue) validateJob(job *Job) error {
	o := &job.Opts
	if o.SizeLimit > 0 && len(job.Data) > o.SizeLimit {
		return invalidConfig("The size of job %s exceeds the limit %d bytes", job.Name, o.SizeLimit)
	}

	if o.JobID != "" {
		if o.JobID == "0" || strings.HasPrefix(o.JobID, "0:") {
			return invalidConfig("JobId cannot be '0' or start with '0:'")
		}

		if err := validateCustomJobID(o.JobID); err != nil {
			return err
		}
	}

	if o.Delay < 0 {
		return invalidConfig("Delay cannot be negative")
	}

	if o.Priority < 0 || o.Priority > 2_097_152 {
		return invalidConfig("Priority should be between 0 and 2097152")
	}

	if o.Parent != nil && o.Parent.ID == "" {
		return invalidConfig("Parent id must be provided")
	}

	return nil
}

// mergeJobOptions applies the queue defaults to o.
func (q *Queue) mergeJobOptions(o JobOptions) JobOptions {
	d := q.defaultOpts
	if o.Attempts == 0 {
		o.Attempts = d.Attempts
	}

	if o.Backoff == nil {
		o.Backoff = d.Backoff
	}

	if o.RemoveOnComplete == nil {
		o.RemoveOnComplete = d.RemoveOnComplete
	}

	if o.RemoveOnFail == nil {
		o.RemoveOnFail = d.RemoveOnFail
	}

	if o.Delay == 0 {
		o.Delay = d.Delay
	}

	if o.Priority == 0 {
		o.Priority = d.Priority
	}

	if !o.LIFO {
		o.LIFO = d.LIFO
	}

	if o.KeepLogs == 0 {
		o.KeepLogs = d.KeepLogs
	}

	return o
}

// addJobCall selects the add script and builds its runArgs argument list
// (pre-boxed keys, see keyCache).
func (q *Queue) addJobCall(job *Job) (*scripts.Script, []any, error) {
	kc := q.keys.cache()

	var (
		name string
		keys []any
	)

	switch {
	case job.Delay > 0:
		name, keys = "addDelayedJob", kc.addDelayedKeys
	case job.Opts.Priority > 0:
		name, keys = "addPrioritizedJob", kc.addPrioritizedKeys
	default:
		name, keys = "addStandardJob", kc.addStandardKeys
	}

	argv1, err := packAddArgs(q.keys, job, "")
	if err != nil {
		return nil, nil, err
	}

	s := scripts.Get(name)
	args := append(s.CallArgs(len(keys)+3), keys...)

	return s, append(args, argv1, []byte(job.Data), packJobOpts(&job.Opts)), nil
}

// packAddArgs packs ARGV[1] of the add scripts: [keyPrefix, jobId, name,
// timestamp, parentKey, parentDepsKey, parent, repeatJobKey, dedupKey].
func packAddArgs(k QueueKeys, job *Job, repeatJobKey string) ([]byte, error) {
	var b msgpack.Buf
	b.ArrayLen(9)
	b.Str(k.KeyPrefix())
	b.Str(job.Opts.JobID)
	b.Str(job.Name)
	b.Uint(uint64(max(job.Timestamp, 0)))

	o := &job.Opts
	if p := o.Parent; p != nil {
		parentQueueKey, err := resolveParentQueueKey(k.Prefix(), p.Queue)
		if err != nil {
			return nil, err
		}

		parentKey := parentQueueKey + ":" + p.ID
		b.Str(parentKey)
		b.Str(parentKey + ":dependencies")

		var m msgpack.Map
		m.PutStr("id", p.ID)
		m.PutStr("queueKey", parentQueueKey)

		if o.FailParentOnFailure {
			m.PutBool("fpof", true)
		}

		if o.IgnoreDependencyOnFailure {
			m.PutBool("idof", true)
		}

		if o.RemoveDependencyOnFailure {
			m.PutBool("rdof", true)
		}

		if o.ContinueParentOnFailure {
			m.PutBool("cpof", true)
		}

		m.Encode(&b)
	} else {
		b.Nil()
		b.Nil()
		b.Nil()
	}

	if repeatJobKey != "" {
		b.Str(repeatJobKey)
	} else {
		b.Nil()
	}

	if d := o.Deduplication; d != nil {
		b.Str(k.Dedup(d.ID))
	} else {
		b.Nil()
	}

	return b.Bytes(), nil
}

// packJobOpts packs ARGV[3] of the add scripts (stored as the job's opts).
func packJobOpts(o *JobOptions) []byte {
	var m msgpack.Map
	if o.Delay > 0 {
		m.PutInt("delay", o.Delay.Milliseconds())
	}

	if o.Priority > 0 {
		m.PutInt("priority", int64(o.Priority))
	}

	if o.Attempts > 0 {
		m.PutInt("attempts", int64(o.Attempts))
	}

	if o.LIFO {
		m.PutBool("lifo", true)
	}

	if o.RemoveOnComplete != nil {
		m.PutRaw("removeOnComplete", packRemoveOnFinish(o.RemoveOnComplete))
	}

	if o.RemoveOnFail != nil {
		m.PutRaw("removeOnFail", packRemoveOnFinish(o.RemoveOnFail))
	}

	if o.Backoff != nil {
		m.PutRaw("backoff", packBackoff(o.Backoff))
	}

	if o.Deduplication != nil {
		m.PutRaw("de", packDeduplication(o.Deduplication))
	}

	if o.JobID != "" {
		m.PutStr("jobId", o.JobID)
	}

	if o.KeepLogs > 0 {
		m.PutInt("kl", int64(o.KeepLogs))
	}

	if o.StackTraceLimit > 0 {
		m.PutInt("stackTraceLimit", int64(o.StackTraceLimit))
	}

	if o.FailParentOnFailure {
		m.PutBool("fpof", true)
	}

	if o.IgnoreDependencyOnFailure {
		m.PutBool("idof", true)
	}

	if o.RemoveDependencyOnFailure {
		m.PutBool("rdof", true)
	}

	if o.ContinueParentOnFailure {
		m.PutBool("cpof", true)
	}

	return m.Bytes()
}

func parseAddedJobID(res any) (string, error) {
	switch v := res.(type) {
	case string:
		return v, nil
	case int64:
		if v < 0 {
			return "", addJobError(v)
		}

		return strconv.FormatInt(v, 10), nil
	}

	return "", fmt.Errorf("bullmq: unexpected add job script result %v", res)
}

func addJobError(code int64) error {
	se := NewScriptError(code)
	switch code {
	case CodeParentJobNotExist:
		return fmt.Errorf("%w: Missing key for parent job. addJob", se)
	case CodeParentJobCannotBeReplaced:
		return fmt.Errorf("%w: The parent job cannot be replaced. addJob", se)
	default:
		return se
	}
}

// ── Queue state ─────────────────────────────────────────────────────────────

// Pause pauses the queue: workers stop picking jobs.
func (q *Queue) Pause(ctx context.Context) error {
	k := q.keys
	_, err := scripts.Get("pause").Run(ctx, q.conn.client,
		[]string{k.Wait(), k.Paused(), k.Meta(), k.Prioritized(), k.Events(), k.Delayed(), k.Marker()}, "paused")

	return err
}

// Resume resumes a paused queue.
func (q *Queue) Resume(ctx context.Context) error {
	k := q.keys
	_, err := scripts.Get("pause").Run(ctx, q.conn.client,
		[]string{k.Paused(), k.Wait(), k.Meta(), k.Prioritized(), k.Events(), k.Delayed(), k.Marker()}, "resumed")

	return err
}

// IsPaused reports whether the queue is paused.
func (q *Queue) IsPaused(ctx context.Context) (bool, error) {
	v, err := q.conn.client.HGet(ctx, q.keys.Meta(), "paused").Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}

	return v == "1", err
}

// GetJob returns the job with id, or (nil, nil) when it does not exist.
func (q *Queue) GetJob(ctx context.Context, id string) (*Job, error) {
	job, err := fetchJob(ctx, q.conn, q.keys, id)
	if err != nil || job == nil {
		return nil, err
	}

	job.bind(q.scriptContext())

	return job, nil
}

// GetJobCounts returns the number of jobs in each state.
func (q *Queue) GetJobCounts(ctx context.Context) (JobCounts, error) {
	k := q.keys
	pipe := q.conn.client.Pipeline()

	cmds := []*redis.IntCmd{
		pipe.LLen(ctx, k.Wait()),
		pipe.LLen(ctx, k.Active()),
		pipe.ZCard(ctx, k.Delayed()),
		pipe.ZCard(ctx, k.Prioritized()),
		pipe.ZCard(ctx, k.Completed()),
		pipe.ZCard(ctx, k.Failed()),
		pipe.ZCard(ctx, k.WaitingChildren()),
		pipe.LLen(ctx, k.Paused()),
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return JobCounts{}, err
	}

	return JobCounts{
		Waiting:         cmds[0].Val(),
		Active:          cmds[1].Val(),
		Delayed:         cmds[2].Val(),
		Prioritized:     cmds[3].Val(),
		Completed:       cmds[4].Val(),
		Failed:          cmds[5].Val(),
		WaitingChildren: cmds[6].Val(),
		Paused:          cmds[7].Val(),
	}, nil
}

// allJobTypes is the expansion of an empty type list.
var allJobTypes = []string{"active", "completed", "delayed", "failed", "paused", "prioritized", "waiting", "waiting-children"}

// sanitizeJobTypes dedupes types (keeping order), adds "paused" when
// "waiting" is requested (waiting jobs sit in the paused list while the queue
// is paused) and expands an empty list to every state.
func sanitizeJobTypes(types []string) []string {
	if len(types) == 0 {
		return slices.Clone(allJobTypes)
	}

	out := make([]string, 0, len(types)+1)

	seen := make(map[string]bool, len(types)+1)
	for _, t := range types {
		if !seen[t] {
			seen[t] = true
			out = append(out, t)
		}
	}

	if seen["waiting"] && !seen["paused"] {
		out = append(out, "paused")
	}

	return out
}

func scriptTypes(types []string) []any {
	out := make([]any, len(types))
	for i, t := range types {
		if t == "waiting" {
			t = "wait"
		}

		out[i] = t
	}

	return out
}

// GetRanges returns job ids in the given states. start/end are inclusive
// indices (-1 = last); asc returns the oldest first.
func (q *Queue) GetRanges(ctx context.Context, types []string, start, end int64, asc bool) ([]string, error) {
	sanitized := sanitizeJobTypes(types)
	st := scriptTypes(sanitized)
	args := append([]any{start, end, boolArg(asc)}, st...)

	res, err := scripts.Get("getRanges").Run(ctx, q.conn.client, []string{q.keys.KeyPrefix()}, args...)
	if err != nil {
		return nil, err
	}

	groups, _ := replyArray(res)

	var out []string

	seen := map[string]bool{}

	for i, g := range groups {
		var ids []string

		if items, ok := replyArray(g); ok {
			for _, it := range items {
				if s, ok := replyString(it); ok {
					ids = append(ids, s)
				}
			}
		} else if s, ok := replyString(g); ok {
			ids = append(ids, s)
		}

		if i < len(st) {
			if t, _ := st[i].(string); asc && (t == "wait" || t == "paused" || t == "active") {
				slices.Reverse(ids)
			}
		}

		for _, id := range ids {
			if !seen[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
	}

	return out, nil
}

// GetJobs returns the jobs in the given states (all states when empty).
func (q *Queue) GetJobs(ctx context.Context, types []string, start, end int64, asc bool) ([]*Job, error) {
	st := scriptTypes(sanitizeJobTypes(types))
	args := append([]any{start, end, boolArg(asc), getJobsMaxBackfillIterations}, st...)

	res, err := scripts.Get("getJobs").Run(ctx, q.conn.client, []string{q.keys.KeyPrefix()}, args...)
	if err != nil {
		return nil, err
	}

	sc := q.scriptContext()
	groups, _ := replyArray(res)

	var jobs []*Job

	seen := map[string]bool{}

	for _, g := range groups {
		entries, _ := replyArray(g)
		for _, e := range entries {
			pair, ok := replyArray(e)
			if !ok || len(pair) < 2 {
				continue
			}

			id, ok := replyString(pair[0])
			if !ok || seen[id] {
				continue
			}

			j := jobFromReply(id, pair[1])
			if j == nil {
				continue
			}

			seen[id] = true

			j.bind(sc)
			jobs = append(jobs, j)
		}
	}

	return jobs, nil
}

// GetWaiting returns waiting (and paused) jobs, oldest first.
func (q *Queue) GetWaiting(ctx context.Context, start, end int64) ([]*Job, error) {
	return q.GetJobs(ctx, []string{"waiting"}, start, end, true)
}

// GetWaitingChildren returns jobs waiting for their children.
func (q *Queue) GetWaitingChildren(ctx context.Context, start, end int64) ([]*Job, error) {
	return q.GetJobs(ctx, []string{"waiting-children"}, start, end, true)
}

// GetActive returns active jobs.
func (q *Queue) GetActive(ctx context.Context, start, end int64) ([]*Job, error) {
	return q.GetJobs(ctx, []string{"active"}, start, end, true)
}

// GetDelayed returns delayed jobs.
func (q *Queue) GetDelayed(ctx context.Context, start, end int64) ([]*Job, error) {
	return q.GetJobs(ctx, []string{"delayed"}, start, end, true)
}

// GetPrioritized returns prioritized jobs.
func (q *Queue) GetPrioritized(ctx context.Context, start, end int64) ([]*Job, error) {
	return q.GetJobs(ctx, []string{"prioritized"}, start, end, true)
}

// GetCompleted returns completed jobs, newest first.
func (q *Queue) GetCompleted(ctx context.Context, start, end int64) ([]*Job, error) {
	return q.GetJobs(ctx, []string{"completed"}, start, end, false)
}

// GetFailed returns failed jobs, newest first.
func (q *Queue) GetFailed(ctx context.Context, start, end int64) ([]*Job, error) {
	return q.GetJobs(ctx, []string{"failed"}, start, end, false)
}

func (q *Queue) getCountsRaw(ctx context.Context, types []string) ([]int64, error) {
	res, err := scripts.Get("getCounts").Run(ctx, q.conn.client, []string{q.keys.KeyPrefix()}, scriptTypes(types)...)
	if err != nil {
		return nil, err
	}

	arr, _ := replyArray(res)

	out := make([]int64, len(types))
	for i := range out {
		if i < len(arr) {
			n, _ := replyInt(arr[i])
			out[i] = max(n, 0)
		}
	}

	return out, nil
}

// GetJobCountsByTypes returns the job count of each type (all when empty).
func (q *Queue) GetJobCountsByTypes(ctx context.Context, types ...string) (map[string]int64, error) {
	sanitized := sanitizeJobTypes(types)

	counts, err := q.getCountsRaw(ctx, sanitized)
	if err != nil {
		return nil, err
	}

	out := make(map[string]int64, len(sanitized))
	for i, t := range sanitized {
		out[t] = counts[i]
	}

	return out, nil
}

// GetJobCountByTypes returns the total number of jobs in the given types.
func (q *Queue) GetJobCountByTypes(ctx context.Context, types ...string) (int64, error) {
	m, err := q.GetJobCountsByTypes(ctx, types...)
	if err != nil {
		return 0, err
	}

	var total int64
	for _, v := range m {
		total += v
	}

	return total, nil
}

// RecordJobCountsMetric returns the counts by type, calling record once per
// (state, count) pair when it is not nil (an integration point for metrics
// backends).
func (q *Queue) RecordJobCountsMetric(ctx context.Context, types []string, record func(state string, count int64)) (map[string]int64, error) {
	m, err := q.GetJobCountsByTypes(ctx, types...)
	if err != nil {
		return nil, err
	}

	if record != nil {
		for _, state := range slices.Sorted(maps.Keys(m)) {
			record(state, m[state])
		}
	}

	return m, nil
}

// Count returns the number of jobs waiting to be processed (waiting, paused,
// delayed, prioritized and waiting-children).
func (q *Queue) Count(ctx context.Context) (int64, error) {
	return q.GetJobCountByTypes(ctx, "waiting", "paused", "delayed", "prioritized", "waiting-children")
}

// GetCountsPerPriority returns the number of jobs per priority.
func (q *Queue) GetCountsPerPriority(ctx context.Context, priorities ...int) (map[int]int64, error) {
	unique := make([]int, 0, len(priorities))

	seen := map[int]bool{}
	for _, p := range priorities {
		if !seen[p] {
			seen[p] = true
			unique = append(unique, p)
		}
	}

	args := make([]any, len(unique))
	for i, p := range unique {
		args[i] = p
	}

	res, err := scripts.Get("getCountsPerPriority").Run(ctx, q.conn.client,
		[]string{q.keys.Wait(), q.keys.Prioritized()}, args...)
	if err != nil {
		return nil, err
	}

	arr, _ := replyArray(res)

	out := make(map[int]int64, len(unique))
	for i, p := range unique {
		var n int64
		if i < len(arr) {
			n, _ = replyInt(arr[i])
		}

		out[p] = max(n, 0)
	}

	return out, nil
}

// GetCompletedCount returns the number of completed jobs.
func (q *Queue) GetCompletedCount(ctx context.Context) (int64, error) {
	return q.GetJobCountByTypes(ctx, "completed")
}

// GetFailedCount returns the number of failed jobs.
func (q *Queue) GetFailedCount(ctx context.Context) (int64, error) {
	return q.GetJobCountByTypes(ctx, "failed")
}

// GetDelayedCount returns the number of delayed jobs.
func (q *Queue) GetDelayedCount(ctx context.Context) (int64, error) {
	return q.GetJobCountByTypes(ctx, "delayed")
}

// GetActiveCount returns the number of active jobs.
func (q *Queue) GetActiveCount(ctx context.Context) (int64, error) {
	return q.GetJobCountByTypes(ctx, "active")
}

// GetPrioritizedCount returns the number of prioritized jobs.
func (q *Queue) GetPrioritizedCount(ctx context.Context) (int64, error) {
	return q.GetJobCountByTypes(ctx, "prioritized")
}

// GetWaitingCount returns the number of waiting (and paused) jobs.
func (q *Queue) GetWaitingCount(ctx context.Context) (int64, error) {
	return q.GetJobCountByTypes(ctx, "waiting")
}

// GetWaitingChildrenCount returns the number of jobs waiting for children.
func (q *Queue) GetWaitingChildrenCount(ctx context.Context) (int64, error) {
	return q.GetJobCountByTypes(ctx, "waiting-children")
}

// GetWorkers lists the workers connected to the queue (from CLIENT LIST).
// Each entry's "name" is the queue name and "rawname" the client name. It
// returns an empty list when the server does not support CLIENT LIST.
func (q *Queue) GetWorkers(ctx context.Context) ([]map[string]string, error) {
	lists, err := q.conn.clientLists(ctx)
	if err != nil {
		return []map[string]string{}, nil //nolint:nilerr // CLIENT LIST unsupported (e.g. managed Redis)
	}

	unnamed := q.keys.ClientName("")
	namedPrefix := q.keys.ClientName(":w:")

	best := []map[string]string{}
	for _, l := range lists {
		if c := parseClientList(l, q.name, unnamed, namedPrefix); len(c) > len(best) {
			best = c
		}
	}

	return best, nil
}

// GetWorkersCount returns the number of connected workers.
func (q *Queue) GetWorkersCount(ctx context.Context) (int, error) {
	w, err := q.GetWorkers(ctx)
	return len(w), err
}

func parseClientList(list, queueName, unnamed, namedPrefix string) []map[string]string {
	var clients []map[string]string

	for _, line := range strings.FieldsFunc(list, func(r rune) bool { return r == '\r' || r == '\n' }) {
		client := map[string]string{}

		for kv := range strings.SplitSeq(line, " ") {
			if k, v, ok := strings.Cut(kv, "="); ok {
				client[k] = v
			}
		}

		name := client["name"]
		if name != "" && (name == unnamed || strings.HasPrefix(name, namedPrefix)) {
			client["name"] = queueName
			client["rawname"] = name
			clients = append(clients, client)
		}
	}

	return clients
}

// GetMeta returns the queue metadata.
func (q *Queue) GetMeta(ctx context.Context) (QueueMeta, error) {
	h, err := q.conn.client.HGetAll(ctx, q.keys.Meta()).Result()
	if err != nil {
		return QueueMeta{}, err
	}

	meta := QueueMeta{Other: map[string]string{}}

	parse := func(v string) *int64 {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil
		}

		return &n
	}
	for k, v := range h {
		switch k {
		case "concurrency":
			meta.Concurrency = parse(v)
		case "max":
			meta.Max = parse(v)
		case "duration":
			meta.Duration = parse(v)
		case "opts.maxLenEvents":
			meta.MaxLenEvents = parse(v)
		case "paused":
			meta.Paused = v == "1"
		default:
			meta.Other[k] = v
		}
	}

	return meta, nil
}

// GetVersion returns the library version stored in the meta hash ("" when
// unset).
func (q *Queue) GetVersion(ctx context.Context) (string, error) {
	v, err := q.conn.client.HGet(ctx, q.keys.Meta(), "library").Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}

	return v, err
}

// IsMaxed reports whether the active jobs reached the global concurrency.
func (q *Queue) IsMaxed(ctx context.Context) (bool, error) {
	res, err := scripts.Get("isMaxed").Run(ctx, q.conn.client, []string{q.keys.Meta(), q.keys.Active()})
	if err != nil {
		return false, err
	}

	n, _ := replyInt(res)

	return n == 1, nil
}

// PrometheusLabel is an extra label added to every exported series.
type PrometheusLabel struct {
	Name, Value string
}

// ExportPrometheusMetrics returns job counts and totals in the Prometheus
// text exposition format.
func (q *Queue) ExportPrometheusMetrics(ctx context.Context, globalLabels ...PrometheusLabel) (string, error) {
	counts, err := q.GetJobCounts(ctx)
	if err != nil {
		return "", err
	}

	queue := escapePrometheusLabelValue(q.name)

	var vars strings.Builder
	for _, l := range globalLabels {
		fmt.Fprintf(&vars, ", %s=\"%s\"", l.Name, escapePrometheusLabelValue(l.Value))
	}

	lines := []string{
		"# HELP bullmq_job_count Number of jobs in the queue by state",
		"# TYPE bullmq_job_count gauge",
	}
	for _, s := range []struct {
		state string
		n     int64
	}{
		{"active", counts.Active},
		{"completed", counts.Completed},
		{"delayed", counts.Delayed},
		{"failed", counts.Failed},
		{"paused", counts.Paused},
		{"prioritized", counts.Prioritized},
		{"waiting", counts.Waiting},
		{"waiting-children", counts.WaitingChildren},
	} {
		lines = append(lines, fmt.Sprintf("bullmq_job_count{queue=\"%s\", state=\"%s\"%s} %d", queue, s.state, vars.String(), s.n))
	}

	completed, err := q.GetMetrics(ctx, "completed", 0, -1)
	if err != nil {
		return "", err
	}

	failed, err := q.GetMetrics(ctx, "failed", 0, -1)
	if err != nil {
		return "", err
	}

	lines = append(lines,
		"# HELP bullmq_job_completed_total Total number of completed jobs",
		"# TYPE bullmq_job_completed_total counter",
		fmt.Sprintf("bullmq_job_completed_total{queue=\"%s\"%s} %d", queue, vars.String(), completed.Meta.Count),
		"# HELP bullmq_job_failed_total Total number of failed jobs",
		"# TYPE bullmq_job_failed_total counter",
		fmt.Sprintf("bullmq_job_failed_total{queue=\"%s\"%s} %d", queue, vars.String(), failed.Meta.Count),
	)

	return strings.Join(lines, "\n"), nil
}

func escapePrometheusLabelValue(v string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(v)
}

// GetMetrics returns the "completed" or "failed" time series. start/end
// index the data points, 0 being the newest.
func (q *Queue) GetMetrics(ctx context.Context, metricType string, start, end int64) (Metrics, error) {
	if metricType != "completed" && metricType != "failed" {
		return Metrics{}, invalidConfig(`metric type must be "completed" or "failed", got %q`, metricType)
	}

	res, err := scripts.Get("getMetrics").Run(ctx, q.conn.client,
		[]string{q.keys.Get("metrics:" + metricType), q.keys.Get("metrics:" + metricType + ":data")}, start, end)
	if err != nil {
		return Metrics{}, err
	}

	var m Metrics

	parts, _ := replyArray(res)
	num := func(v any) int64 { n, _ := replyInt(v); return max(n, 0) }

	if len(parts) > 0 {
		meta, _ := replyArray(parts[0])
		if len(meta) > 0 {
			m.Meta.Count = num(meta[0])
		}

		if len(meta) > 1 {
			m.Meta.PrevTS = num(meta[1])
		}

		if len(meta) > 2 {
			m.Meta.PrevCount = num(meta[2])
		}
	}

	if len(parts) > 1 {
		data, _ := replyArray(parts[1])

		m.Data = make([]int64, len(data))
		for i, d := range data {
			m.Data[i] = num(d)
		}
	}

	if len(parts) > 2 {
		m.Count = num(parts[2])
	}

	return m, nil
}

// ── Dependencies ────────────────────────────────────────────────────────────

// DependencyItem is one entry of [Queue.GetDependencies]. For processed
// dependencies Value holds the parsed return value, or Err the parse error.
type DependencyItem struct {
	ID    string
	Value any
	Err   string
}

// QueueDependencies is a page of a parent's dependencies.
type QueueDependencies struct {
	Items []DependencyItem
	Jobs  []*Job
	Total int64
}

// GetDependencies pages through a parent's "pending" or "processed"
// dependencies. start/end are inclusive (-1 = all).
func (q *Queue) GetDependencies(ctx context.Context, parentID, depType string, start, end int64) (QueueDependencies, error) {
	parentKey := q.keys.JobKey(parentID)

	var key string

	switch depType {
	case "processed":
		key = parentKey + ":processed"
	case "pending":
		key = parentKey + ":dependencies"
	default:
		return QueueDependencies{}, invalidConfig("invalid dependency type: %s (expected 'processed' or 'pending')", depType)
	}

	pageSize := int64(-1)
	if end >= 0 {
		pageSize = max(end-start+1, 0)
	}

	cursor, offset := "0", int64(0)

	var (
		total int64
		items []any
		jobs  []*Job
	)

	for {
		res, err := scripts.Get("paginate").Run(ctx, q.conn.client, []string{key},
			start+int64(len(items)), end, cursor, offset, 5, 1)
		if err != nil {
			return QueueDependencies{}, err
		}

		next, nextOffset, page, pageTotal, rawJobs := parsePaginateReply(res)
		if total == 0 {
			total = pageTotal
		}

		for i := 0; i < len(page) && i < len(rawJobs); i++ {
			qk, ok := paginateItemKey(page[i])
			if !ok {
				continue
			}

			split, ok := splitQualifiedJobKey(qk)
			if !ok {
				continue
			}
			// A removed child keeps its entry in the processed hash; build a
			// default job from the key to keep one job per item.
			j := jobFromReply(split.jobID, rawJobs[i])
			if j == nil { // child removed (removeOnComplete): keep one job per item
				j = jobFromHash(split.jobID, nil)
			}

			j.bind(q.scriptContextFor(NewQueueKeys(split.queueName, split.prefix)))
			jobs = append(jobs, j)
		}

		items = append(items, page...)

		cursor, offset = next, nextOffset
		if cursor == "0" || (pageSize >= 0 && int64(len(items)) >= pageSize) {
			break
		}
	}

	out := QueueDependencies{Jobs: jobs, Total: total}

	for _, it := range items {
		if d, ok := decodeDependencyItem(it); ok {
			out.Items = append(out.Items, d)
		}
	}

	return out, nil
}

// parsePaginateReply parses `[cursor, offset, items, total, jobs]`.
func parsePaginateReply(res any) (cursor string, offset int64, items []any, total int64, jobs []any) {
	arr, _ := replyArray(res)
	cursor = "0"

	if len(arr) > 0 {
		if s, ok := replyString(arr[0]); ok {
			cursor = s
		}
	}

	if len(arr) > 1 {
		offset, _ = replyInt(arr[1])
	}

	if len(arr) > 2 {
		items, _ = replyArray(arr[2])
	}

	if len(arr) > 3 {
		total, _ = replyInt(arr[3])
		total = max(total, 0)
	}

	if len(arr) > 4 {
		jobs, _ = replyArray(arr[4])
	}

	return cursor, offset, items, total, jobs
}

func paginateItemKey(item any) (string, bool) {
	if pair, ok := replyArray(item); ok {
		if len(pair) == 0 {
			return "", false
		}

		return replyString(pair[0])
	}

	return replyString(item)
}

func decodeDependencyItem(item any) (DependencyItem, bool) {
	if pair, ok := replyArray(item); ok {
		if len(pair) == 0 {
			return DependencyItem{}, false
		}

		id, ok := replyString(pair[0])
		if !ok {
			return DependencyItem{}, false
		}

		var raw string
		if len(pair) > 1 {
			raw, _ = replyString(pair[1])
		}

		var v any
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			return DependencyItem{ID: id, Err: err.Error()}, true
		}

		return DependencyItem{ID: id, Value: v}, true
	}

	id, ok := replyString(item)

	return DependencyItem{ID: id}, ok
}

// GetChildrenValues returns the return values of a parent's completed
// children.
func (q *Queue) GetChildrenValues(ctx context.Context, jobID string) (map[string]any, error) {
	return childrenValues(ctx, q.conn, q.keys.JobKey(jobID))
}

// GetIgnoredChildrenFailures returns the failures of children that failed
// with IgnoreDependencyOnFailure.
func (q *Queue) GetIgnoredChildrenFailures(ctx context.Context, jobID string) (map[string]string, error) {
	return q.conn.client.HGetAll(ctx, q.keys.JobKey(jobID)+":failed").Result()
}

// GetFailedChildrenValues is a deprecated alias of
// [Queue.GetIgnoredChildrenFailures].
func (q *Queue) GetFailedChildrenValues(ctx context.Context, jobID string) (map[string]string, error) {
	return q.GetIgnoredChildrenFailures(ctx, jobID)
}

// GetDependenciesCount returns a parent's children counts by type.
func (q *Queue) GetDependenciesCount(ctx context.Context, jobID string) (DependenciesCount, error) {
	return dependenciesCount(ctx, q.conn, q.keys.JobKey(jobID))
}

// GetUnprocessedDependencies returns a parent's pending child keys.
func (q *Queue) GetUnprocessedDependencies(ctx context.Context, jobID string) ([]string, error) {
	return q.conn.client.SMembers(ctx, q.keys.JobKey(jobID)+":dependencies").Result()
}

// RemoveChildDependency removes child jobID's dependency from the parent at
// parentKey (`{prefix}:{queue}:{id}`).
func (q *Queue) RemoveChildDependency(ctx context.Context, jobID, parentKey string) (bool, error) {
	return removeChildDependency(ctx, q.conn, q.keys, jobID, parentKey)
}

// GetJobState returns the state of a job ([StateUnknown] if missing).
func (q *Queue) GetJobState(ctx context.Context, jobID string) (JobState, error) {
	return jobState(ctx, q.conn, q.keys, jobID)
}

// ── Removal & maintenance ───────────────────────────────────────────────────

// Remove removes a job and its children. It reports false (without error)
// when the job or one of its dependencies is locked. A missing job counts as
// removed. Jobs belonging to a scheduler cannot be removed directly.
func (q *Queue) Remove(ctx context.Context, jobID string) (bool, error) {
	return q.removeJob(ctx, jobID, true)
}

// RemoveWithoutChildren removes a job but keeps its children.
func (q *Queue) RemoveWithoutChildren(ctx context.Context, jobID string) (bool, error) {
	return q.removeJob(ctx, jobID, false)
}

func (q *Queue) removeJob(ctx context.Context, jobID string, removeChildren bool) (bool, error) {
	res, err := scripts.Get("removeJob").Run(ctx, q.conn.client,
		[]string{q.keys.JobKey(jobID), q.keys.Repeat()}, jobID, boolArg(removeChildren), q.keys.KeyPrefix())
	if err != nil {
		return false, err
	}

	code, _ := replyInt(res)
	switch {
	case code == 1:
		return true, nil
	case code == CodeJobBelongsToJobScheduler:
		return false, &ScriptError{Code: code, Message: fmt.Sprintf(
			"Job %s belongs to a job scheduler and cannot be removed directly. removeJob", jobID,
		)}
	case code < 0:
		return false, NewScriptError(code)
	default:
		return false, nil
	}
}

// RemoveUnprocessedChildren removes the pending children of a job (active
// children are skipped).
func (q *Queue) RemoveUnprocessedChildren(ctx context.Context, jobID string) error {
	_, err := scripts.Get("removeUnprocessedChildren").Run(ctx, q.conn.client,
		[]string{q.keys.JobKey(jobID), q.keys.Meta()}, q.keys.KeyPrefix(), jobID)

	return err
}

// Clean removes jobs older than grace from state ("completed", "failed",
// "wait"/"waiting", "active", "delayed", "paused", "prioritized"). limit 0
// removes all matching jobs. It returns the removed ids.
func (q *Queue) Clean(ctx context.Context, grace time.Duration, limit int, state string) ([]string, error) {
	if state == "waiting" {
		state = "wait"
	}

	var setKey string

	switch state {
	case "completed", "failed", "wait", "active", "delayed", "paused", "prioritized":
		setKey = q.keys.Get(state)
	default:
		return nil, invalidConfig("invalid state: %s", state)
	}

	ts := nowMillis() - grace.Milliseconds()

	perCall := 10000
	if limit > 0 {
		perCall = min(limit, 10000)
	}

	var deleted []string

	for {
		res, err := scripts.Get("cleanJobsInSet").Run(ctx, q.conn.client,
			[]string{setKey, q.keys.Events(), q.keys.Repeat()}, q.keys.KeyPrefix(), ts, perCall, state)
		if err != nil {
			return deleted, err
		}

		batch, _ := replyArray(res)
		for _, v := range batch {
			if s, ok := replyString(v); ok {
				deleted = append(deleted, s)
			}
		}

		if len(batch) < perCall || (limit > 0 && len(deleted) >= limit) {
			return deleted, nil
		}
	}
}

// Drain removes waiting (and, with delayed, delayed) jobs.
func (q *Queue) Drain(ctx context.Context, delayed bool) error {
	k := q.keys
	_, err := scripts.Get("drain").Run(ctx, q.conn.client,
		[]string{k.Wait(), k.Paused(), k.Delayed(), k.Prioritized(), k.Repeat()}, k.KeyPrefix(), boolArg(delayed))

	return err
}

// RetryJobs moves failed (or completed) jobs finished before timestamp
// (zero = now) back to wait, count per batch.
func (q *Queue) RetryJobs(ctx context.Context, state string, count int, timestamp time.Time) error {
	if state == "" {
		state = "failed"
	}

	if count <= 0 {
		count = 1000
	}

	ts := timeToMillis(timestamp)
	if ts == 0 {
		ts = nowMillis()
	}

	return q.moveJobsToWait(ctx, q.keys.Get(state), count, ts, state)
}

// PromoteJobs moves every delayed job to wait, count per batch.
func (q *Queue) PromoteJobs(ctx context.Context, count int) error {
	if count <= 0 {
		count = 1000
	}

	return q.moveJobsToWait(ctx, q.keys.Delayed(), count, maxSafeInteger, "delayed")
}

func (q *Queue) moveJobsToWait(ctx context.Context, setKey string, count int, ts any, state string) error {
	k := q.keys

	keys := []string{k.KeyPrefix(), k.Events(), setKey, k.Wait(), k.Paused(), k.Meta(), k.Active(), k.Marker()}
	for {
		res, err := scripts.Get("moveJobsToWait").Run(ctx, q.conn.client, keys, count, ts, state)
		if err != nil {
			return err
		}

		if n, _ := replyInt(res); n != 1 {
			return nil
		}
	}
}

// RateLimit activates the rate limit for expire, blocking all workers.
func (q *Queue) RateLimit(ctx context.Context, expire time.Duration) error {
	return q.conn.client.Set(ctx, q.keys.Limiter(), maxSafeInteger, expire).Err()
}

// RemoveRateLimitKey removes the rate limit so processing resumes.
func (q *Queue) RemoveRateLimitKey(ctx context.Context) (bool, error) {
	n, err := q.conn.client.Del(ctx, q.keys.Limiter()).Result()
	return n > 0, err
}

// SetGlobalConcurrency limits the active jobs across all workers.
func (q *Queue) SetGlobalConcurrency(ctx context.Context, concurrency int) error {
	return q.conn.client.HSet(ctx, q.keys.Meta(), "concurrency", concurrency).Err()
}

// RemoveGlobalConcurrency removes the global concurrency limit.
func (q *Queue) RemoveGlobalConcurrency(ctx context.Context) error {
	return q.conn.client.HDel(ctx, q.keys.Meta(), "concurrency").Err()
}

// GetGlobalConcurrency returns the global concurrency (0, false when unset).
func (q *Queue) GetGlobalConcurrency(ctx context.Context) (int64, bool, error) {
	v, err := q.conn.client.HGet(ctx, q.keys.Meta(), "concurrency").Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, err
	}

	n, err := strconv.ParseInt(v, 10, 64)

	return n, err == nil, nil
}

// SetGlobalRateLimit limits the queue to max jobs per duration.
func (q *Queue) SetGlobalRateLimit(ctx context.Context, maxJobs int64, duration time.Duration) error {
	return q.conn.client.HSet(ctx, q.keys.Meta(), "max", maxJobs, "duration", duration.Milliseconds()).Err()
}

// RemoveGlobalRateLimit removes the global rate limit.
func (q *Queue) RemoveGlobalRateLimit(ctx context.Context) error {
	return q.conn.client.HDel(ctx, q.keys.Meta(), "max", "duration").Err()
}

// GetGlobalRateLimit returns the global rate limit (ok is false when unset).
func (q *Queue) GetGlobalRateLimit(ctx context.Context) (maxJobs int64, duration time.Duration, ok bool, err error) {
	vals, err := q.conn.client.HMGet(ctx, q.keys.Meta(), "max", "duration").Result()
	if err != nil {
		return 0, 0, false, err
	}

	m, okM := parseOptInt(vals[0])

	d, okD := parseOptInt(vals[1])
	if !okM || !okD {
		return 0, 0, false, nil
	}

	return m, time.Duration(d) * time.Millisecond, true, nil
}

func parseOptInt(v any) (int64, bool) {
	s, ok := v.(string)
	if !ok {
		return 0, false
	}

	n, err := strconv.ParseInt(s, 10, 64)

	return n, err == nil
}

// GetRateLimitTTL returns the remaining rate limit time in ms. maxJobs 0
// skips the max check. Negative values mirror PTTL (-2 no key, -1 no expiry).
func (q *Queue) GetRateLimitTTL(ctx context.Context, maxJobs int64) (int64, error) {
	res, err := scripts.Get("getRateLimitTtl").Run(ctx, q.conn.client,
		[]string{q.keys.Limiter(), q.keys.Meta()}, maxJobs)
	if err != nil {
		return 0, err
	}

	n, _ := replyInt(res)

	return n, nil
}

// RemoveDeduplicationKey removes the deduplication key if it still points to
// jobID.
func (q *Queue) RemoveDeduplicationKey(ctx context.Context, dedupID, jobID string) (bool, error) {
	res, err := scripts.Get("removeDeduplicationKey").Run(ctx, q.conn.client, []string{q.keys.Dedup(dedupID)}, jobID)
	if err != nil {
		return false, err
	}

	n, _ := replyInt(res)

	return n == 1, nil
}

// GetDeduplicationJobID returns the job id owning a deduplication id ("",
// false when none).
func (q *Queue) GetDeduplicationJobID(ctx context.Context, dedupID string) (string, bool, error) {
	v, err := q.conn.client.Get(ctx, q.keys.Dedup(dedupID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}

	if err != nil {
		return "", false, err
	}

	return v, true, nil
}

// GetDebounceJobID is a deprecated alias of [Queue.GetDeduplicationJobID].
func (q *Queue) GetDebounceJobID(ctx context.Context, id string) (string, bool, error) {
	return q.GetDeduplicationJobID(ctx, id)
}

// RemoveDebounceKey deletes a debounce key unconditionally and returns the
// number of keys removed. Deprecated: use [Queue.RemoveDeduplicationKey].
func (q *Queue) RemoveDebounceKey(ctx context.Context, id string) (int64, error) {
	return q.conn.client.Del(ctx, q.keys.Dedup(id)).Result()
}

// GetJobLogs returns a page of a job's logs and the total count.
func (q *Queue) GetJobLogs(ctx context.Context, jobID string, start, end int64, asc bool) ([]string, int64, error) {
	key := q.keys.JobKey(jobID) + ":logs"
	pipe := q.conn.client.Pipeline()

	var rng *redis.StringSliceCmd
	if asc {
		rng = pipe.LRange(ctx, key, start, end)
	} else {
		rng = pipe.LRange(ctx, key, -(end + 1), -(start + 1))
	}

	count := pipe.LLen(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, 0, err
	}

	logs := rng.Val()
	if !asc {
		slices.Reverse(logs)
	}

	return logs, count.Val(), nil
}

// TrimEvents trims the event stream to about maxLength entries.
func (q *Queue) TrimEvents(ctx context.Context, maxLength int64) (int64, error) {
	return q.conn.client.XTrimMaxLenApprox(ctx, q.keys.Events(), maxLength, 0).Result()
}

// UpdateJobProgress updates a job's progress by id.
func (q *Queue) UpdateJobProgress(ctx context.Context, jobID string, progress any) error {
	raw, err := marshalProgress(progress)
	if err != nil {
		return err
	}

	return updateProgress(ctx, q.conn, q.keys, jobID, raw)
}

// Obliterate removes every key of the queue. Without force it fails when
// jobs are active; with force the queue is paused first.
func (q *Queue) Obliterate(ctx context.Context, force bool, count int) error {
	if force {
		_ = q.Pause(ctx) // best effort; the script checks the paused state itself
	}

	if count <= 0 {
		count = 1000
	}

	for {
		res, err := scripts.Get("obliterate").Run(ctx, q.conn.client,
			[]string{q.keys.Meta(), q.keys.KeyPrefix()}, count, boolArg(force))
		if err != nil {
			return err
		}

		n, _ := replyInt(res)
		switch {
		case n == 1:
			continue
		case n == -1:
			return fmt.Errorf("%w: Cannot obliterate non-paused queue", ErrInvalidConfig)
		case n == -2:
			return fmt.Errorf("%w: Cannot obliterate queue with active jobs", ErrInvalidConfig)
		case n < 0:
			return NewScriptError(n)
		default:
			return nil
		}
	}
}
