package bullmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/imdat99/bullmq/internal/msgpack"
	"github.com/imdat99/bullmq/internal/scripts"
)

// cronParser accepts 5-field patterns and 6-field patterns with seconds.
var cronParser = cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// NextCronTime returns the first occurrence of pattern strictly after the
// reference time, which is max(now, startDate). tz is an IANA zone ("" = UTC).
func NextCronTime(pattern string, now time.Time, tz string, startDate time.Time) (time.Time, error) {
	sched, err := cronParser.Parse(pattern)
	if err != nil {
		return time.Time{}, invalidConfig("invalid cron pattern '%s': %v", pattern, err)
	}

	loc := time.UTC
	if tz != "" {
		if loc, err = time.LoadLocation(tz); err != nil {
			return time.Time{}, invalidConfig("invalid timezone: %s", tz)
		}
	}

	ref := now
	if !startDate.IsZero() && startDate.After(now) {
		ref = startDate
	}

	next := sched.Next(ref.In(loc))
	if next.IsZero() {
		return time.Time{}, invalidConfig("cron pattern '%s' has no next occurrence", pattern)
	}

	return next, nil
}

// JobScheduler describes a job scheduler.
type JobScheduler struct {
	// Key is the scheduler id.
	Key string
	// Name is the name of the produced jobs.
	Name           string
	IterationCount int64
	Limit          int64
	StartDate      time.Time
	EndDate        time.Time
	TZ             string
	Pattern        string
	Every          time.Duration
	Offset         time.Duration
	// Next is the next execution time (zero if unknown).
	Next time.Time
	// Template holds the template data and options of the produced jobs.
	Template *JobSchedulerTemplate
}

// JobSchedulerTemplate is the template of the jobs a scheduler produces.
type JobSchedulerTemplate struct {
	Data json.RawMessage
	Opts json.RawMessage
}

func parseSchedulerHash(id string, f map[string]string, next int64) JobScheduler {
	s := JobScheduler{
		Key:            id,
		Name:           f["name"],
		IterationCount: parseInt64(f["ic"]),
		Limit:          parseInt64(f["limit"]),
		StartDate:      millisToTime(parseInt64(f["startDate"])),
		EndDate:        millisToTime(parseInt64(f["endDate"])),
		TZ:             f["tz"],
		Pattern:        f["pattern"],
		Every:          time.Duration(parseInt64(f["every"])) * time.Millisecond,
		Offset:         time.Duration(parseInt64(f["offset"])) * time.Millisecond,
		Next:           millisToTime(next),
	}

	data, opts := f["data"], f["opts"]
	if data != "" || opts != "" {
		t := &JobSchedulerTemplate{}
		if data != "" && json.Valid([]byte(data)) {
			t.Data = json.RawMessage(data)
		}

		if opts != "" && json.Valid([]byte(opts)) {
			t.Opts = json.RawMessage(opts)
		}

		s.Template = t
	}

	return s
}

// packSchedulerOpts packs ARGV[2] of addJobScheduler.
func packSchedulerOpts(name string, r *RepeatOptions) []byte {
	var m msgpack.Map
	m.PutStr("name", name)

	if r.TZ != "" {
		m.PutStr("tz", r.TZ)
	}

	if r.Pattern != "" {
		m.PutStr("pattern", r.Pattern)
	}

	if r.Every > 0 {
		m.PutInt("every", r.Every.Milliseconds())
	}

	if r.Limit > 0 {
		m.PutInt("limit", r.Limit)
	}

	if r.Offset > 0 {
		m.PutInt("offset", r.Offset.Milliseconds())
	}

	if !r.StartDate.IsZero() {
		m.PutInt("startDate", r.StartDate.UnixMilli())
	}

	if !r.EndDate.IsZero() {
		m.PutInt("endDate", r.EndDate.UnixMilli())
	}

	return m.Bytes()
}

// packRepeat packs the repeat options carried by produced jobs, with count
// set to iteration.
func packRepeat(r *RepeatOptions, iteration int64, offset time.Duration) []byte {
	var m msgpack.Map
	m.PutInt("count", iteration)

	if offset > 0 {
		m.PutInt("offset", offset.Milliseconds())
	}

	if r.Every > 0 {
		m.PutInt("every", r.Every.Milliseconds())
	}

	if r.Pattern != "" {
		m.PutStr("pattern", r.Pattern)
	}

	if r.Limit > 0 {
		m.PutInt("limit", r.Limit)
	}

	if r.TZ != "" {
		m.PutStr("tz", r.TZ)
	}

	if !r.StartDate.IsZero() {
		m.PutInt("startDate", r.StartDate.UnixMilli())
	}

	if !r.EndDate.IsZero() {
		m.PutInt("endDate", r.EndDate.UnixMilli())
	}

	return m.Bytes()
}

// packDelayedJobOpts packs ARGV[6] of addJobScheduler (the first job).
func packDelayedJobOpts(o *JobOptions, schedulerID string, nextMillis, iteration int64, offset time.Duration, r *RepeatOptions) []byte {
	now := nowMillis()
	delay := max(nextMillis+offset.Milliseconds()-now, 0)

	var m msgpack.Map
	m.PutInt("delay", delay)
	m.PutInt("timestamp", now)
	m.PutStr("jobId", "repeat:"+schedulerID+":"+strconv.FormatInt(nextMillis, 10))
	m.PutStr("repeatJobKey", schedulerID)
	m.PutRaw("repeat", packRepeat(r, iteration, offset))
	putTemplateOpts(&m, o)

	return m.Bytes()
}

// packTemplateOpts packs ARGV[5] of addJobScheduler (stored template opts).
func packTemplateOpts(o *JobOptions) []byte {
	var m msgpack.Map
	putTemplateOpts(&m, o)

	return m.Bytes()
}

// UpsertJobScheduler creates or updates a job scheduler producing jobs named
// jobName (default schedulerID) with data and opts on a cron pattern or at a
// fixed interval. It returns the next job, or nil when the scheduler is
// already exhausted (limit reached).
func (q *Queue) UpsertJobScheduler(ctx context.Context, schedulerID string, repeat RepeatOptions,
	jobName string, data any, opts *JobOptions,
) (*Job, error) {
	now := time.Now()
	if err := validateRepeat(&repeat, now); err != nil {
		return nil, err
	}

	iteration := repeat.Count + 1
	if repeat.Limit > 0 && iteration > repeat.Limit {
		return nil, nil
	}

	nextMillis, err := firstSchedulerMillis(&repeat, now)
	if err != nil {
		return nil, err
	}

	if jobName == "" {
		jobName = schedulerID
	}

	if data == nil {
		data = map[string]any{}
	}

	raw, err := marshalData(data)
	if err != nil {
		return nil, err
	}

	var o JobOptions
	if opts != nil {
		o = *opts
	}

	var offset time.Duration
	if repeat.Every > 0 {
		offset = repeat.Offset
	}

	k := q.keys

	res, err := scripts.Get("addJobScheduler").Run(ctx, q.conn.client,
		[]string{
			k.Repeat(), k.Delayed(), k.Wait(), k.Paused(), k.Meta(), k.Prioritized(),
			k.Marker(), k.ID(), k.Events(), k.PC(), k.Active(),
		},
		nextMillis, packSchedulerOpts(jobName, &repeat), schedulerID, []byte(raw),
		packTemplateOpts(&o), packDelayedJobOpts(&o, schedulerID, nextMillis, iteration, offset, &repeat),
		now.UnixMilli(), k.KeyPrefix(), "")
	if err != nil {
		return nil, err
	}

	if code, ok := res.(int64); ok && code < 0 {
		return nil, schedulerError(code, schedulerID)
	}

	arr, ok := replyArray(res)
	if !ok || len(arr) < 1 {
		return nil, nil
	}

	id, ok := replyString(arr[0])
	if !ok {
		return nil, nil
	}

	job := newJob(jobName, raw, o)
	job.ID = id
	job.RepeatJobKey = schedulerID
	job.Opts.RepeatJobKey = schedulerID
	job.bind(q.scriptContext())

	return job, nil
}

// validateRepeat checks the scheduler options like Node.js does.
func validateRepeat(r *RepeatOptions, now time.Time) error {
	switch {
	case r.Pattern != "" && r.Every > 0:
		return invalidConfig("Both .pattern and .every options are defined; only one may be used")
	case r.Pattern == "" && r.Every <= 0:
		return invalidConfig("Either .pattern or .every option must be defined")
	case r.Immediately && !r.StartDate.IsZero():
		return invalidConfig("Both .immediately and .startDate options are defined; only one may be used")
	case !r.EndDate.IsZero() && r.EndDate.Before(now):
		return invalidConfig("End date must be greater than current timestamp")
	}

	return nil
}

// firstSchedulerMillis returns the first execution time of a cron scheduler
// (0 for `every`, which the Lua script computes itself).
func firstSchedulerMillis(r *RepeatOptions, now time.Time) (int64, error) {
	switch {
	case r.Pattern == "":
		return 0, nil
	case r.Immediately:
		return now.UnixMilli(), nil
	}

	next, err := NextCronTime(r.Pattern, now, r.TZ, r.StartDate)
	if err != nil {
		return 0, err
	}

	return next.UnixMilli(), nil
}

func schedulerError(code int64, id string) error {
	se := NewScriptError(code)
	switch code {
	case CodeSchedulerJobIDCollision:
		return &detailedScriptError{se, "Cannot create job scheduler iteration - job ID already exists. addJobScheduler " + id}
	case CodeSchedulerJobSlotsBusy:
		return &detailedScriptError{se, "Cannot create job scheduler iteration - current and next time slots already have jobs. addJobScheduler " + id}
	default:
		return se
	}
}

// GetJobScheduler returns a scheduler (nil when it does not exist).
func (q *Queue) GetJobScheduler(ctx context.Context, schedulerID string) (*JobScheduler, error) {
	res, err := scripts.Get("getJobScheduler").Run(ctx, q.conn.client, []string{q.keys.Repeat()}, schedulerID)
	if err != nil {
		return nil, err
	}

	arr, ok := replyArray(res)
	if !ok || len(arr) < 2 {
		return nil, nil
	}

	fields := replyHash(arr[0])
	if len(fields) == 0 {
		return nil, nil
	}

	next, _ := replyInt(arr[1])
	s := parseSchedulerHash(schedulerID, fields, next)

	return &s, nil
}

// GetJobSchedulers returns schedulers ordered by next execution time.
func (q *Queue) GetJobSchedulers(ctx context.Context, start, end int64, asc bool) ([]JobScheduler, error) {
	var (
		zs  []redisZ
		err error
	)
	if asc {
		zs, err = zrangeWithScores(ctx, q.conn, q.keys.Repeat(), start, end, false)
	} else {
		zs, err = zrangeWithScores(ctx, q.conn, q.keys.Repeat(), start, end, true)
	}

	if err != nil {
		return nil, err
	}

	pipe := q.conn.client.Pipeline()

	cmds := make([]func() map[string]string, len(zs))
	for i, z := range zs {
		c := pipe.HGetAll(ctx, q.keys.Get("repeat:"+z.member))
		cmds[i] = c.Val
	}

	if len(zs) > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, err
		}
	}

	out := make([]JobScheduler, 0, len(zs))
	for i, z := range zs {
		if f := cmds[i](); len(f) > 0 {
			out = append(out, parseSchedulerHash(z.member, f, int64(z.score)))
		}
	}

	return out, nil
}

type redisZ struct {
	member string
	score  float64
}

func zrangeWithScores(ctx context.Context, conn *RedisConnection, key string, start, end int64, rev bool) ([]redisZ, error) {
	var res []struct {
		Score  float64
		Member any
	}

	if rev {
		zs, err := conn.client.ZRevRangeWithScores(ctx, key, start, end).Result()
		if err != nil {
			return nil, err
		}

		for _, z := range zs {
			res = append(res, struct {
				Score  float64
				Member any
			}{z.Score, z.Member})
		}
	} else {
		zs, err := conn.client.ZRangeWithScores(ctx, key, start, end).Result()
		if err != nil {
			return nil, err
		}

		for _, z := range zs {
			res = append(res, struct {
				Score  float64
				Member any
			}{z.Score, z.Member})
		}
	}

	out := make([]redisZ, len(res))
	for i, z := range res {
		m, _ := replyString(z.Member)
		out[i] = redisZ{member: m, score: z.Score}
	}

	return out, nil
}

// GetJobSchedulersCount returns the number of schedulers.
func (q *Queue) GetJobSchedulersCount(ctx context.Context) (int64, error) {
	return q.conn.client.ZCard(ctx, q.keys.Repeat()).Result()
}

// RemoveJobScheduler removes a scheduler and its next delayed job. It
// reports whether the scheduler existed.
func (q *Queue) RemoveJobScheduler(ctx context.Context, schedulerID string) (bool, error) {
	res, err := scripts.Get("removeJobScheduler").Run(ctx, q.conn.client,
		[]string{q.keys.Repeat(), q.keys.Delayed(), q.keys.Events()}, schedulerID, q.keys.KeyPrefix())
	if err != nil {
		return false, err
	}

	n, _ := replyInt(res)

	return n == 0, nil
}

// packUpdateSchedulerOpts packs ARGV[4] of updateJobScheduler from the
// options of the job that just finished.
func packUpdateSchedulerOpts(o *JobOptions) []byte {
	var m msgpack.Map

	if r := o.Repeat; r != nil {
		var rm msgpack.Map
		if r.Every > 0 {
			rm.PutInt("every", r.Every.Milliseconds())
		}

		if r.Pattern != "" {
			rm.PutStr("pattern", r.Pattern)
		}

		if r.Offset > 0 {
			rm.PutInt("offset", r.Offset.Milliseconds())
		}

		rm.PutInt("count", max(r.Count+1, 1))

		if r.Limit > 0 {
			rm.PutInt("limit", r.Limit)
		}

		if r.TZ != "" {
			rm.PutStr("tz", r.TZ)
		}

		if !r.StartDate.IsZero() {
			rm.PutInt("startDate", r.StartDate.UnixMilli())
		}

		if !r.EndDate.IsZero() {
			rm.PutInt("endDate", r.EndDate.UnixMilli())
		}

		m.PutRaw("repeat", rm.Bytes())
	} else {
		m.PutRaw("repeat", (&msgpack.Map{}).Bytes())
	}

	if o.RepeatJobKey != "" {
		m.PutStr("repeatJobKey", o.RepeatJobKey)
	}

	putTemplateOpts(&m, o)

	return m.Bytes()
}

// updateJobScheduler schedules the next iteration of a scheduler after one of
// its jobs finished.
func updateJobScheduler(ctx context.Context, conn *RedisConnection, keys QueueKeys, schedulerID, jobID string,
	data json.RawMessage, o *JobOptions,
) error {
	now := time.Now()

	var nextMillis int64

	if r := o.Repeat; r != nil {
		if r.Limit > 0 && r.Count+1 > r.Limit {
			return nil
		}

		if !r.EndDate.IsZero() && now.After(r.EndDate) {
			return nil
		}

		if r.Pattern != "" {
			ref := now
			if o.PrevMillis > now.UnixMilli() {
				ref = time.UnixMilli(o.PrevMillis)
			}

			next, err := NextCronTime(r.Pattern, ref, r.TZ, r.StartDate)
			if err != nil {
				return fmt.Errorf("bullmq: cron computation failed for scheduler '%s': %w", schedulerID, err)
			}

			nextMillis = max(next.UnixMilli(), now.UnixMilli())
		}
	}

	if len(data) == 0 {
		data = json.RawMessage("{}")
	}

	_, err := scripts.Get("updateJobScheduler").Run(ctx, conn.client,
		[]string{
			keys.Repeat(), keys.Delayed(), keys.Wait(), keys.Paused(), keys.Meta(), keys.Prioritized(),
			keys.Marker(), keys.ID(), keys.Events(), keys.PC(), keys.JobKey(jobID), keys.Active(),
		},
		nextMillis, schedulerID, []byte(data), packUpdateSchedulerOpts(o), now.UnixMilli(), keys.KeyPrefix(), jobID)

	return err
}
