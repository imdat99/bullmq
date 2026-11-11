package bullmq

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"
)

// JobState is the state a job is in.
type JobState string

// Job states. The string values match the ones Node.js BullMQ reports.
const (
	StateUnknown         JobState = "unknown"
	StateWaiting         JobState = "waiting"
	StateActive          JobState = "active"
	StateDelayed         JobState = "delayed"
	StatePrioritized     JobState = "prioritized"
	StateCompleted       JobState = "completed"
	StateFailed          JobState = "failed"
	StateWaitingChildren JobState = "waiting-children"
)

// parseJobState parses the state string returned by the getState script.
func parseJobState(s string) JobState {
	switch s {
	case "wait", "waiting":
		return StateWaiting
	case "active":
		return StateActive
	case "delayed":
		return StateDelayed
	case "prioritized":
		return StatePrioritized
	case "completed":
		return StateCompleted
	case "failed":
		return StateFailed
	case "waiting-children":
		return StateWaitingChildren
	default:
		return StateUnknown
	}
}

// RetryOptions configures [Job.Retry].
type RetryOptions struct {
	// ResetAttemptsMade resets the attemptsMade counter.
	ResetAttemptsMade bool
	// ResetAttemptsStarted resets the attemptsStarted counter.
	ResetAttemptsStarted bool
}

// KeepJobs selects which finished jobs to keep. Nil fields are unset.
type KeepJobs struct {
	// Age is the maximum age in seconds of the jobs to keep.
	Age *int64 `json:"age,omitempty"`
	// Count is the maximum number of jobs to keep.
	Count *int64 `json:"count,omitempty"`
	// Limit bounds how many jobs are removed per eviction pass (the Lua
	// scripts default to 1000).
	Limit *int64 `json:"limit,omitempty"`
}

// RemoveOnFinish is a removeOnComplete/removeOnFail retention policy. Build
// one with [RemoveAll], [KeepAll], [KeepLast] or [KeepJobsPolicy].
type RemoveOnFinish struct {
	kind  removeKind
	count int64
	keep  KeepJobs
}

type removeKind uint8

const (
	removeBool removeKind = iota + 1
	removeCount
	removeOptions
)

// RemoveAll removes the job as soon as it finishes (removeOnX: true).
func RemoveAll() *RemoveOnFinish { return &RemoveOnFinish{kind: removeBool, count: 1} }

// KeepAll keeps every finished job (removeOnX: false).
func KeepAll() *RemoveOnFinish { return &RemoveOnFinish{kind: removeBool, count: 0} }

// KeepLast keeps at most n finished jobs (removeOnX: n).
func KeepLast(n int64) *RemoveOnFinish { return &RemoveOnFinish{kind: removeCount, count: n} }

// KeepJobsPolicy keeps finished jobs matching k (removeOnX: {age, count, limit}).
func KeepJobsPolicy(k KeepJobs) *RemoveOnFinish { return &RemoveOnFinish{kind: removeOptions, keep: k} }

// RemoveOnFinishBool is the boolean form: true removes immediately.
func RemoveOnFinishBool(remove bool) *RemoveOnFinish {
	if remove {
		return RemoveAll()
	}

	return KeepAll()
}

// IsBool reports the boolean form and its value.
func (r *RemoveOnFinish) IsBool() (value, ok bool) {
	return r.kind == removeBool && r.count == 1, r.kind == removeBool
}

// Count reports the count form and its value.
func (r *RemoveOnFinish) Count() (int64, bool) { return r.count, r.kind == removeCount }

// KeepJobs reports the options form and its value.
func (r *RemoveOnFinish) KeepJobs() (KeepJobs, bool) { return r.keep, r.kind == removeOptions }

// MarshalJSON encodes the policy as Node.js does: bool, number or object.
func (r *RemoveOnFinish) MarshalJSON() ([]byte, error) {
	switch r.kind {
	case removeCount:
		return json.Marshal(r.count)
	case removeOptions:
		return json.Marshal(r.keep)
	default:
		return json.Marshal(r.count == 1)
	}
}

// UnmarshalJSON decodes a bool, number or object.
func (r *RemoveOnFinish) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	switch {
	case bytes.Equal(b, []byte("true")):
		*r = *RemoveAll()
	case bytes.Equal(b, []byte("false")), bytes.Equal(b, []byte("null")):
		*r = *KeepAll()
	case len(b) > 0 && b[0] == '{':
		var k KeepJobs
		if err := json.Unmarshal(b, &k); err != nil {
			return err
		}

		*r = *KeepJobsPolicy(k)
	default:
		var f float64
		if err := json.Unmarshal(b, &f); err != nil {
			return fmt.Errorf("bullmq: invalid remove-on-finish policy %s: %w", b, err)
		}

		*r = *KeepLast(int64(f))
	}

	return nil
}

// Built-in backoff types.
const (
	BackoffFixed       = "fixed"
	BackoffExponential = "exponential"
)

// BackoffStrategy is the retry backoff of a job. Type is "fixed",
// "exponential", or the name of a custom strategy handled by
// [WorkerOptions.BackoffStrategy].
type BackoffStrategy struct {
	Type  string
	Delay time.Duration
}

// FixedBackoff retries after a constant delay.
func FixedBackoff(delay time.Duration) *BackoffStrategy {
	return &BackoffStrategy{Type: BackoffFixed, Delay: delay}
}

// ExponentialBackoff retries after delay * 2^(attempt-1).
func ExponentialBackoff(delay time.Duration) *BackoffStrategy {
	return &BackoffStrategy{Type: BackoffExponential, Delay: delay}
}

// CustomBackoff delegates the delay computation to the worker's
// [WorkerOptions.BackoffStrategy].
func CustomBackoff(name string) *BackoffStrategy { return &BackoffStrategy{Type: name} }

// IsCustom reports whether the strategy is not one of the built-in types.
func (b *BackoffStrategy) IsCustom() bool {
	return b.Type != BackoffFixed && b.Type != BackoffExponential
}

type backoffJSON struct {
	Type  string `json:"type"`
	Delay int64  `json:"delay"`
}

// MarshalJSON encodes {"type": ..., "delay": ms}.
func (b *BackoffStrategy) MarshalJSON() ([]byte, error) {
	delay := b.Delay.Milliseconds()
	if b.IsCustom() {
		delay = 0
	}

	return json.Marshal(backoffJSON{Type: b.Type, Delay: delay})
}

// UnmarshalJSON decodes {"type": ..., "delay": ms} or a bare number (fixed).
func (b *BackoffStrategy) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) > 0 && data[0] != '{' {
		var ms float64
		if err := json.Unmarshal(data, &ms); err != nil {
			return err
		}

		*b = BackoffStrategy{Type: BackoffFixed, Delay: time.Duration(ms) * time.Millisecond}

		return nil
	}

	var raw struct {
		Type  string  `json:"type"`
		Delay float64 `json:"delay"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	*b = BackoffStrategy{Type: raw.Type, Delay: time.Duration(raw.Delay) * time.Millisecond}

	return nil
}

// JobCounts is the number of jobs in each state.
type JobCounts struct {
	Waiting         int64
	Active          int64
	Delayed         int64
	Prioritized     int64
	Completed       int64
	Failed          int64
	WaitingChildren int64
	Paused          int64
}

// MetricsMeta is the metadata of a time-series metric.
type MetricsMeta struct {
	// Count is the total number of jobs counted since collection began.
	Count int64
	// PrevTS is the timestamp (ms) of the previous data point.
	PrevTS int64
	// PrevCount is the cumulative count at the previous data point.
	PrevCount int64
}

// Metrics is a per-minute time series of completed or failed jobs.
type Metrics struct {
	Meta MetricsMeta
	// Data holds per-minute data points, newest first.
	Data []int64
	// Count is the total number of data points available.
	Count int64
}

// ParentKeys identifies a parent job.
type ParentKeys struct {
	// QueueKey is the parent's qualified queue key (`{prefix}:{queue}`).
	QueueKey string `json:"queueKey"`
	// ID is the parent job id.
	ID string `json:"id"`
}

// QueueMeta is the public metadata stored in the queue's meta hash.
type QueueMeta struct {
	// Concurrency is the global concurrency limit, 0 when unset.
	Concurrency *int64
	// Max is the global rate limit max, nil when unset.
	Max *int64
	// Duration is the global rate limit window in ms, nil when unset.
	Duration *int64
	// MaxLenEvents is the events stream max length, nil when unset.
	MaxLenEvents *int64
	// Paused reports whether the queue is paused.
	Paused bool
	// Other holds every other field of the meta hash (for example `library`).
	Other map[string]string
}

// DependenciesCount is the number of children of a parent job by type.
type DependenciesCount struct {
	// Processed is the number of children that completed.
	Processed int64
	// Unprocessed is the number of children still pending.
	Unprocessed int64
	// Ignored is the number of children that failed with ignoreDependencyOnFailure.
	Ignored int64
	// Failed is the number of children that failed (unsuccessful set).
	Failed int64
}

// DependenciesResult is a page of a parent job's dependencies.
type DependenciesResult struct {
	// Processed maps child job keys to their parsed return values.
	Processed map[string]any
	// NextProcessedCursor is the HSCAN cursor for the next page (0 = done).
	NextProcessedCursor uint64
	// Unprocessed lists pending child job keys.
	Unprocessed []string
	// NextUnprocessedCursor is the SSCAN cursor for the next page (0 = done).
	NextUnprocessedCursor uint64
}

// Ptr returns a pointer to v. It is handy for optional option fields.
func Ptr[T any](v T) *T { return new(v) }
