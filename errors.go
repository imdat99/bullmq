package bullmq

import (
	"errors"
	"fmt"
	"time"
)

// Sentinel errors. Use [errors.Is] to test for them; most are returned wrapped
// with additional context.
var (
	// ErrInvalidConfig reports invalid options or arguments.
	ErrInvalidConfig = errors.New("invalid configuration")
	// ErrJobNotFound reports that a job does not exist in Redis.
	ErrJobNotFound = errors.New("job not found")
	// ErrJobLocked reports that a job lock could not be acquired or extended.
	ErrJobLocked = errors.New("job is locked")
	// ErrWorkerClosed reports an operation on a closed worker.
	ErrWorkerClosed = errors.New("worker closed")
	// ErrConnectionClosed reports an operation on a closed connection.
	ErrConnectionClosed = errors.New("connection closed")
	// ErrJobLockNotExist reports that the job lock is missing or was lost.
	ErrJobLockNotExist = errors.New("job lock does not exist")
	// ErrJobNotInState reports that a job is not in the expected state.
	ErrJobNotInState = errors.New("job is not in the expected state")
	// ErrNoScriptContext reports a job method called on a job that is not
	// bound to a queue (for example a zero-value [Job]).
	ErrNoScriptContext = errors.New("job has no script context")

	// ErrDelayed must be returned by a processor after it moved the job to the
	// delayed set with [Job.MoveToDelayed]. The worker then leaves the job
	// alone instead of completing or failing it.
	ErrDelayed = errors.New("delayed")
	// ErrWaitingChildren must be returned by a processor after it moved the job
	// to waiting-children with [Job.MoveToWaitingChildren].
	ErrWaitingChildren = errors.New("waiting-children")
)

// BullMQ protocol error codes returned by the Lua scripts.
const (
	CodeJobNotExist               int64 = -1
	CodeJobLockNotExist           int64 = -2
	CodeJobNotInState             int64 = -3
	CodeJobPendingDependencies    int64 = -4
	CodeParentJobNotExist         int64 = -5
	CodeJobLockMismatch           int64 = -6
	CodeParentJobCannotBeReplaced int64 = -7
	CodeJobBelongsToJobScheduler  int64 = -8
	CodeJobHasFailedChildren      int64 = -9
	CodeSchedulerJobIDCollision   int64 = -10
	CodeSchedulerJobSlotsBusy     int64 = -11
)

// ScriptError is a BullMQ protocol error returned by a Lua script.
type ScriptError struct {
	Code    int64
	Message string
}

func (e *ScriptError) Error() string {
	return fmt.Sprintf("script error: %s (code: %d)", e.Message, e.Code)
}

// NewScriptError builds a [ScriptError] with the canonical message for code.
func NewScriptError(code int64) *ScriptError {
	var msg string

	switch code {
	case CodeJobNotExist:
		msg = "Job does not exist"
	case CodeJobLockNotExist:
		msg = "Job lock does not exist"
	case CodeJobNotInState:
		msg = "Job is not in the expected state"
	case CodeJobPendingDependencies:
		msg = "Job has pending dependencies"
	case CodeParentJobNotExist:
		msg = "Parent job does not exist"
	case CodeJobLockMismatch:
		msg = "Job lock mismatch"
	case CodeParentJobCannotBeReplaced:
		msg = "Parent job cannot be replaced"
	case CodeJobBelongsToJobScheduler:
		msg = "Job belongs to a job scheduler"
	case CodeJobHasFailedChildren:
		msg = "Job has failed children"
	case CodeSchedulerJobIDCollision:
		msg = "Scheduler job id collision"
	case CodeSchedulerJobSlotsBusy:
		msg = "Scheduler job slots are busy"
	default:
		msg = "Unknown script error"
	}

	return &ScriptError{Code: code, Message: msg}
}

// ScriptErrorCode returns the protocol code of a [ScriptError] wrapped in err.
func ScriptErrorCode(err error) (int64, bool) {
	if se, ok := errors.AsType[*ScriptError](err); ok {
		return se.Code, true
	}

	return 0, false
}

// UnrecoverableError fails a job immediately, skipping any remaining
// attempts. Its message is stored verbatim as the job's failedReason,
// matching Node.js `UnrecoverableError`.
type UnrecoverableError struct {
	Message string
}

func (e *UnrecoverableError) Error() string { return e.Message }

// NewUnrecoverableError returns an error that fails the job without retries.
func NewUnrecoverableError(msg string) error { return &UnrecoverableError{Message: msg} }

// IsUnrecoverable reports whether err (or an error it wraps) is an
// [UnrecoverableError].
func IsUnrecoverable(err error) bool {
	var ue *UnrecoverableError
	return errors.As(err, &ue)
}

// RateLimitError is returned by a processor (after calling [Queue.RateLimit])
// to move the job back to wait instead of failing it.
type RateLimitError struct {
	Delay time.Duration
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("rate limited (wait %dms)", e.Delay.Milliseconds())
}

// NewRateLimitError returns the error a processor returns to signal that the
// job must be moved back to wait because the queue is rate limited.
func NewRateLimitError(delay time.Duration) error { return &RateLimitError{Delay: delay} }

func invalidConfig(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidConfig, fmt.Sprintf(format, args...))
}
