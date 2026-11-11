package bullmq

import (
	"encoding/base64"
	"math"
	"strconv"
	"strings"
	"unicode"
)

// DefaultPrefix is the default key prefix, compatible with Node.js BullMQ.
const DefaultPrefix = "bull"

// validateQueueName applies the same separator restriction as BullMQ Node.js.
func validateQueueName(name string) error {
	if name == "" {
		return invalidConfig("Queue name must be provided")
	}

	if strings.Contains(name, ":") {
		return invalidConfig("Queue name cannot contain :")
	}

	return nil
}

// validatePrefix ensures the prefix is non-empty and free of ':'. Together
// with the same restriction on queue names this makes a qualified job key
// (`{prefix}:{queue}:{jobId}`) unambiguous to parse back.
func validatePrefix(prefix string) error {
	if prefix == "" || strings.Contains(prefix, ":") {
		return invalidConfig("Prefix must be non-empty and cannot contain :")
	}

	return nil
}

// validateCustomJobID mirrors `Job.validateOptions` in BullMQ Node.js: integer
// ids are rejected (they would collide with automatic ids) and ':' is only
// allowed in legacy three-segment repeatable ids.
func validateCustomJobID(jobID string) error {
	if jobID == "" {
		return nil
	}

	if isCanonicalInteger(jobID) {
		return invalidConfig("Custom Id cannot be integers")
	}

	if strings.Contains(jobID, ":") && len(strings.Split(jobID, ":")) != 3 {
		return invalidConfig("Custom Id cannot contain :")
	}

	return nil
}

// isCanonicalInteger emulates Node.js `${parseInt(value, 10)}` === value.
func isCanonicalInteger(value string) bool {
	s, ok := jsNumberToString(jsParseIntRadix10(value))
	return ok && s == value
}

// jsParseIntRadix10 emulates JavaScript parseInt(value, 10).
func jsParseIntRadix10(value string) float64 {
	rest := strings.TrimLeftFunc(value, func(r rune) bool {
		return unicode.IsSpace(r) || r == 0xFEFF
	})
	negative := false

	switch {
	case strings.HasPrefix(rest, "-"):
		negative = true
		rest = rest[1:]
	case strings.HasPrefix(rest, "+"):
		rest = rest[1:]
	}

	n := 0
	for n < len(rest) && rest[n] >= '0' && rest[n] <= '9' {
		n++
	}

	if n == 0 {
		return math.NaN()
	}

	magnitude, err := strconv.ParseFloat(rest[:n], 64)
	if err != nil { // only out-of-range, which ParseFloat reports as ±Inf
		magnitude = math.Inf(1)
	}

	if negative {
		return -magnitude
	}

	return magnitude
}

// jsNumberToString emulates `${n}` for the integral values parseInt returns.
// It reports false for magnitudes >= 1e21, where JavaScript switches to
// exponential notation and the round trip can never succeed.
func jsNumberToString(v float64) (string, bool) {
	switch {
	case math.IsNaN(v):
		return "NaN", true
	case math.IsInf(v, 1):
		return "Infinity", true
	case math.IsInf(v, -1):
		return "-Infinity", true
	case v == 0:
		return "0", true
	case math.Abs(v) >= 1e21:
		return "", false
	}

	return strconv.FormatFloat(v, 'f', -1, 64), true
}

// resolveParentQueueKey resolves ParentOptions.Queue into a qualified queue
// key. It accepts an unqualified queue name or a `{prefix}:{queue}` key for the
// current prefix.
func resolveParentQueueKey(prefix, queue string) (string, error) {
	if name, ok := strings.CutPrefix(queue, prefix+":"); ok {
		if err := validateQueueName(name); err != nil {
			return "", err
		}

		return queue, nil
	}

	if err := validateQueueName(queue); err != nil {
		return "", err
	}

	return prefix + ":" + queue, nil
}

// QueueKeys builds the Redis keys of a queue: `{prefix}:{name}:{suffix}`.
//
// The well-known keys are computed once by [NewQueueKeys] and shared by every
// copy, so the hot paths (one script call per job, each taking up to 14 keys)
// do not rebuild them. Use [NewQueueKeys]; the zero value is usable but falls
// back to computing keys on every call.
type QueueKeys struct {
	prefix string
	name   string
	c      *keyCache
}

// keyCache holds the precomputed, immutable key strings of a queue.
type keyCache struct {
	// Pre-boxed key lists of the hot scripts (see scripts.Script.RunArgs).
	moveToActiveKeys   []any // the 11 keys of moveToActive
	moveToFinishedBase []any // the first 10 keys of moveToFinished
	addStandardKeys    []any
	addDelayedKeys     []any
	addPrioritizedKeys []any
	boxed              struct{ keyPrefix, completed, failed, metricsCompleted, metricsFailed, marker any }

	base, keyPrefix                 string
	metricsCompleted, metricsFailed string
	waitKey                         string
	activeKey                       string
	delayedKey                      string
	prioritizedKey                  string
	completedKey                    string
	failedKey                       string
	pausedKey                       string
	waitingChildrenKey              string
	stalledKey                      string
	stalledCheckKey                 string
	limiterKey                      string
	eventsKey                       string
	metaKey                         string
	markerKey                       string
	pCKey                           string
	iDKey                           string
	repeatKey                       string
}

func newKeyCache(prefix, name string) *keyCache {
	kp := prefix + ":" + name + ":"

	c := &keyCache{
		base:               prefix + ":" + name,
		keyPrefix:          kp,
		metricsCompleted:   kp + "metrics:completed",
		metricsFailed:      kp + "metrics:failed",
		waitKey:            kp + "wait",
		activeKey:          kp + "active",
		delayedKey:         kp + "delayed",
		prioritizedKey:     kp + "prioritized",
		completedKey:       kp + "completed",
		failedKey:          kp + "failed",
		pausedKey:          kp + "paused",
		waitingChildrenKey: kp + "waiting-children",
		stalledKey:         kp + "stalled",
		stalledCheckKey:    kp + "stalled-check",
		limiterKey:         kp + "limiter",
		eventsKey:          kp + "events",
		metaKey:            kp + "meta",
		markerKey:          kp + "marker",
		pCKey:              kp + "pc",
		iDKey:              kp + "id",
		repeatKey:          kp + "repeat",
	}

	c.moveToActiveKeys = []any{
		c.waitKey, c.activeKey, c.prioritizedKey, c.eventsKey, c.stalledKey,
		c.limiterKey, c.delayedKey, c.pausedKey, c.metaKey, c.pCKey, c.markerKey,
	}
	c.moveToFinishedBase = []any{
		c.waitKey, c.activeKey, c.prioritizedKey, c.eventsKey, c.stalledKey,
		c.limiterKey, c.delayedKey, c.pausedKey, c.metaKey, c.pCKey,
	}
	c.addStandardKeys = []any{
		c.waitKey, c.pausedKey, c.metaKey, c.iDKey, c.completedKey, c.delayedKey, c.activeKey, c.eventsKey, c.markerKey,
	}
	c.addDelayedKeys = []any{c.markerKey, c.metaKey, c.iDKey, c.delayedKey, c.completedKey, c.eventsKey}
	c.addPrioritizedKeys = []any{
		c.markerKey, c.metaKey, c.iDKey, c.prioritizedKey, c.delayedKey, c.completedKey, c.activeKey, c.eventsKey, c.pCKey,
	}
	c.boxed.keyPrefix, c.boxed.completed, c.boxed.failed = c.keyPrefix, c.completedKey, c.failedKey
	c.boxed.metricsCompleted, c.boxed.metricsFailed, c.boxed.marker = c.metricsCompleted, c.metricsFailed, c.markerKey

	return c
}

// NewQueueKeys returns the key builder for queue name. An empty prefix selects
// [DefaultPrefix].
func NewQueueKeys(name, prefix string) QueueKeys {
	if prefix == "" {
		prefix = DefaultPrefix
	}

	return QueueKeys{prefix: prefix, name: name, c: newKeyCache(prefix, name)}
}

func (k QueueKeys) cache() *keyCache {
	if k.c != nil {
		return k.c
	}

	return newKeyCache(k.prefix, k.name)
}

// Base returns `{prefix}:{name}`.
func (k QueueKeys) Base() string { return k.cache().base }

// KeyPrefix returns `{prefix}:{name}:`, the prefix of every job key.
func (k QueueKeys) KeyPrefix() string { return k.cache().keyPrefix }

// JobKey returns `{prefix}:{name}:{jobID}`.
func (k QueueKeys) JobKey(jobID string) string { return k.KeyPrefix() + jobID }

// Get returns the key with the given suffix.
func (k QueueKeys) Get(suffix string) string { return k.KeyPrefix() + suffix }

// Wait returns the wait list key.
func (k QueueKeys) Wait() string { return k.cache().waitKey }

// Active returns the active list key.
func (k QueueKeys) Active() string { return k.cache().activeKey }

// Delayed returns the delayed sorted-set key.
func (k QueueKeys) Delayed() string { return k.cache().delayedKey }

// Prioritized returns the prioritized sorted-set key.
func (k QueueKeys) Prioritized() string { return k.cache().prioritizedKey }

// Completed returns the completed sorted-set key.
func (k QueueKeys) Completed() string { return k.cache().completedKey }

// Failed returns the failed sorted-set key.
func (k QueueKeys) Failed() string { return k.cache().failedKey }

// Paused returns the paused list key.
func (k QueueKeys) Paused() string { return k.cache().pausedKey }

// WaitingChildren returns the waiting-children sorted-set key.
func (k QueueKeys) WaitingChildren() string { return k.cache().waitingChildrenKey }

// Stalled returns the stalled set key.
func (k QueueKeys) Stalled() string { return k.cache().stalledKey }

// StalledCheck returns the stalled-check key.
func (k QueueKeys) StalledCheck() string { return k.cache().stalledCheckKey }

// Limiter returns the rate limiter key.
func (k QueueKeys) Limiter() string { return k.cache().limiterKey }

// Events returns the events stream key.
func (k QueueKeys) Events() string { return k.cache().eventsKey }

// Meta returns the queue meta hash key.
func (k QueueKeys) Meta() string { return k.cache().metaKey }

// Marker returns the marker key used to wake blocked workers.
func (k QueueKeys) Marker() string { return k.cache().markerKey }

// PC returns the priority counter key.
func (k QueueKeys) PC() string { return k.cache().pCKey }

// ID returns the job id counter key.
func (k QueueKeys) ID() string { return k.cache().iDKey }

// Repeat returns the job scheduler sorted-set key.
func (k QueueKeys) Repeat() string { return k.cache().repeatKey }

// Dedup returns the deduplication key for id.
func (k QueueKeys) Dedup(id string) string { return k.Get("de:" + id) }

// Prefix returns the key prefix.
func (k QueueKeys) Prefix() string { return k.prefix }

// Name returns the queue name.
func (k QueueKeys) Name() string { return k.name }

// ClientName returns the Redis client name used by workers:
// `{prefix}:{base64(name)}{suffix}`, matching Node.js so that
// [Queue.GetWorkers] discovers workers across implementations.
func (k QueueKeys) ClientName(suffix string) string {
	return k.prefix + ":" + base64.StdEncoding.EncodeToString([]byte(k.name)) + suffix
}

// splitJobKey is a `{prefix}:{queueName}:{jobId}` split of a qualified key.
type splitJobKey struct {
	prefix, queueName, jobID string
}

// splitQualifiedJobKey splits a qualified job key. Prefixes and queue names
// cannot contain ':', so everything after the second separator is the job id.
func splitQualifiedJobKey(key string) (splitJobKey, bool) {
	prefix, rest, ok := strings.Cut(key, ":")
	if !ok {
		return splitJobKey{}, false
	}

	queueName, jobID, ok := strings.Cut(rest, ":")
	if !ok || prefix == "" || queueName == "" || jobID == "" {
		return splitJobKey{}, false
	}

	return splitJobKey{prefix: prefix, queueName: queueName, jobID: jobID}, true
}
