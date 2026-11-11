package bullmq

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// TLSCerts are PEM-encoded certificates used to connect to Redis over TLS.
// Setting them implies a TLS connection.
type TLSCerts struct {
	// RootCert is a root CA used when the server CA is not in the system pool.
	RootCert []byte
	// ClientCert and ClientKey enable mutual TLS.
	ClientCert []byte
	ClientKey  []byte
}

// String redacts the certificate material.
func (c *TLSCerts) String() string { return "TLSCerts{***}" }

// RedisConnectionOptions configures the Redis connection.
//
// The zero value connects to redis://127.0.0.1:6379.
type RedisConnectionOptions struct {
	// URL is used when Host is empty. Use a rediss:// URL for TLS.
	URL string
	// MaxConnections is the connection pool size (default 4... at least 4).
	MaxConnections int
	// Host, when set, builds the URL from the typed fields below.
	Host string
	// Port defaults to 6379 when Host is set.
	Port int
	// Username for ACL authentication.
	Username string
	// Password for authentication.
	Password string
	// DB is the logical database to select (nil keeps the URL's database).
	DB *int
	// TLS upgrades the connection to TLS (rediss://).
	TLS bool
	// TLSCerts supplies a custom root CA and/or a client certificate.
	TLSCerts *TLSCerts
	// ClusterNodes, when non-empty, selects a Redis Cluster client seeded
	// with these node URLs. URL, Host, Port and DB are then ignored.
	//
	// Every key of a queue must hash to the same slot: use a hash tag in the
	// prefix (e.g. "{bull}") or in the queue name.
	ClusterNodes []string
}

// DefaultRedisURL is the URL used when no connection is configured.
const DefaultRedisURL = "redis://127.0.0.1:6379"

// IsCluster reports whether the options select a Redis Cluster.
func (o RedisConnectionOptions) IsCluster() bool { return len(o.ClusterNodes) > 0 }

// EffectiveURL returns the URL actually used to connect.
//
// When Host is set, the URL is built from the typed fields
// (`scheme://[user][:pass]@host:port[/db]`). Otherwise URL is returned (or the
// default URL), upgraded to rediss:// when TLS or TLSCerts is set.
func (o RedisConnectionOptions) EffectiveURL() string {
	useTLS := o.TLS || o.TLSCerts != nil
	if o.Host == "" {
		u := o.URL
		if u == "" {
			u = DefaultRedisURL
		}

		if useTLS && strings.HasPrefix(u, "redis://") {
			return "rediss://" + strings.TrimPrefix(u, "redis://")
		}

		return u
	}

	scheme := "redis"
	if useTLS {
		scheme = "rediss"
	}

	var auth string

	switch {
	case o.Username != "" && o.Password != "":
		auth = encodeUserinfo(o.Username) + ":" + encodeUserinfo(o.Password) + "@"
	case o.Password != "":
		auth = ":" + encodeUserinfo(o.Password) + "@"
	case o.Username != "":
		auth = encodeUserinfo(o.Username) + "@"
	}

	port := o.Port
	if port == 0 {
		port = 6379
	}

	host := o.Host
	if strings.Contains(host, ":") && (!strings.HasPrefix(host, "[") || !strings.HasSuffix(host, "]")) {
		host = "[" + host + "]"
	}

	var db string
	if o.DB != nil {
		db = "/" + strconv.Itoa(*o.DB)
	}

	return fmt.Sprintf("%s://%s%s:%d%s", scheme, auth, host, port, db)
}

// String redacts credentials.
func (o RedisConnectionOptions) String() string {
	nodes := make([]string, len(o.ClusterNodes))
	for i, n := range o.ClusterNodes {
		nodes[i] = redactURLUserinfo(n)
	}

	return fmt.Sprintf("RedisConnectionOptions{URL:%q Host:%q Port:%d DB:%v TLS:%v Cluster:%v}",
		redactURLUserinfo(o.URL), o.Host, o.Port, o.DB, o.TLS || o.TLSCerts != nil, nodes)
}

func encodeUserinfo(s string) string {
	var b strings.Builder

	for i := range len(s) {
		c := s[i]
		if ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z') || ('0' <= c && c <= '9') ||
			c == '-' || c == '.' || c == '_' || c == '~' {
			b.WriteByte(c)
			continue
		}

		fmt.Fprintf(&b, "%%%02X", c)
	}

	return b.String()
}

// redactURLUserinfo replaces the userinfo of a URL with "***".
func redactURLUserinfo(u string) string {
	scheme, rest, ok := strings.Cut(u, "://")
	if !ok {
		return u
	}

	end := strings.IndexAny(rest, "/?#")
	if end < 0 {
		end = len(rest)
	}

	authority := rest[:end]

	at := strings.LastIndex(authority, "@")
	if at < 0 {
		return u
	}

	return scheme + "://***@" + authority[at+1:] + rest[end:]
}

// QueueOptions configures a [Queue].
type QueueOptions struct {
	// Connection configures Redis.
	Connection RedisConnectionOptions
	// Prefix is the key prefix (default "bull").
	Prefix string
	// DefaultJobOptions are merged into every job added to the queue. Only
	// Attempts, Backoff, RemoveOnComplete, RemoveOnFail, Delay, Priority and
	// LIFO are applied as defaults.
	DefaultJobOptions JobOptions
	// SkipVersionCheck is accepted for API parity; no check is performed.
	SkipVersionCheck bool
}

// MetricsOptions enables per-minute metrics collection on a worker.
type MetricsOptions struct {
	// MaxDataPoints is the number of minute data points to keep (default two
	// weeks).
	MaxDataPoints int
}

// DefaultMetricsMaxDataPoints is two weeks of minute data points.
const DefaultMetricsMaxDataPoints = 2 * 7 * 24 * 60

// RateLimiterOptions limits a worker to Max jobs per Duration.
type RateLimiterOptions struct {
	Max      int64
	Duration time.Duration
}

// BackoffFunc computes the delay of a custom backoff strategy. attemptsMade
// counts the attempt that just failed (1 for the first failure). A negative
// delay fails the job without further retries.
type BackoffFunc func(attemptsMade int, backoffType string, err error, job *Job) time.Duration

// WorkerOptions configures a [Worker]. Zero fields select the defaults
// documented on each field.
type WorkerOptions struct {
	// Connection configures Redis.
	Connection RedisConnectionOptions
	// Prefix is the key prefix (default "bull").
	Prefix string
	// Name is stored on processed jobs (`processedBy`) and in the client name.
	Name string
	// Concurrency is the number of jobs processed in parallel (default 1).
	Concurrency int
	// LockDuration is the job lock TTL (default 30s).
	LockDuration time.Duration
	// LockRenewTime is the lock renewal interval (default LockDuration/2).
	LockRenewTime time.Duration
	// MaxStalledCount is how many times a stalled job is re-queued before it
	// fails (default 1 when nil).
	MaxStalledCount *int
	// StalledInterval is the period of the stalled-jobs check (default 30s).
	StalledInterval time.Duration
	// DrainDelay is how long the worker blocks waiting for jobs when the queue
	// is empty (default 5s).
	DrainDelay time.Duration
	// ManualStart disables the automatic start; call [Worker.Run].
	ManualStart bool
	// SkipVersionCheck is accepted for API parity; no check is performed.
	SkipVersionCheck bool
	// RemoveOnComplete is the default completed-job retention policy.
	RemoveOnComplete *RemoveOnFinish
	// RemoveOnFail is the default failed-job retention policy.
	RemoveOnFail *RemoveOnFinish
	// RunRetryDelay is the pause after a transient Redis error (default 15s).
	RunRetryDelay time.Duration
	// BackoffStrategy computes delays for custom backoff types.
	BackoffStrategy BackoffFunc
	// Limiter rate limits job processing.
	Limiter *RateLimiterOptions
	// MaximumRateLimitDelay caps the wait when rate limited (default 30s).
	MaximumRateLimitDelay time.Duration
	// MaxStartedAttempts fails a job once it has been started more than this
	// many times (nil disables the check).
	MaxStartedAttempts *int
	// SkipStalledCheck disables the stalled-jobs check for this worker.
	SkipStalledCheck bool
	// SkipLockRenewal disables lock renewal for this worker.
	SkipLockRenewal bool
	// Metrics enables per-minute completed/failed metrics.
	Metrics *MetricsOptions
}

// Worker option defaults.
const (
	DefaultLockDuration          = 30 * time.Second
	DefaultStalledInterval       = 30 * time.Second
	DefaultDrainDelay            = 5 * time.Second
	DefaultRunRetryDelay         = 15 * time.Second
	DefaultMaximumRateLimitDelay = 30 * time.Second
	DefaultMaxStalledCount       = 1
)

// withDefaults returns a copy with zero fields replaced by their defaults.
func (o WorkerOptions) withDefaults() WorkerOptions {
	if o.Prefix == "" {
		o.Prefix = DefaultPrefix
	}

	if o.Concurrency == 0 {
		o.Concurrency = 1
	}

	if o.LockDuration == 0 {
		o.LockDuration = DefaultLockDuration
	}

	if o.LockRenewTime <= 0 {
		o.LockRenewTime = o.LockDuration / 2
	}

	if o.LockRenewTime < time.Millisecond {
		o.LockRenewTime = time.Millisecond
	}

	if o.MaxStalledCount == nil {
		o.MaxStalledCount = new(DefaultMaxStalledCount)
	}

	if o.StalledInterval == 0 {
		o.StalledInterval = DefaultStalledInterval
	}

	if o.DrainDelay == 0 {
		o.DrainDelay = DefaultDrainDelay
	}

	if o.RunRetryDelay == 0 {
		o.RunRetryDelay = DefaultRunRetryDelay
	}

	if o.MaximumRateLimitDelay == 0 {
		o.MaximumRateLimitDelay = DefaultMaximumRateLimitDelay
	}

	if o.Metrics != nil && o.Metrics.MaxDataPoints <= 0 {
		m := *o.Metrics
		m.MaxDataPoints = DefaultMetricsMaxDataPoints
		o.Metrics = &m
	}

	return o
}

func (o WorkerOptions) validate() error {
	if o.Concurrency < 0 {
		return invalidConfig("concurrency must be a finite number greater than 0")
	}

	if o.StalledInterval < 0 {
		return invalidConfig("stalledInterval must be greater than 0")
	}

	if o.DrainDelay < 0 {
		return invalidConfig("drainDelay must be greater than 0")
	}

	if o.LockDuration < 0 {
		return invalidConfig("lockDuration must be greater than 0")
	}

	if o.MaxStalledCount != nil && *o.MaxStalledCount < 0 {
		return invalidConfig("maxStalledCount must be greater than or equal to 0")
	}

	if o.Limiter != nil && (o.Limiter.Max <= 0 || o.Limiter.Duration <= 0) {
		return invalidConfig("limiter max and duration must be greater than 0")
	}

	return nil
}

// ParentOptions attaches a job to a parent (flows).
type ParentOptions struct {
	// Queue is the parent queue name, or `{prefix}:{name}`.
	Queue string `json:"queue"`
	// ID is the parent job id.
	ID string `json:"id"`
}

// DeduplicationOptions deduplicates jobs sharing ID.
type DeduplicationOptions struct {
	// ID is the deduplication identifier.
	ID string `json:"id"`
	// TTL of the deduplication key (0 = until the job finishes).
	TTL time.Duration `json:"-"`
	// Extend extends the TTL when a duplicate is added.
	Extend bool `json:"extend,omitempty"`
	// Replace replaces the existing (delayed) job with the new one.
	Replace bool `json:"replace,omitempty"`
	// Reschedule applies the new delay to the existing delayed job.
	Reschedule bool `json:"reschedule,omitempty"`
	// KeepLastIfActive stores the duplicate while the job is active and adds
	// it once the active job finishes.
	KeepLastIfActive bool `json:"keepLastIfActive,omitempty"`
}

// RepeatOptions configures a job scheduler (and is stored on the jobs it
// produces).
type RepeatOptions struct {
	// Pattern is a cron pattern (seconds field optional). Exclusive with Every.
	Pattern string
	// Every repeats at a fixed interval. Exclusive with Pattern.
	Every time.Duration
	// Limit is the maximum number of iterations (0 = unlimited).
	Limit int64
	// Immediately runs the first iteration right away (cron only).
	Immediately bool
	// Offset shifts every iteration.
	Offset time.Duration
	// StartDate is the earliest execution time.
	StartDate time.Time
	// EndDate is the latest execution time.
	EndDate time.Time
	// TZ is an IANA time zone for Pattern.
	TZ string
	// Count is the current iteration (internal).
	Count int64
}

type repeatJSON struct {
	Pattern     string  `json:"pattern,omitempty"`
	Every       float64 `json:"every,omitempty"`
	Limit       float64 `json:"limit,omitempty"`
	Immediately bool    `json:"immediately,omitempty"`
	Offset      float64 `json:"offset,omitempty"`
	StartDate   float64 `json:"startDate,omitempty"`
	EndDate     float64 `json:"endDate,omitempty"`
	TZ          string  `json:"tz,omitempty"`
	Count       float64 `json:"count,omitempty"`
}

// MarshalJSON encodes durations and dates as milliseconds.
func (r RepeatOptions) MarshalJSON() ([]byte, error) {
	return json.Marshal(repeatJSON{
		Pattern:     r.Pattern,
		Every:       float64(r.Every.Milliseconds()),
		Limit:       float64(r.Limit),
		Immediately: r.Immediately,
		Offset:      float64(r.Offset.Milliseconds()),
		StartDate:   float64(timeToMillis(r.StartDate)),
		EndDate:     float64(timeToMillis(r.EndDate)),
		TZ:          r.TZ,
		Count:       float64(r.Count),
	})
}

// UnmarshalJSON decodes durations and dates expressed in milliseconds.
func (r *RepeatOptions) UnmarshalJSON(b []byte) error {
	var raw repeatJSON
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}

	*r = RepeatOptions{
		Pattern:     raw.Pattern,
		Every:       msToDuration(raw.Every),
		Limit:       int64(raw.Limit),
		Immediately: raw.Immediately,
		Offset:      msToDuration(raw.Offset),
		StartDate:   millisToTime(int64(raw.StartDate)),
		EndDate:     millisToTime(int64(raw.EndDate)),
		TZ:          raw.TZ,
		Count:       int64(raw.Count),
	}

	return nil
}

// JobOptions configures a job. Zero fields are unset.
type JobOptions struct {
	// Delay before the job becomes available.
	Delay time.Duration
	// Priority: lower values are processed first; 0 means not prioritized.
	Priority int
	// Attempts is the total number of attempts before the job fails.
	Attempts int
	// Backoff is the retry backoff strategy.
	Backoff *BackoffStrategy
	// LIFO adds the job to the front of the wait list.
	LIFO bool
	// RemoveOnComplete overrides the completed-job retention policy.
	RemoveOnComplete *RemoveOnFinish
	// RemoveOnFail overrides the failed-job retention policy.
	RemoveOnFail *RemoveOnFinish
	// KeepLogs is the maximum number of log entries to keep (0 = all).
	KeepLogs int
	// JobID overrides the generated id. It must not be an integer.
	JobID string
	// Timestamp is the logical creation time (default now).
	Timestamp time.Time
	// StackTraceLimit bounds the stored stack traces.
	StackTraceLimit int
	// Parent attaches the job to a parent job.
	Parent *ParentOptions
	// Deduplication deduplicates the job.
	Deduplication *DeduplicationOptions
	// Repeat is set on jobs produced by job schedulers.
	Repeat *RepeatOptions
	// PrevMillis is used by scheduler iterations.
	PrevMillis int64
	// RepeatJobKey is the id of the scheduler that produced the job.
	RepeatJobKey string
	// FailParentOnFailure fails the parent when this child fails.
	FailParentOnFailure bool
	// IgnoreDependencyOnFailure lets the parent proceed when this child fails,
	// recording the failure.
	IgnoreDependencyOnFailure bool
	// RemoveDependencyOnFailure removes the dependency when this child fails.
	RemoveDependencyOnFailure bool
	// ContinueParentOnFailure starts the parent as soon as this child fails.
	ContinueParentOnFailure bool
	// SizeLimit rejects jobs whose JSON data exceeds this many bytes.
	SizeLimit int
}

// jobOptionsJSON is the JSON form of the job options stored in Redis.
type jobOptionsJSON struct {
	Delay                     float64          `json:"delay,omitempty"`
	Priority                  float64          `json:"priority,omitempty"`
	Attempts                  float64          `json:"attempts,omitempty"`
	Backoff                   *BackoffStrategy `json:"backoff,omitempty"`
	LIFO                      bool             `json:"lifo,omitempty"`
	RemoveOnComplete          *RemoveOnFinish  `json:"removeOnComplete,omitempty"`
	RemoveOnFail              *RemoveOnFinish  `json:"removeOnFail,omitempty"`
	KeepLogs                  float64          `json:"kl,omitempty"`
	KeepLogsLong              float64          `json:"keepLogs,omitempty"`
	JobID                     string           `json:"jobId,omitempty"`
	Timestamp                 float64          `json:"timestamp,omitempty"`
	StackTraceLimit           float64          `json:"stackTraceLimit,omitempty"`
	Parent                    *ParentOptions   `json:"parent,omitempty"`
	Deduplication             *dedupJSON       `json:"de,omitempty"`
	DeduplicationLong         *dedupJSON       `json:"deduplication,omitempty"`
	Repeat                    *RepeatOptions   `json:"repeat,omitempty"`
	PrevMillis                float64          `json:"prevMillis,omitempty"`
	RepeatJobKey              string           `json:"repeatJobKey,omitempty"`
	FailParentOnFailure       bool             `json:"fpof,omitempty"`
	IgnoreDependencyOnFailure bool             `json:"idof,omitempty"`
	RemoveDependencyOnFailure bool             `json:"rdof,omitempty"`
	ContinueParentOnFailure   bool             `json:"cpof,omitempty"`
	SizeLimit                 float64          `json:"sizeLimit,omitempty"`
}

type dedupJSON struct {
	ID               string  `json:"id"`
	TTL              float64 `json:"ttl,omitempty"`
	Extend           bool    `json:"extend,omitempty"`
	Replace          bool    `json:"replace,omitempty"`
	Reschedule       bool    `json:"reschedule,omitempty"`
	KeepLastIfActive bool    `json:"keepLastIfActive,omitempty"`
}

// MarshalJSON encodes the options as stored in Redis (milliseconds).
func (o JobOptions) MarshalJSON() ([]byte, error) {
	raw := jobOptionsJSON{
		Delay:                     float64(o.Delay.Milliseconds()),
		Priority:                  float64(o.Priority),
		Attempts:                  float64(o.Attempts),
		Backoff:                   o.Backoff,
		LIFO:                      o.LIFO,
		RemoveOnComplete:          o.RemoveOnComplete,
		RemoveOnFail:              o.RemoveOnFail,
		KeepLogs:                  float64(o.KeepLogs),
		JobID:                     o.JobID,
		Timestamp:                 float64(timeToMillis(o.Timestamp)),
		StackTraceLimit:           float64(o.StackTraceLimit),
		Parent:                    o.Parent,
		Repeat:                    o.Repeat,
		PrevMillis:                float64(o.PrevMillis),
		RepeatJobKey:              o.RepeatJobKey,
		FailParentOnFailure:       o.FailParentOnFailure,
		IgnoreDependencyOnFailure: o.IgnoreDependencyOnFailure,
		RemoveDependencyOnFailure: o.RemoveDependencyOnFailure,
		ContinueParentOnFailure:   o.ContinueParentOnFailure,
		SizeLimit:                 float64(o.SizeLimit),
	}
	if d := o.Deduplication; d != nil {
		raw.Deduplication = &dedupJSON{
			ID: d.ID, TTL: float64(d.TTL.Milliseconds()), Extend: d.Extend,
			Replace: d.Replace, Reschedule: d.Reschedule, KeepLastIfActive: d.KeepLastIfActive,
		}
	}

	return json.Marshal(raw)
}

// UnmarshalJSON decodes the options stored in Redis. Unknown fields are
// ignored so options written by other BullMQ implementations still load.
func (o *JobOptions) UnmarshalJSON(b []byte) error {
	var raw jobOptionsJSON
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}

	keepLogs := raw.KeepLogs
	if keepLogs == 0 {
		keepLogs = raw.KeepLogsLong
	}

	*o = JobOptions{
		Delay:                     msToDuration(raw.Delay),
		Priority:                  int(raw.Priority),
		Attempts:                  int(raw.Attempts),
		Backoff:                   raw.Backoff,
		LIFO:                      raw.LIFO,
		RemoveOnComplete:          raw.RemoveOnComplete,
		RemoveOnFail:              raw.RemoveOnFail,
		KeepLogs:                  int(keepLogs),
		JobID:                     raw.JobID,
		Timestamp:                 millisToTime(int64(raw.Timestamp)),
		StackTraceLimit:           int(raw.StackTraceLimit),
		Parent:                    raw.Parent,
		Repeat:                    raw.Repeat,
		PrevMillis:                int64(raw.PrevMillis),
		RepeatJobKey:              raw.RepeatJobKey,
		FailParentOnFailure:       raw.FailParentOnFailure,
		IgnoreDependencyOnFailure: raw.IgnoreDependencyOnFailure,
		RemoveDependencyOnFailure: raw.RemoveDependencyOnFailure,
		ContinueParentOnFailure:   raw.ContinueParentOnFailure,
		SizeLimit:                 int(raw.SizeLimit),
	}

	d := raw.Deduplication
	if d == nil {
		d = raw.DeduplicationLong
	}

	if d != nil {
		o.Deduplication = &DeduplicationOptions{
			ID: d.ID, TTL: msToDuration(d.TTL), Extend: d.Extend,
			Replace: d.Replace, Reschedule: d.Reschedule, KeepLastIfActive: d.KeepLastIfActive,
		}
	}

	return nil
}

func msToDuration(ms float64) time.Duration { return time.Duration(ms * float64(time.Millisecond)) }

// timeToMillis converts t to Unix milliseconds; the zero time maps to 0.
func timeToMillis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}

	return t.UnixMilli()
}

// millisToTime converts Unix milliseconds to a time; 0 maps to the zero time.
func millisToTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}

	return time.UnixMilli(ms)
}

func nowMillis() int64 { return time.Now().UnixMilli() }
