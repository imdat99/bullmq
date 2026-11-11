package bullmq

import (
	"bullmq/internal/eventqueue"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cross-process queue event listener.
//
// [QueueEvents] consumes the Redis stream BullMQ writes to (the
// `{prefix}:{name}:events` key) and exposes each entry as a typed
// [QueueEvent]. Unlike the in-process [WorkerEvent] queue, these events are
// observable from any process connected to the same Redis server, mirroring
// the Node.js QueueEvents class.

// DefaultQueueEventsBlockingTimeout is the default maximum duration of each
// XREAD BLOCK issued by [QueueEvents].
const DefaultQueueEventsBlockingTimeout = 5 * time.Second

// Retry delays of the consume loop after a failed read.
const (
	queueEventsMinRetryDelay = 100 * time.Millisecond
	queueEventsMaxRetryDelay = 2 * time.Second
)

// QueueEventsOptions configures a [QueueEvents] listener. The zero value
// selects the defaults.
type QueueEventsOptions struct {
	// Connection configures Redis.
	Connection RedisConnectionOptions
	// Prefix is the key prefix (default "bull").
	Prefix string
	// BlockingTimeout is the maximum time each XREAD BLOCK waits before the
	// loop iterates again (default [DefaultQueueEventsBlockingTimeout]).
	// A negative value blocks indefinitely (Redis `BLOCK 0`), which also
	// disables the read deadline acting as a watchdog against half-open
	// sockets. [QueueEvents.Close] interrupts a block either way.
	BlockingTimeout time.Duration
	// LastEventID is the stream id to start reading after. Empty (or "$")
	// delivers only the events produced after the listener starts; "0"
	// replays the full history. A [QueueEventEntry.ID] resumes after that
	// entry.
	LastEventID string
	// ManualStart disables starting the consume loop on creation; call
	// [QueueEvents.Run] to start it (Rust `autorun: false`).
	ManualStart bool
}

func (o QueueEventsOptions) withDefaults() QueueEventsOptions {
	if o.Prefix == "" {
		o.Prefix = DefaultPrefix
	}

	if o.BlockingTimeout == 0 {
		o.BlockingTimeout = DefaultQueueEventsBlockingTimeout
	}

	return o
}

// xreadBlock returns the go-redis Block argument: 0 blocks forever.
func (o QueueEventsOptions) xreadBlock() time.Duration {
	if o.BlockingTimeout < 0 {
		return 0
	}
	// XREAD BLOCK takes whole milliseconds; never send 0 by rounding down.
	return max(o.BlockingTimeout, time.Millisecond)
}

// QueueEventType identifies the kind of a [QueueEvent].
type QueueEventType uint8

// Queue event types. Their String form is the event name written to the
// stream, except for [QueueEventOther] whose name is in [QueueEvent.Event].
const (
	QueueEventActive QueueEventType = iota + 1
	QueueEventAdded
	QueueEventCleaned
	QueueEventCompleted
	QueueEventDebounced
	QueueEventDeduplicated
	QueueEventDelayed
	QueueEventDrained
	QueueEventDuplicated
	QueueEventError
	QueueEventFailed
	QueueEventPaused
	QueueEventProgress
	QueueEventRemoved
	QueueEventResumed
	QueueEventRetriesExhausted
	QueueEventStalled
	QueueEventWaiting
	QueueEventWaitingChildren
	// QueueEventOther is an unrecognized event (forward compatibility).
	QueueEventOther
)

var queueEventNames = [...]string{
	QueueEventActive:           "active",
	QueueEventAdded:            "added",
	QueueEventCleaned:          "cleaned",
	QueueEventCompleted:        "completed",
	QueueEventDebounced:        "debounced",
	QueueEventDeduplicated:     "deduplicated",
	QueueEventDelayed:          "delayed",
	QueueEventDrained:          "drained",
	QueueEventDuplicated:       "duplicated",
	QueueEventError:            "error",
	QueueEventFailed:           "failed",
	QueueEventPaused:           "paused",
	QueueEventProgress:         "progress",
	QueueEventRemoved:          "removed",
	QueueEventResumed:          "resumed",
	QueueEventRetriesExhausted: "retries-exhausted",
	QueueEventStalled:          "stalled",
	QueueEventWaiting:          "waiting",
	QueueEventWaitingChildren:  "waiting-children",
	QueueEventOther:            "other",
}

// queueEventTypesByName maps a stream event name to its type.
var queueEventTypesByName = func() map[string]QueueEventType {
	m := make(map[string]QueueEventType, len(queueEventNames))
	for t, n := range queueEventNames {
		if n != "" && QueueEventType(t) != QueueEventOther {
			m[n] = QueueEventType(t)
		}
	}

	return m
}()

func (t QueueEventType) String() string {
	if int(t) < len(queueEventNames) && queueEventNames[t] != "" {
		return queueEventNames[t]
	}

	return "unknown(" + strconv.Itoa(int(t)) + ")"
}

// HasJobID reports whether events of this type carry a job id.
func (t QueueEventType) HasJobID() bool {
	switch t {
	case QueueEventActive, QueueEventAdded, QueueEventCompleted, QueueEventDebounced,
		QueueEventDeduplicated, QueueEventDelayed, QueueEventDuplicated, QueueEventFailed,
		QueueEventProgress, QueueEventRemoved, QueueEventRetriesExhausted, QueueEventStalled,
		QueueEventWaiting, QueueEventWaitingChildren:
		return true
	default:
		return false
	}
}

// QueueEvent is a typed event consumed from the queue events stream. Type
// selects which fields are meaningful; the others hold their zero value.
// Event names and payloads mirror the Node.js QueueEventsListener.
type QueueEvent struct {
	// Type is the kind of event.
	Type QueueEventType
	// JobID is the job id, for the types where [QueueEventType.HasJobID]
	// holds.
	JobID string
	// Prev is the previous job state, when reported (Active, Completed,
	// Failed, Removed, Waiting). Empty means not reported.
	Prev string
	// JobName is the job name (Added).
	JobName string
	// Count is the number of cleaned jobs as reported by Redis (Cleaned).
	Count string
	// ReturnValue is the JSON-encoded return value (Completed); nil when the
	// field is missing from the stream entry.
	ReturnValue json.RawMessage
	// DebounceID is the debounce id (Debounced, the deprecated alias of
	// Deduplicated).
	DebounceID string
	// DeduplicationID is the deduplication id (Deduplicated).
	DeduplicationID string
	// DeduplicatedJobID is the id of the existing job that caused the
	// deduplication, when reported (Deduplicated).
	DeduplicatedJobID string
	// Delay is the timestamp, in Unix milliseconds, at which the job becomes
	// due (Delayed); 0 when missing or unparsable. See [QueueEvent.DelayUntil].
	Delay int64
	// Message is the error message (Error). Errors met while reading the
	// stream are delivered as Error events with an empty entry id.
	Message string
	// FailedReason is the failure reason (Failed).
	FailedReason string
	// Progress is the progress payload (Progress): the raw JSON when the
	// stream value is valid JSON, otherwise the value encoded as a JSON
	// string.
	Progress json.RawMessage
	// AttemptsMade is the number of attempts as reported by Redis
	// (RetriesExhausted).
	AttemptsMade string
	// Event is the raw event name (Other).
	Event string
	// Fields holds every field of the stream entry except "event" (Other).
	Fields map[string]string
}

// Name returns the event name as written to the stream (the Node.js event
// name); for [QueueEventOther] it is the raw [QueueEvent.Event].
func (e QueueEvent) Name() string {
	if e.Type == QueueEventOther {
		return e.Event
	}

	return e.Type.String()
}

// LookupJobID returns the job id associated with the event, and false for
// event types that carry none (Cleaned, Drained, Error, Paused, Resumed,
// Other).
func (e QueueEvent) LookupJobID() (string, bool) {
	if !e.Type.HasJobID() {
		return "", false
	}

	return e.JobID, true
}

// DelayUntil returns [QueueEvent.Delay] as a time, or the zero time when it
// is unset.
func (e QueueEvent) DelayUntil() time.Time {
	if e.Delay == 0 {
		return time.Time{}
	}

	return time.UnixMilli(e.Delay)
}

// DecodeReturnValue unmarshals [QueueEvent.ReturnValue] into v.
func (e QueueEvent) DecodeReturnValue(v any) error { return json.Unmarshal(e.ReturnValue, v) }

// DecodeProgress unmarshals [QueueEvent.Progress] into v.
func (e QueueEvent) DecodeProgress(v any) error { return json.Unmarshal(e.Progress, v) }

// QueueEventEntry is a single entry read from the queue events stream.
type QueueEventEntry struct {
	// ID is the Redis stream id of the entry (e.g. "1518945496404-0"). It can
	// be passed as [QueueEventsOptions.LastEventID] to resume after it. It is
	// empty for Error events raised by the listener itself.
	ID string
	// Event is the parsed event.
	Event QueueEvent
}

// parseEvent parses a stream entry's fields into a typed event. It takes
// ownership of fields, which it consumes.
func parseEvent(fields map[string]string) QueueEvent {
	take := func(k string) string {
		v := fields[k]
		delete(fields, k)

		return v
	}
	event := take("event")

	typ, ok := queueEventTypesByName[event]
	if !ok {
		return QueueEvent{Type: QueueEventOther, Event: event, Fields: fields}
	}

	e := QueueEvent{Type: typ}
	if typ.HasJobID() {
		e.JobID = take("jobId")
	}

	switch typ {
	case QueueEventActive, QueueEventRemoved, QueueEventWaiting:
		e.Prev = take("prev")
	case QueueEventAdded:
		e.JobName = take("name")
	case QueueEventCleaned:
		e.Count = take("count")
	case QueueEventCompleted:
		if rv, ok := fields["returnvalue"]; ok {
			e.ReturnValue = json.RawMessage(rv)
		}

		e.Prev = take("prev")
	case QueueEventDebounced:
		e.DebounceID = take("debounceId")
	case QueueEventDeduplicated:
		e.DeduplicationID = take("deduplicationId")
		e.DeduplicatedJobID = take("deduplicatedJobId")
	case QueueEventDelayed:
		e.Delay, _ = strconv.ParseInt(take("delay"), 10, 64)
	case QueueEventError:
		e.Message = take("message")
	case QueueEventFailed:
		e.FailedReason = take("failedReason")
		e.Prev = take("prev")
	case QueueEventProgress:
		e.Progress = progressJSON(take("data"))
	case QueueEventRetriesExhausted:
		e.AttemptsMade = take("attemptsMade")
	default:
		// Events without extra fields (drained, paused, stalled, ...).
	}

	return e
}

// progressJSON returns raw when it is valid JSON, else raw encoded as a JSON
// string.
func progressJSON(raw string) json.RawMessage {
	if json.Valid([]byte(raw)) {
		return json.RawMessage(raw)
	}

	b, _ := json.Marshal(raw) // marshaling a string cannot fail

	return b
}

// streamValuesToStrings converts the field map of a stream entry into a
// string map.
func streamValuesToStrings(values map[string]any) map[string]string {
	out := make(map[string]string, len(values))
	for k, v := range values {
		switch s := v.(type) {
		case string:
			out[k] = s
		case []byte:
			out[k] = string(s)
		case nil:
		default:
			out[k] = fmt.Sprint(s)
		}
	}

	return out
}

// QueueEvents is a cross-process, stream-based queue event listener. It is
// safe for concurrent use.
//
//	events, err := bullmq.NewQueueEvents(ctx, "my-queue", nil)
//	if err != nil { ... }
//	defer events.Close()
//	for {
//		entry, ok := events.NextEvent(ctx)
//		if !ok {
//			break
//		}
//		switch entry.Event.Type {
//		case bullmq.QueueEventCompleted:
//			fmt.Println(entry.Event.JobID, "completed")
//		case bullmq.QueueEventFailed:
//			fmt.Println(entry.Event.JobID, "failed:", entry.Event.FailedReason)
//		}
//	}
type QueueEvents struct {
	name     string
	keys     QueueKeys
	conn     *RedisConnection
	ownsConn bool
	opts     QueueEventsOptions
	events   *eventqueue.Queue[QueueEventEntry]
	running  atomic.Bool

	mu       sync.Mutex
	closed   bool
	blocking redis.UniversalClient // dedicated client of the XREAD BLOCK loop

	// lifeCtx is cancelled on Close to stop the consume loop.
	lifeCtx    context.Context //nolint:containedctx // lifetime of the consume loop, cancelled by Close
	lifeCancel context.CancelFunc
	loop       sync.WaitGroup
	closeOnce  sync.Once
	closeErr   error
}

// NewQueueEvents creates a listener for queue name with its own Redis
// connection and, unless [QueueEventsOptions.ManualStart] is set, starts
// consuming events. A nil opts selects the defaults.
func NewQueueEvents(ctx context.Context, name string, opts *QueueEventsOptions) (*QueueEvents, error) {
	var o QueueEventsOptions
	if opts != nil {
		o = *opts
	}

	o = o.withDefaults()
	if err := validateQueueEventsArgs(name, o); err != nil {
		return nil, err
	}

	conn, err := NewRedisConnection(ctx, o.Connection)
	if err != nil {
		return nil, err
	}

	qe, err := newQueueEvents(ctx, name, conn, true, o)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	return qe, nil
}

// NewQueueEventsWithConnection is like [NewQueueEvents] but uses conn (which
// must wrap a *redis.Client or *redis.ClusterClient). The listener still
// opens a dedicated connection with the same settings for its blocking XREAD.
// Close does not close conn; opts.Connection is ignored.
func NewQueueEventsWithConnection(ctx context.Context, name string, conn *RedisConnection, opts *QueueEventsOptions) (*QueueEvents, error) {
	var o QueueEventsOptions
	if opts != nil {
		o = *opts
	}

	o = o.withDefaults()
	if err := validateQueueEventsArgs(name, o); err != nil {
		return nil, err
	}

	if conn == nil {
		return nil, invalidConfig("connection must not be nil")
	}

	return newQueueEvents(ctx, name, conn, false, o)
}

// NewQueueEventsFromQueueOptions creates a listener using the connection and
// prefix of opts and the default listener options. A nil opts selects the
// defaults.
func NewQueueEventsFromQueueOptions(ctx context.Context, name string, opts *QueueOptions) (*QueueEvents, error) {
	var qo QueueOptions
	if opts != nil {
		qo = *opts
	}

	return NewQueueEvents(ctx, name, &QueueEventsOptions{Connection: qo.Connection, Prefix: qo.Prefix})
}

func validateQueueEventsArgs(name string, o QueueEventsOptions) error {
	if err := validateQueueName(name); err != nil {
		return err
	}

	return validatePrefix(o.Prefix)
}

func newQueueEvents(ctx context.Context, name string, conn *RedisConnection, owns bool, o QueueEventsOptions) (*QueueEvents, error) {
	lifeCtx, cancel := context.WithCancel(context.Background())

	qe := &QueueEvents{
		name:       name,
		keys:       NewQueueKeys(name, o.Prefix),
		conn:       conn,
		ownsConn:   owns,
		opts:       o,
		events:     eventqueue.New[QueueEventEntry](maxBufferedEvents),
		lifeCtx:    lifeCtx,
		lifeCancel: cancel,
	}
	if !o.ManualStart {
		if err := qe.run(ctx); err != nil {
			cancel()
			return nil, err
		}
	}

	return qe, nil
}

// Name returns the observed queue name.
func (qe *QueueEvents) Name() string { return qe.name }

// Keys returns the key builder of the observed queue.
func (qe *QueueEvents) Keys() QueueKeys { return qe.keys }

// IsRunning reports whether the consume loop runs.
func (qe *QueueEvents) IsRunning() bool { return qe.running.Load() }

// Run starts consuming events in the background. It is called by the
// constructors unless [QueueEventsOptions.ManualStart] is set. It returns an
// error wrapping [ErrInvalidConfig] if the listener already runs or was
// closed.
//
// When reading from the end of the stream (the default), the current end is
// resolved before Run returns, so events added after Run returns are never
// missed.
func (qe *QueueEvents) Run() error { return qe.run(qe.lifeCtx) }

func (qe *QueueEvents) run(ctx context.Context) error {
	if !qe.running.CompareAndSwap(false, true) {
		return invalidConfig("QueueEvents is already running")
	}

	qe.mu.Lock()
	closed := qe.closed
	qe.mu.Unlock()

	if closed {
		qe.running.Store(false)
		return invalidConfig("Cannot run QueueEvents after close()")
	}

	startID := qe.opts.LastEventID
	if startID == "" || startID == "$" {
		// Best effort: on failure the loop resolves it, retrying.
		startID = "$"
		if id, err := qe.streamEndID(ctx); err == nil {
			startID = id
		}
	}

	qe.mu.Lock()
	defer qe.mu.Unlock()

	if qe.closed {
		qe.running.Store(false)
		return invalidConfig("Cannot run QueueEvents after close()")
	}

	blocking, err := qe.conn.newDedicatedClient("")
	if err != nil {
		qe.running.Store(false)
		return err
	}

	qe.blocking = blocking

	qe.loop.Add(1)
	go qe.consume(blocking, startID)

	return nil
}

// streamEndID returns the id of the last entry of the events stream, or
// "0-0" when the stream is empty or missing: reading after it delivers
// exactly the events added from now on, like XREAD's "$" but without the
// gap between resolving "$" and issuing the first XREAD.
func (qe *QueueEvents) streamEndID(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, ioTimeout)
	defer cancel()

	msgs, err := qe.conn.Client().XRevRangeN(ctx, qe.keys.Events(), "+", "-", 1).Result()
	if err != nil {
		return "", err
	}

	if len(msgs) == 0 {
		return "0-0", nil
	}

	return msgs[0].ID, nil
}

// consume reads the events stream until Close. go-redis re-dials the
// dedicated connection after a failure; the read deadline of XREAD BLOCK
// (block time + 10s) detects half-open sockets.
func (qe *QueueEvents) consume(client redis.UniversalClient, id string) {
	defer qe.loop.Done()

	key := qe.keys.Events()
	block := qe.opts.xreadBlock()
	retryDelay := queueEventsMinRetryDelay

	fail := func(err error) {
		slog.Warn("bullmq: queue events read failed, retrying", "queue", qe.name, "error", err)
		qe.events.Push(QueueEventEntry{Event: QueueEvent{Type: QueueEventError, Message: err.Error()}})

		t := time.NewTimer(retryDelay)
		select {
		case <-t.C:
		case <-qe.lifeCtx.Done():
		}

		t.Stop()

		retryDelay = min(retryDelay*2, queueEventsMaxRetryDelay)
	}

	for qe.lifeCtx.Err() == nil {
		if id == "$" {
			end, err := qe.streamEndID(qe.lifeCtx)
			if err != nil {
				if qe.lifeCtx.Err() != nil {
					break
				}

				fail(err)

				continue
			}

			id = end
		}

		streams, err := client.XRead(qe.lifeCtx, &redis.XReadArgs{
			Streams: []string{key, id},
			Block:   block,
		}).Result()
		if errors.Is(err, redis.Nil) {
			retryDelay = queueEventsMinRetryDelay
			continue // block timeout elapsed with no new events
		}

		if err != nil {
			if qe.lifeCtx.Err() != nil {
				break
			}

			fail(err)

			continue
		}

		retryDelay = queueEventsMinRetryDelay

		for _, s := range streams {
			for _, msg := range s.Messages {
				id = msg.ID
				qe.events.Push(QueueEventEntry{ID: msg.ID, Event: parseEvent(streamValuesToStrings(msg.Values))})
			}
		}
	}

	slog.Debug("bullmq: queue events loop stopped", "queue", qe.name)
}

// NextEvent waits for the next event. It returns false when ctx is done, or
// when the listener is closed and every buffered event has been read.
func (qe *QueueEvents) NextEvent(ctx context.Context) (QueueEventEntry, bool) {
	return qe.events.Next(ctx)
}

// TryNextEvent returns the next buffered event without waiting.
func (qe *QueueEvents) TryNextEvent() (QueueEventEntry, bool) { return qe.events.TryNext() }

// Close stops consuming events, interrupting a pending XREAD BLOCK, waits
// for the consume loop to exit and releases the dedicated connection (and the
// shared one when the listener owns it). Events already buffered can still be
// read. Close is idempotent.
func (qe *QueueEvents) Close() error {
	qe.closeOnce.Do(func() {
		qe.mu.Lock()
		qe.closed = true
		blocking := qe.blocking
		qe.mu.Unlock()

		qe.lifeCancel()

		if blocking != nil {
			// Closing the client closes its in-use connection, which unblocks
			// XREAD right away.
			_ = blocking.Close()
		}

		qe.loop.Wait()
		qe.running.Store(false)
		qe.events.Close()

		if qe.ownsConn {
			qe.closeErr = qe.conn.Close()
		}
	})

	return qe.closeErr
}
