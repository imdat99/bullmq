package bullmq

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/imdat99/bullmq/internal/msgpack"
	"github.com/imdat99/bullmq/internal/scripts"
)

// Defaults of [FlowProducer.GetFlow].
const (
	defaultFlowDepth       = 10
	defaultFlowMaxChildren = 20
	// flowPaginateMaxIterations bounds the paginate script's work per round
	// trip when GetFlow collects child keys.
	flowPaginateMaxIterations = 5
)

// FlowJob describes a job to add as part of a flow. Children must complete
// before their parent is processed.
type FlowJob struct {
	// Name is the job name.
	Name string
	// QueueName is the queue the job belongs to.
	QueueName string
	// Data is the job payload. It is JSON-encoded (a json.RawMessage is used
	// verbatim); nil is stored as JSON null.
	Data any
	// Opts are the job options (may be nil).
	Opts *JobOptions
	// Prefix overrides the producer's key prefix for this job ("" = inherit
	// the producer's prefix).
	Prefix string
	// Children are the jobs that must complete before this job is processed.
	Children []FlowJob
}

// GetFlowOptions selects the flow tree retrieved by [FlowProducer.GetFlow].
type GetFlowOptions struct {
	// QueueName is the queue of the root job.
	QueueName string
	// ID is the root job id.
	ID string
	// Prefix overrides the producer's key prefix ("" = producer's prefix).
	Prefix string
	// Depth is the maximum tree depth to traverse (0 selects the default, 10).
	Depth int
	// MaxChildren is the maximum number of children retrieved per node (0
	// selects the default, 20).
	MaxChildren int
}

// JobNode is a node of a flow tree: a job and its children.
type JobNode struct {
	// Job is the job at this node. It is bound to its own queue, so methods
	// such as [Job.GetState] work directly.
	Job *Job
	// Children are the child nodes (nil when the job has none).
	Children []JobNode
}

// FlowQueueOptions are per-queue options used when adding a flow.
type FlowQueueOptions struct {
	// DefaultJobOptions are applied to every job of the flow added to this
	// queue. Options set on the job itself always take precedence.
	DefaultJobOptions JobOptions
}

// FlowOptions are the extra options of [FlowProducer.AddWithOptions] and
// [FlowProducer.AddBulkWithOptions].
type FlowOptions struct {
	// QueuesOptions holds per-queue options keyed by queue name.
	QueuesOptions map[string]FlowQueueOptions
}

// FlowProducerOptions configures [NewFlowProducer].
type FlowProducerOptions struct {
	// Connection is the Redis connection configuration.
	Connection RedisConnectionOptions
	// Prefix is the key prefix ("" selects [DefaultPrefix]).
	Prefix string
}

// FlowProducer atomically adds trees of dependent jobs (flows) to queues.
//
// A flow is a tree in which children must complete before their parent can
// be processed; the parent can then read its children's results. A
// FlowProducer is safe for concurrent use.
type FlowProducer struct {
	conn     *RedisConnection
	prefix   string
	ownsConn bool
}

// NewFlowProducer connects to Redis and returns a flow producer. A nil opts
// selects the defaults.
func NewFlowProducer(ctx context.Context, opts *FlowProducerOptions) (*FlowProducer, error) {
	var o FlowProducerOptions
	if opts != nil {
		o = *opts
	}

	prefix := o.Prefix
	if prefix == "" {
		prefix = DefaultPrefix
	}

	if err := validatePrefix(prefix); err != nil {
		return nil, err
	}

	conn, err := NewRedisConnection(ctx, o.Connection)
	if err != nil {
		return nil, err
	}

	return &FlowProducer{conn: conn, prefix: prefix, ownsConn: true}, nil
}

// NewFlowProducerWithConnection returns a flow producer that shares conn.
// An empty prefix selects [DefaultPrefix]. Closing the producer does not
// close conn.
func NewFlowProducerWithConnection(conn *RedisConnection, prefix string) (*FlowProducer, error) {
	if conn == nil {
		return nil, invalidConfig("Connection must be provided")
	}

	if prefix == "" {
		prefix = DefaultPrefix
	}

	if err := validatePrefix(prefix); err != nil {
		return nil, err
	}

	return &FlowProducer{conn: conn, prefix: prefix}, nil
}

// Prefix returns the key prefix.
func (f *FlowProducer) Prefix() string { return f.prefix }

// Connection returns the Redis connection.
func (f *FlowProducer) Connection() *RedisConnection { return f.conn }

// Close releases the connection when the producer owns it.
func (f *FlowProducer) Close() error {
	if f.ownsConn {
		return f.conn.Close()
	}

	return nil
}

// ── Adding flows ────────────────────────────────────────────────────────────

// Add atomically adds a flow (a tree of jobs with parent-child dependencies).
//
// Children are added to their respective queues and parents are placed in
// the waiting-children state; once all its children complete, a parent moves
// to wait and becomes processable.
//
// The returned [JobNode] has the requested tree shape. The root id is taken
// from Redis; child nodes keep their generated (or custom) ids, which may not
// exist in Redis when an internal parent was deduplicated and its descendants
// were skipped. An error is returned when the root job cannot be added (for
// example when a referenced external parent is missing).
func (f *FlowProducer) Add(ctx context.Context, flow FlowJob) (JobNode, error) {
	return f.AddWithOptions(ctx, flow, nil)
}

// AddWithOptions is [FlowProducer.Add] with per-queue default job options:
// for every job whose queue appears in opts.QueuesOptions, the queue's
// DefaultJobOptions fill the options the job leaves unset. opts may be nil.
func (f *FlowProducer) AddWithOptions(ctx context.Context, flow FlowJob, opts *FlowOptions) (JobNode, error) {
	nodes, err := f.addFlows(ctx, []FlowJob{applyQueueDefaults(flow, opts)})
	if err != nil {
		return JobNode{}, err
	}

	return nodes[0], nil
}

// AddBulk atomically adds several flows in a single transaction.
//
// The bulk add is validated as a whole: if the root job of any flow cannot be
// added, an error is returned instead of a partial result.
func (f *FlowProducer) AddBulk(ctx context.Context, flows []FlowJob) ([]JobNode, error) {
	return f.AddBulkWithOptions(ctx, flows, nil)
}

// AddBulkWithOptions is [FlowProducer.AddBulk] with per-queue default job
// options (see [FlowProducer.AddWithOptions]). opts may be nil.
func (f *FlowProducer) AddBulkWithOptions(ctx context.Context, flows []FlowJob, opts *FlowOptions) ([]JobNode, error) {
	if len(flows) == 0 {
		return nil, nil
	}

	withDefaults := make([]FlowJob, len(flows))
	for i, flow := range flows {
		withDefaults[i] = applyQueueDefaults(flow, opts)
	}

	return f.addFlows(ctx, withDefaults)
}

// flowCall is one script invocation of a flow transaction.
type flowCall struct {
	script *scripts.Script
	keys   []string
	args   []any
}

// addFlows validates the flows, queues every job insert in pre-order (each
// node before its children, tree after tree) followed by one release check
// per distinct parent touched, and runs everything in one MULTI/EXEC.
//
// Release checks run only after every tree has queued its inserts: if two
// trees reuse the same parent id, releasing it right after the first tree
// (which may reference only an already-completed child) would let it run
// before the second tree attaches its still-pending child.
func (f *FlowProducer) addFlows(ctx context.Context, flows []FlowJob) ([]JobNode, error) {
	for i := range flows {
		if err := validateFlowQueueNames(&flows[i]); err != nil {
			return nil, err
		}

		hasParent := flows[i].Opts != nil && flows[i].Opts.Parent != nil
		if err := validateDeferredChildDeduplication(&flows[i], hasParent); err != nil {
			return nil, err
		}
	}

	var (
		calls   []flowCall
		parents []parentContext
	)

	nodes := make([]JobNode, len(flows))
	for i := range flows {
		flow := &flows[i]

		var rootParent *parentContext

		if flow.Opts != nil && flow.Opts.Parent != nil {
			p, err := externalParentContext(f.nodePrefix(flow), flow.Opts.Parent)
			if err != nil {
				return nil, err
			}

			rootParent = &p
		}

		node, nodeParents, err := f.addNode(&calls, flow, rootParent)
		if err != nil {
			return nil, err
		}

		parents = append(parents, nodeParents...)
		if rootParent != nil {
			parents = append(parents, *rootParent)
		}

		nodes[i] = node
	}

	releases := dedupParentContexts(parents)
	release := scripts.Get("releaseParentIfNoPendingDependencies")

	now := nowMillis()
	for _, p := range releases {
		calls = append(calls, flowCall{
			script: release,
			keys:   []string{p.parentKey, p.parentKey + ":dependencies"},
			args:   []any{p.parentQueueKey, p.parentID, now},
		})
	}

	results, err := f.execFlowCalls(ctx, flows, calls)
	if err != nil {
		return nil, err
	}

	// Commands were queued tree by tree, each in pre-order, so one sequential
	// walk lines the job replies up with the nodes. The trailing release
	// replies are ignored.
	for i := range nodes {
		if results, err = applyPipelineResults(&nodes[i], results, true); err != nil {
			return nil, err
		}
	}

	return nodes, nil
}

// execFlowCalls runs calls in one MULTI/EXEC transaction and returns the
// reply of each call (nil for a nil reply). Like the Rust implementation, a
// server error on any command fails the whole call; MULTI/EXEC does not roll
// back the commands that succeeded.
func (f *FlowProducer) execFlowCalls(ctx context.Context, flows []FlowJob, calls []flowCall) ([]any, error) {
	client := f.conn.client
	// EVALSHA inside MULTI has no NOSCRIPT fallback: load the scripts first.
	loaded := map[string]bool{}

	for i := range flows {
		for _, name := range flowScriptNames(&flows[i]) {
			if loaded[name] {
				continue
			}

			if err := scripts.Get(name).Load(ctx, client); err != nil {
				return nil, fmt.Errorf("bullmq: load script %s: %w", name, err)
			}

			loaded[name] = true
		}
	}

	pipe := client.TxPipeline()

	cmds := make([]*redis.Cmd, len(calls))
	for i, c := range calls {
		if len(c.keys) != c.script.NumKeys() {
			return nil, fmt.Errorf("bullmq: script %s expects %d keys, got %d", c.script.Name(), c.script.NumKeys(), len(c.keys))
		}

		cmds[i] = c.script.EvalSha(ctx, pipe, c.keys, c.args...)
	}

	_, execErr := pipe.Exec(ctx)

	results := make([]any, len(cmds))
	for i, cmd := range cmds {
		res, err := cmd.Result()
		switch {
		case errors.Is(err, redis.Nil):
			results[i] = nil
		case err != nil:
			return nil, fmt.Errorf("bullmq: add flow: %s: %w", calls[i].script.Name(), err)
		default:
			results[i] = res
		}
	}

	if execErr != nil && !errors.Is(execErr, redis.Nil) {
		return nil, fmt.Errorf("bullmq: add flow: %w", execErr)
	}

	return results, nil
}

// nodePrefix returns the key prefix of a flow node.
func (f *FlowProducer) nodePrefix(node *FlowJob) string {
	if node.Prefix != "" {
		return node.Prefix
	}

	return f.prefix
}

// addNode queues the insert of node and, recursively, of its children. A
// node with children is added with addParentJob (waiting-children); a leaf
// with the standard, delayed or prioritized add script. It returns the node
// and the parents it touched (to release once everything is queued).
func (f *FlowProducer) addNode(calls *[]flowCall, node *FlowJob, parent *parentContext) (JobNode, []parentContext, error) {
	prefix := f.nodePrefix(node)
	keys := NewQueueKeys(node.QueueName, prefix)

	var opts JobOptions
	if node.Opts != nil {
		opts = *node.Opts
	}

	jobID := opts.JobID
	if jobID == "" {
		jobID = uuid.NewString()
	}

	data, err := marshalData(node.Data)
	if err != nil {
		return JobNode{}, nil, err
	}

	job := newJob(node.Name, data, opts)
	job.ID = jobID
	job.bind(&scriptContext{conn: f.conn, keys: keys})

	if parent != nil {
		job.Parent = &ParentKeys{ID: parent.parentID, QueueKey: parent.parentQueueKey}
		job.ParentKey = parent.parentQueueKey + ":" + parent.parentID
	}

	if len(node.Children) == 0 {
		name, scriptKeys := leafJobScript(keys, job)
		*calls = append(*calls, flowCall{
			script: scripts.Get(name),
			keys:   scriptKeys,
			args:   []any{packFlowAddArgs(keys, job, parent), []byte(job.Data), packJobOpts(&job.Opts)},
		})

		return JobNode{Job: job}, nil, nil
	}

	*calls = append(*calls, flowCall{
		script: scripts.Get("addParentJob"),
		keys:   []string{keys.Meta(), keys.ID(), keys.Delayed(), keys.WaitingChildren(), keys.Completed(), keys.Events()},
		args:   []any{packFlowAddArgs(keys, job, parent), []byte(job.Data), packJobOpts(&job.Opts)},
	})
	parentQueueKey := keys.Base()
	parentKey := parentQueueKey + ":" + jobID
	childParent := parentContext{
		parentID:       jobID,
		parentKey:      parentKey,
		parentQueueKey: parentQueueKey,
		parentDepsKey:  parentKey + ":dependencies",
	}
	children := make([]JobNode, 0, len(node.Children))

	releases := []parentContext{childParent}
	for i := range node.Children {
		child, childReleases, err := f.addNode(calls, &node.Children[i], &childParent)
		if err != nil {
			return JobNode{}, nil, err
		}

		children = append(children, child)
		releases = append(releases, childReleases...)
	}

	return JobNode{Job: job, Children: children}, releases, nil
}

// leafJobScript selects the add script of a leaf job and builds its keys.
func leafJobScript(k QueueKeys, job *Job) (string, []string) {
	switch {
	case job.Delay > 0:
		return "addDelayedJob", []string{k.Marker(), k.Meta(), k.ID(), k.Delayed(), k.Completed(), k.Events()}
	case job.Opts.Priority > 0:
		return "addPrioritizedJob", []string{k.Marker(), k.Meta(), k.ID(), k.Prioritized(), k.Delayed(), k.Completed(), k.Active(), k.Events(), k.PC()}
	default:
		return "addStandardJob", []string{k.Wait(), k.Paused(), k.Meta(), k.ID(), k.Completed(), k.Delayed(), k.Active(), k.Events(), k.Marker()}
	}
}

// flowScriptNames lists the scripts a flow transaction runs.
func flowScriptNames(node *FlowJob) []string {
	var names []string
	collectFlowScriptNames(node, &names)

	if node.Opts != nil && node.Opts.Parent != nil && !containsString(names, "releaseParentIfNoPendingDependencies") {
		names = append(names, "releaseParentIfNoPendingDependencies")
	}

	return names
}

func collectFlowScriptNames(node *FlowJob, names *[]string) {
	if len(node.Children) > 0 {
		*names = append(*names, "addParentJob")
		for i := range node.Children {
			collectFlowScriptNames(&node.Children[i], names)
		}

		*names = append(*names, "releaseParentIfNoPendingDependencies")

		return
	}

	var o JobOptions
	if node.Opts != nil {
		o = *node.Opts
	}

	switch {
	case o.Delay > 0:
		*names = append(*names, "addDelayedJob")
	case o.Priority > 0:
		*names = append(*names, "addPrioritizedJob")
	default:
		*names = append(*names, "addStandardJob")
	}
}

func containsString(s []string, v string) bool { return slices.Contains(s, v) }

// packFlowAddArgs packs ARGV[1] of the add scripts for a flow insert:
// [keyPrefix, jobId, name, timestamp, parentKey, parentDepsKey, parent,
// repeatJobKey, dedupKey, deferParentRelease]. The deferral flag stops a
// script from releasing a parent while later siblings of the same tree are
// still queued; the trailing release checks do it instead.
func packFlowAddArgs(k QueueKeys, job *Job, parent *parentContext) []byte {
	var b msgpack.Buf
	b.ArrayLen(10)
	b.Str(k.KeyPrefix())
	b.Str(job.ID)
	b.Str(job.Name)
	b.Uint(uint64(max(job.Timestamp, 0)))
	o := &job.Opts

	if parent != nil {
		b.Str(parent.parentQueueKey + ":" + parent.parentID)
		b.Str(parent.parentDepsKey)

		var m msgpack.Map
		m.PutStr("id", parent.parentID)
		m.PutStr("queueKey", parent.parentQueueKey)

		if o.FailParentOnFailure {
			m.PutBool("fpof", true)
		}

		if o.RemoveDependencyOnFailure {
			m.PutBool("rdof", true)
		}

		if o.IgnoreDependencyOnFailure {
			m.PutBool("idof", true)
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

	b.Nil() // repeat job key: never set for flows

	if d := o.Deduplication; d != nil {
		b.Str(k.Dedup(d.ID))
	} else {
		b.Nil()
	}

	b.Uint(1)

	return b.Bytes()
}

// applyPipelineResults walks node in the pre-order its commands were queued,
// consuming one reply per node, and returns the remaining replies.
//
// Only the root of a tree (isRoot) is validated strictly: a negative code
// there is a genuine failure (e.g. a missing external parent). For other
// nodes a -5 (missing parent key) is benign: it happens when an internal
// parent was deduplicated and its new key was never created, so its children
// are intentionally not added. Any other negative code still fails. Only the
// root id is updated from Redis; children keep their in-memory ids, as in
// Node.js.
func applyPipelineResults(node *JobNode, results []any, isRoot bool) ([]any, error) {
	if len(results) > 0 {
		res := results[0]
		results = results[1:]

		if isRoot {
			if err := checkAddResult(res, node.Job.ParentKey); err != nil {
				return nil, err
			}

			if id, ok := extractJobID(res); ok {
				node.Job.ID = id
			}
		} else if code, ok := res.(int64); ok && code < 0 && code != CodeParentJobNotExist {
			return nil, NewScriptError(code)
		}
	}

	for i := range node.Children {
		var err error
		if results, err = applyPipelineResults(&node.Children[i], results, false); err != nil {
			return nil, err
		}
	}

	return results, nil
}

// checkAddResult converts a negative add-script code into an error. -5 means
// the referenced parent key is missing: the job was not added.
func checkAddResult(res any, parentKey string) error {
	code, ok := res.(int64)
	if !ok || code >= 0 {
		return nil
	}

	if code == CodeParentJobNotExist {
		return &ScriptError{Code: code, Message: fmt.Sprintf("Missing key for parent job %s. addJob", parentKey)}
	}

	return NewScriptError(code)
}

// extractJobID returns the job id of an add-script reply.
func extractJobID(res any) (string, bool) {
	switch v := res.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	case int64:
		if v >= 0 {
			return strconv.FormatInt(v, 10), true
		}
	}

	return "", false
}

// parentContext describes the parent of a flow node.
type parentContext struct {
	parentID       string
	parentKey      string
	parentQueueKey string
	parentDepsKey  string
}

// externalParentContext builds the context of a root node's opts.Parent.
func externalParentContext(prefix string, p *ParentOptions) (parentContext, error) {
	parentQueueKey, err := resolveParentQueueKey(prefix, p.Queue)
	if err != nil {
		return parentContext{}, err
	}

	parentKey := parentQueueKey + ":" + p.ID

	return parentContext{
		parentID:       p.ID,
		parentKey:      parentKey,
		parentQueueKey: parentQueueKey,
		parentDepsKey:  parentKey + ":dependencies",
	}, nil
}

// dedupParentContexts deduplicates parents by key: the last context of a key
// wins, at the position of the key's first occurrence (so the release order
// is deterministic).
func dedupParentContexts(parents []parentContext) []parentContext {
	out := make([]parentContext, 0, len(parents))

	index := make(map[string]int, len(parents))
	for _, p := range parents {
		if i, ok := index[p.parentKey]; ok {
			out[i] = p
			continue
		}

		index[p.parentKey] = len(out)
		out = append(out, p)
	}

	return out
}

// applyQueueDefaults returns a copy of flow in which the per-queue default
// job options fill, recursively, the options each node leaves unset. flow
// itself (including its Children slice) is not modified.
func applyQueueDefaults(flow FlowJob, opts *FlowOptions) FlowJob {
	if opts == nil || len(opts.QueuesOptions) == 0 {
		return flow
	}

	if qo, ok := opts.QueuesOptions[flow.QueueName]; ok {
		var over JobOptions
		if flow.Opts != nil {
			over = *flow.Opts
		}

		merged := overlayJobOptions(qo.DefaultJobOptions, over)
		flow.Opts = &merged
	}

	if len(flow.Children) > 0 {
		children := make([]FlowJob, len(flow.Children))
		for i, child := range flow.Children {
			children[i] = applyQueueDefaults(child, opts)
		}

		flow.Children = children
	}

	return flow
}

// overlayJobOptions returns base with every field set (non-zero) in over
// replacing the base value: job-level options win over queue defaults.
func overlayJobOptions(base, over JobOptions) JobOptions {
	base.Delay = cmp.Or(over.Delay, base.Delay)
	base.Priority = cmp.Or(over.Priority, base.Priority)
	base.Attempts = cmp.Or(over.Attempts, base.Attempts)
	base.Backoff = cmp.Or(over.Backoff, base.Backoff)
	base.LIFO = cmp.Or(over.LIFO, base.LIFO)
	base.RemoveOnComplete = cmp.Or(over.RemoveOnComplete, base.RemoveOnComplete)
	base.RemoveOnFail = cmp.Or(over.RemoveOnFail, base.RemoveOnFail)
	base.KeepLogs = cmp.Or(over.KeepLogs, base.KeepLogs)
	base.JobID = cmp.Or(over.JobID, base.JobID)
	base.Timestamp = cmp.Or(over.Timestamp, base.Timestamp)
	base.StackTraceLimit = cmp.Or(over.StackTraceLimit, base.StackTraceLimit)
	base.Parent = cmp.Or(over.Parent, base.Parent)
	base.Deduplication = cmp.Or(over.Deduplication, base.Deduplication)
	base.Repeat = cmp.Or(over.Repeat, base.Repeat)
	base.PrevMillis = cmp.Or(over.PrevMillis, base.PrevMillis)
	base.RepeatJobKey = cmp.Or(over.RepeatJobKey, base.RepeatJobKey)
	base.FailParentOnFailure = cmp.Or(over.FailParentOnFailure, base.FailParentOnFailure)
	base.IgnoreDependencyOnFailure = cmp.Or(over.IgnoreDependencyOnFailure, base.IgnoreDependencyOnFailure)
	base.RemoveDependencyOnFailure = cmp.Or(over.RemoveDependencyOnFailure, base.RemoveDependencyOnFailure)
	base.ContinueParentOnFailure = cmp.Or(over.ContinueParentOnFailure, base.ContinueParentOnFailure)
	base.SizeLimit = cmp.Or(over.SizeLimit, base.SizeLimit)

	return base
}

// validateFlowQueueNames validates the queue name, prefix override and
// custom job id of every node.
func validateFlowQueueNames(flow *FlowJob) error {
	if err := validateQueueName(flow.QueueName); err != nil {
		return err
	}

	if flow.Prefix != "" {
		if err := validatePrefix(flow.Prefix); err != nil {
			return err
		}
	}

	if flow.Opts != nil {
		if err := validateCustomJobID(flow.Opts.JobID); err != nil {
			return err
		}
	}

	for i := range flow.Children {
		if err := validateFlowQueueNames(&flow.Children[i]); err != nil {
			return err
		}
	}

	return nil
}

// validateDeferredChildDeduplication rejects keepLastIfActive deduplication
// on nodes that have a parent (in the flow or external).
func validateDeferredChildDeduplication(flow *FlowJob, hasParent bool) error {
	if hasParent && flow.Opts != nil && flow.Opts.Deduplication != nil && flow.Opts.Deduplication.KeepLastIfActive {
		return invalidConfig("keepLastIfActive deduplication is not supported for flow nodes with a parent")
	}

	for i := range flow.Children {
		if err := validateDeferredChildDeduplication(&flow.Children[i], true); err != nil {
			return err
		}
	}

	return nil
}

// ── Reading flows ───────────────────────────────────────────────────────────

// GetFlow loads an existing flow tree from Redis, following each job's
// pending, processed and failed dependencies.
//
// It returns an error wrapping [ErrJobNotFound] when the root job does not
// exist. Children that no longer exist are skipped.
func (f *FlowProducer) GetFlow(ctx context.Context, opts GetFlowOptions) (JobNode, error) {
	if err := validateQueueName(opts.QueueName); err != nil {
		return JobNode{}, err
	}

	prefix := opts.Prefix
	if prefix == "" {
		prefix = f.prefix
	}

	if err := validatePrefix(prefix); err != nil {
		return JobNode{}, err
	}

	if opts.Depth < 0 {
		return JobNode{}, invalidConfig("Depth cannot be negative")
	}

	if opts.MaxChildren < 0 {
		return JobNode{}, invalidConfig("MaxChildren cannot be negative")
	}

	depth := opts.Depth
	if depth == 0 {
		depth = defaultFlowDepth
	}

	maxChildren := opts.MaxChildren
	if maxChildren == 0 {
		maxChildren = defaultFlowMaxChildren
	}

	return f.getNode(ctx, prefix, opts.QueueName, opts.ID, depth, maxChildren)
}

// getNode recursively loads a node.
func (f *FlowProducer) getNode(ctx context.Context, prefix, queueName, jobID string, depth, maxChildren int) (JobNode, error) {
	keys := NewQueueKeys(queueName, prefix)
	jobKey := keys.JobKey(jobID)

	hash, err := f.conn.client.HGetAll(ctx, jobKey).Result()
	if err != nil {
		return JobNode{}, err
	}

	if len(hash) == 0 {
		return JobNode{}, fmt.Errorf("%w: %s", ErrJobNotFound, jobID)
	}

	job := jobFromHash(jobID, hash)
	job.bind(&scriptContext{conn: f.conn, keys: keys})

	node := JobNode{Job: job}
	if depth == 0 {
		return node, nil
	}

	// Collect child keys from the pending dependencies set and the processed
	// and failed hashes with the paginate script (SSCAN/HSCAN with COUNT), so
	// large flows are never loaded whole; each category is bounded.
	var childKeys []string

	for _, suffix := range []string{":dependencies", ":processed", ":failed"} {
		page, err := f.paginateChildKeys(ctx, jobKey+suffix, maxChildren)
		if err != nil {
			return JobNode{}, err
		}

		childKeys = append(childKeys, page...)
	}

	if len(childKeys) > maxChildren {
		childKeys = childKeys[:maxChildren]
	}

	for _, ck := range childKeys {
		split, ok := splitQualifiedJobKey(ck)
		if !ok {
			continue
		}

		child, err := f.getNode(ctx, split.prefix, split.queueName, split.jobID, depth-1, maxChildren)
		if errors.Is(err, ErrJobNotFound) {
			continue
		}

		if err != nil {
			return JobNode{}, err
		}

		node.Children = append(node.Children, child)
	}

	return node, nil
}

// paginateChildKeys collects up to limit member keys of a set or hash with
// the paginate script, without loading the whole collection.
func (f *FlowProducer) paginateChildKeys(ctx context.Context, key string, limit int) ([]string, error) {
	if limit <= 0 {
		return nil, nil
	}

	script := scripts.Get("paginate")
	cursor, offset := "0", int64(0)
	end := limit - 1

	var collected []string
	for {
		res, err := script.Run(ctx, f.conn.client, []string{key},
			len(collected), end, cursor, offset, flowPaginateMaxIterations)
		if err != nil {
			return nil, err
		}

		var page []string

		cursor, offset, page = parsePaginateChildKeys(res)

		collected = append(collected, page...)
		if cursor == "0" || len(collected) >= limit {
			break
		}
	}

	if len(collected) > limit {
		collected = collected[:limit]
	}

	return collected, nil
}

// parsePaginateChildKeys parses a paginate reply `[cursor, offset, items,
// numItems, jobs]` into its cursor, offset and item keys. Hash items are
// `[field, value]` pairs; only the field (the child key) is kept.
func parsePaginateChildKeys(res any) (cursor string, offset int64, keys []string) {
	cursor, offset, items, _, _ := parsePaginateReply(res)
	for _, it := range items {
		if k, ok := paginateItemKey(it); ok {
			keys = append(keys, k)
		}
	}

	return cursor, offset, keys
}
