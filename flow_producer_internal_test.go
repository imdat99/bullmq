package bullmq

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func flowLeaf(name string) JobNode {
	return JobNode{Job: newJob(name, json.RawMessage("{}"), JobOptions{})}
}

func TestFlowApplyPipelineResultsUpdatesOnlyRootID(t *testing.T) {
	root := JobNode{
		Job:      newJob("root", json.RawMessage("{}"), JobOptions{}),
		Children: []JobNode{flowLeaf("child-a"), flowLeaf("child-b")},
	}
	root.Children[0].Job.ID = "mem-child-a"
	root.Children[1].Job.ID = "mem-child-b"
	results := []any{"root-id", "child-a-id", "child-b-id"}
	if _, err := applyPipelineResults(&root, results, true); err != nil {
		t.Fatal(err)
	}
	if root.Job.ID != "root-id" {
		t.Fatalf("root id = %q", root.Job.ID)
	}
	if root.Children[0].Job.ID != "mem-child-a" || root.Children[1].Job.ID != "mem-child-b" {
		t.Fatalf("child ids changed: %q %q", root.Children[0].Job.ID, root.Children[1].Job.ID)
	}
}

func TestFlowApplyPipelineResultsErrorsOnNegativeChildCode(t *testing.T) {
	root := JobNode{
		Job:      newJob("root", json.RawMessage("{}"), JobOptions{}),
		Children: []JobNode{flowLeaf("child")},
	}
	_, err := applyPipelineResults(&root, []any{"root-id", int64(-3)}, true)
	if err == nil {
		t.Fatal("negative child code should propagate")
	}
	if code, ok := ScriptErrorCode(err); !ok || code != -3 {
		t.Fatalf("unexpected error %v", err)
	}
}

func TestFlowApplyPipelineResultsToleratesDeduplicatedChild(t *testing.T) {
	// A child whose internal parent was deduplicated returns -5 (its new
	// parent key was never created); that must not fail the whole flow.
	root := JobNode{
		Job:      newJob("root", json.RawMessage("{}"), JobOptions{}),
		Children: []JobNode{flowLeaf("child")},
	}
	root.Children[0].Job.ID = "mem-child"
	if _, err := applyPipelineResults(&root, []any{"existing-root-id", int64(-5)}, true); err != nil {
		t.Fatal(err)
	}
	if root.Job.ID != "existing-root-id" {
		t.Fatalf("root id = %q", root.Job.ID)
	}
	if root.Children[0].Job.ID != "mem-child" {
		t.Fatalf("child id = %q", root.Children[0].Job.ID)
	}
}

func TestFlowApplyPipelineResultsErrorsOnNegativeRootCode(t *testing.T) {
	root := JobNode{Job: newJob("root", json.RawMessage("{}"), JobOptions{})}
	root.Job.ParentKey = "bull:parent-queue:missing"
	_, err := applyPipelineResults(&root, []any{int64(-5)}, true)
	if err == nil {
		t.Fatal("negative root code should propagate")
	}
	if code, ok := ScriptErrorCode(err); !ok || code != CodeParentJobNotExist {
		t.Fatalf("unexpected error %v", err)
	}
	if !strings.Contains(err.Error(), "Missing key for parent job bull:parent-queue:missing. addJob") {
		t.Fatalf("unexpected message %v", err)
	}
}

func TestFlowPipelineScriptsIncludeFinalParentReleaseCheck(t *testing.T) {
	flow := FlowJob{
		Name: "parent", QueueName: "queue", Data: map[string]any{},
		Children: []FlowJob{{Name: "child", QueueName: "queue", Data: map[string]any{}}},
	}
	scripts := flowScriptNames(&flow)
	for _, want := range []string{"addParentJob", "addStandardJob", "releaseParentIfNoPendingDependencies"} {
		if !containsString(scripts, want) {
			t.Fatalf("scripts %v missing %s", scripts, want)
		}
	}
}

func TestFlowRejectsDeferredDeduplicationForChildNodes(t *testing.T) {
	child := FlowJob{
		Name: "child", QueueName: "queue", Data: map[string]any{},
		Opts: &JobOptions{Deduplication: &DeduplicationOptions{KeepLastIfActive: true}},
	}
	flow := FlowJob{Name: "parent", QueueName: "queue", Data: map[string]any{}, Children: []FlowJob{child}}
	err := validateDeferredChildDeduplication(&flow, false)
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("expected invalid config, got %v", err)
	}
}

func TestFlowAllowsDeferredDeduplicationWithoutAParent(t *testing.T) {
	flow := FlowJob{
		Name: "root", QueueName: "queue", Data: map[string]any{},
		Opts: &JobOptions{Deduplication: &DeduplicationOptions{KeepLastIfActive: true}},
	}
	if err := validateDeferredChildDeduplication(&flow, false); err != nil {
		t.Fatal(err)
	}
}

func TestFlowReleaseParentContextsAreDeduplicatedByKey(t *testing.T) {
	parent := func(id string) parentContext {
		return parentContext{
			parentID:       id,
			parentKey:      "bull:queue:parent",
			parentQueueKey: "bull:queue",
			parentDepsKey:  "bull:queue:parent:dependencies",
		}
	}
	parents := dedupParentContexts([]parentContext{parent("first"), parent("second")})
	if len(parents) != 1 {
		t.Fatalf("len = %d", len(parents))
	}
	if parents[0].parentKey != "bull:queue:parent" || parents[0].parentID != "second" {
		t.Fatalf("unexpected parent %+v", parents[0])
	}
}

func TestFlowPipelineScriptsIncludeExternalParentReleaseCheckForLeaf(t *testing.T) {
	flow := FlowJob{
		Name: "child", QueueName: "queue", Data: map[string]any{},
		Opts: &JobOptions{Parent: &ParentOptions{ID: "parent-id", Queue: "parent-queue"}},
	}
	if !containsString(flowScriptNames(&flow), "releaseParentIfNoPendingDependencies") {
		t.Fatal("release check missing")
	}
}

func TestFlowParseChildKeyPreservesColonsInLegacyRepeatIDs(t *testing.T) {
	got, ok := splitQualifiedJobKey("bull:queue:repeat:scheduler-id:1700000000000")
	want := splitJobKey{prefix: "bull", queueName: "queue", jobID: "repeat:scheduler-id:1700000000000"}
	if !ok || got != want {
		t.Fatalf("got %+v, %v", got, ok)
	}
}

func TestFlowParseChildKeyRejectsIncompleteKeys(t *testing.T) {
	for _, k := range []string{"bull:queue", "bull::job-1", "bull:queue:"} {
		if _, ok := splitQualifiedJobKey(k); ok {
			t.Fatalf("%q should be rejected", k)
		}
	}
}

func TestFlowJobNewReturnsSerializationErrors(t *testing.T) {
	// The data is serialized before anything is sent to Redis, so the client
	// never needs to connect.
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	fp, err := NewFlowProducerWithConnection(NewRedisConnectionFromClient(client), "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = fp.Add(context.Background(), FlowJob{Name: "job", QueueName: "queue", Data: make(chan int)})
	if err == nil || !strings.Contains(err.Error(), "marshal job data") {
		t.Fatalf("serialization failures should be returned, got %v", err)
	}
}

func TestFlowParsePaginateResultHandlesSetMembers(t *testing.T) {
	res := []any{"0", int64(2), []any{"bull:q:1", "bull:q:2"}, int64(2), []any{}}
	cursor, offset, keys := parsePaginateChildKeys(res)
	if cursor != "0" || offset != 2 {
		t.Fatalf("cursor=%q offset=%d", cursor, offset)
	}
	if strings.Join(keys, ",") != "bull:q:1,bull:q:2" {
		t.Fatalf("keys = %v", keys)
	}
}

func TestFlowParsePaginateResultHandlesHashFields(t *testing.T) {
	res := []any{int64(5), int64(1), []any{[]any{"bull:q:child", `{"result":1}`}}, int64(1), []any{}}
	cursor, offset, keys := parsePaginateChildKeys(res)
	if cursor != "5" || offset != 1 {
		t.Fatalf("cursor=%q offset=%d", cursor, offset)
	}
	if len(keys) != 1 || keys[0] != "bull:q:child" {
		t.Fatalf("keys = %v", keys)
	}
}

func TestFlowApplyQueueDefaultsDoesNotMutateInput(t *testing.T) {
	flow := FlowJob{
		Name: "parent", QueueName: "p",
		Children: []FlowJob{{Name: "child", QueueName: "c", Opts: &JobOptions{Attempts: 5}}},
	}
	opts := &FlowOptions{QueuesOptions: map[string]FlowQueueOptions{
		"c": {DefaultJobOptions: JobOptions{Attempts: 2, Priority: 3}},
	}}
	got := applyQueueDefaults(flow, opts)
	if got.Opts != nil {
		t.Fatalf("parent opts = %+v", got.Opts)
	}
	if o := got.Children[0].Opts; o.Attempts != 5 || o.Priority != 3 {
		t.Fatalf("child opts = %+v", o)
	}
	if o := flow.Children[0].Opts; o.Attempts != 5 || o.Priority != 0 {
		t.Fatalf("input mutated: %+v", o)
	}
}

// ── Integration sanity checks ───────────────────────────────────────────────

func flowTestRedisURL() string {
	if u := os.Getenv("REDIS_URL"); u != "" {
		return u
	}
	return "redis://127.0.0.1:6379"
}

func newFlowTestProducer(t *testing.T) *FlowProducer {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fp, err := NewFlowProducer(ctx, &FlowProducerOptions{Connection: RedisConnectionOptions{URL: flowTestRedisURL()}})
	if err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	t.Cleanup(func() { _ = fp.Close() })
	return fp
}

func newFlowTestQueue(t *testing.T, fp *FlowProducer) *Queue {
	t.Helper()
	name := "test-flow-" + uuid.NewString()
	q, err := NewQueueWithConnection(context.Background(), name, fp.Connection(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = q.Obliterate(ctx, true, 1000)
		_ = q.Close()
	})
	return q
}

func TestFlowAddParentWithChildrenAcrossQueues(t *testing.T) {
	ctx := context.Background()
	fp := newFlowTestProducer(t)
	parentQ := newFlowTestQueue(t, fp)
	childQ := newFlowTestQueue(t, fp)

	tree, err := fp.Add(ctx, FlowJob{
		Name: "parent", QueueName: parentQ.Name(), Data: map[string]any{"p": 1},
		Children: []FlowJob{
			{Name: "child-1", QueueName: childQ.Name(), Data: map[string]any{"c": 1}},
			{Name: "child-2", QueueName: childQ.Name(), Data: map[string]any{"c": 2}, Opts: &JobOptions{Priority: 3}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if tree.Job.ID == "" || len(tree.Children) != 2 {
		t.Fatalf("unexpected tree %+v", tree)
	}
	if tree.Job.QueueName != parentQ.Name() || tree.Children[0].Job.QueueName != childQ.Name() {
		t.Fatal("jobs are not bound to their queues")
	}
	state, err := tree.Job.GetState(ctx)
	if err != nil || state != StateWaitingChildren {
		t.Fatalf("parent state = %v, %v", state, err)
	}
	for i, want := range []JobState{StateWaiting, StatePrioritized} {
		st, err := tree.Children[i].Job.GetState(ctx)
		if err != nil || st != want {
			t.Fatalf("child %d state = %v, %v (want %v)", i, st, err, want)
		}
		if tree.Children[i].Job.ParentKey != parentQ.Keys().JobKey(tree.Job.ID) {
			t.Fatalf("child %d parent key = %q", i, tree.Children[i].Job.ParentKey)
		}
	}
	counts, err := tree.Job.GetDependenciesCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Unprocessed != 2 || counts.Processed != 0 {
		t.Fatalf("dependency counts = %+v", counts)
	}
	n, err := parentQ.GetWaitingChildrenCount(ctx)
	if err != nil || n != 1 {
		t.Fatalf("waiting-children count = %d, %v", n, err)
	}

	// A missing external parent fails the root.
	_, err = fp.Add(ctx, FlowJob{
		Name: "orphan", QueueName: childQ.Name(),
		Opts: &JobOptions{Parent: &ParentOptions{ID: "missing", Queue: parentQ.Name()}},
	})
	if code, ok := ScriptErrorCode(err); !ok || code != CodeParentJobNotExist {
		t.Fatalf("expected missing parent error, got %v", err)
	}
}

func TestFlowGetFlowReturnsTree(t *testing.T) {
	ctx := context.Background()
	fp := newFlowTestProducer(t)
	parentQ := newFlowTestQueue(t, fp)
	childQ := newFlowTestQueue(t, fp)

	added, err := fp.Add(ctx, FlowJob{
		Name: "root", QueueName: parentQ.Name(),
		Children: []FlowJob{{
			Name: "mid", QueueName: childQ.Name(), Data: map[string]any{"foo": "bar"},
			Children: []FlowJob{{Name: "leaf", QueueName: childQ.Name(), Data: map[string]any{"foo": "baz"}}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	tree, err := fp.GetFlow(ctx, GetFlowOptions{QueueName: parentQ.Name(), ID: added.Job.ID})
	if err != nil {
		t.Fatal(err)
	}
	if tree.Job.ID != added.Job.ID || len(tree.Children) != 1 {
		t.Fatalf("unexpected root %+v", tree)
	}
	mid := tree.Children[0]
	var data map[string]string
	if err := mid.Job.DecodeData(&data); err != nil || data["foo"] != "bar" {
		t.Fatalf("mid data = %v, %v", data, err)
	}
	if mid.Job.QueueName != childQ.Name() {
		t.Fatalf("mid queue = %q", mid.Job.QueueName)
	}
	if st, err := mid.Job.GetState(ctx); err != nil || st != StateWaitingChildren {
		t.Fatalf("mid state = %v, %v", st, err)
	}
	if len(mid.Children) != 1 || mid.Children[0].Children != nil {
		t.Fatalf("unexpected mid children %+v", mid.Children)
	}

	shallow, err := fp.GetFlow(ctx, GetFlowOptions{QueueName: parentQ.Name(), ID: added.Job.ID, Depth: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(shallow.Children) != 1 || shallow.Children[0].Children != nil {
		t.Fatalf("depth 1 should stop at the children: %+v", shallow)
	}

	_, err = fp.GetFlow(ctx, GetFlowOptions{QueueName: parentQ.Name(), ID: "does-not-exist"})
	if !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestFlowAddBulkWithQueueDefaults(t *testing.T) {
	ctx := context.Background()
	fp := newFlowTestProducer(t)
	q := newFlowTestQueue(t, fp)

	opts := &FlowOptions{QueuesOptions: map[string]FlowQueueOptions{
		q.Name(): {DefaultJobOptions: JobOptions{Attempts: 4}},
	}}
	nodes, err := fp.AddBulkWithOptions(ctx, []FlowJob{
		{Name: "a", QueueName: q.Name(), Children: []FlowJob{{Name: "a1", QueueName: q.Name()}}},
		{Name: "b", QueueName: q.Name(), Opts: &JobOptions{Attempts: 7}},
	}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("len = %d", len(nodes))
	}
	for _, tc := range []struct {
		id       string
		attempts int
	}{{nodes[0].Job.ID, 4}, {nodes[0].Children[0].Job.ID, 4}, {nodes[1].Job.ID, 7}} {
		j, err := q.GetJob(ctx, tc.id)
		if err != nil || j == nil {
			t.Fatalf("GetJob(%s) = %v, %v", tc.id, j, err)
		}
		if j.Opts.Attempts != tc.attempts {
			t.Fatalf("job %s attempts = %d, want %d", tc.id, j.Opts.Attempts, tc.attempts)
		}
	}
	if st, err := nodes[1].Job.GetState(ctx); err != nil || st != StateWaiting {
		t.Fatalf("b state = %v, %v", st, err)
	}
}
