// Queue tests — creation, add, bulk, pause, drain, obliterate, getters.
// Ported from bullmq_rust/tests/queue_test.rs.
package bullmq_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/imdat99/bullmq"
)

// qtDecodeData decodes a job's JSON data into a generic map.
func qtDecodeData(t *testing.T, job *bullmq.Job) map[string]any {
	t.Helper()
	var m map[string]any
	if err := job.DecodeData(&m); err != nil {
		t.Fatalf("decode data of job %s: %v", job.ID, err)
	}
	return m
}

// qtJSON normalizes v through a JSON round-trip so it can be compared with
// decoded job data.
func qtJSON(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

// qtExpectErrContains asserts err is non-nil and contains msg.
func qtExpectErrContains(t *testing.T, err error, msg string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q, got nil", msg)
	}
	if !strings.Contains(err.Error(), msg) {
		t.Fatalf("got: %v", err)
	}
}

// qtNullProcessor completes every job with a null result.
func qtNullProcessor(ctx context.Context, job *bullmq.Job) (any, error) {
	return nil, nil
}

// qtAddChildren adds n children ({"idx": i}) of parentID to q.
func qtAddChildren(t *testing.T, q *bullmq.Queue, parentQueue, parentID string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		_, err := q.Add(ctx, "child", map[string]any{"idx": i}, &bullmq.JobOptions{
			Parent: &bullmq.ParentOptions{Queue: parentQueue, ID: parentID},
		})
		noErr(t, err)
	}
}

// qtFindJob returns the job with id in jobs or fails the test.
func qtFindJob(t *testing.T, jobs []*bullmq.Job, id, msg string) *bullmq.Job {
	t.Helper()
	for _, j := range jobs {
		if j.ID == id {
			return j
		}
	}
	t.Fatalf("%s", msg)
	return nil
}

// ═══════════════════════════════════════════════════════════════════════════
// Queue CRUD
// ═══════════════════════════════════════════════════════════════════════════

func TestQueueCreation(t *testing.T) {
	name := testQueueName()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: testConnection()})
	if queue.Name() != name {
		t.Fatalf("name %q != %q", queue.Name(), name)
	}
}

// Rust: test_queue_name_cannot_contain_colon (renamed: worker_part2_test.go
// already declares TestQueueNameCannotContainColon for the Worker variant).
func TestQueueQueueNameCannotContainColon(t *testing.T) {
	q, err := bullmq.NewQueue(context.Background(), "invalid:queue", &bullmq.QueueOptions{Connection: testConnection()})
	if err == nil {
		_ = q.Close()
		t.Fatal("expected error")
	}
	qtExpectErrContains(t, err, "Queue name cannot contain :")
}

func TestAddJob(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)
	job := must(queue.Add(ctx, "test-job", map[string]any{"key": "value"}, nil))

	if job.ID == "" {
		t.Fatal("empty job id")
	}
	if job.Name != "test-job" {
		t.Fatalf("name %q", job.Name)
	}
	if got := qtDecodeData(t, job); !reflect.DeepEqual(got, map[string]any{"key": "value"}) {
		t.Fatalf("data %v", got)
	}
}

func TestAddJobWithCustomId(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)
	job := must(queue.Add(ctx, "custom-id-job", map[string]any{}, &bullmq.JobOptions{JobID: "my-custom-id"}))
	if job.ID != "my-custom-id" {
		t.Fatalf("id %q", job.ID)
	}
}

func TestAddJobWithParentQueueName(t *testing.T) {
	ctx := context.Background()
	parentName := testQueueName()
	childName := testQueueName()

	parentQueue := newTestQueue(t, parentName, nil)
	childQueue := newTestQueue(t, childName, nil)

	parent := must(parentQueue.Add(ctx, "parent", map[string]any{}, &bullmq.JobOptions{JobID: "parent-id"}))

	child := must(childQueue.Add(ctx, "child", map[string]any{}, &bullmq.JobOptions{
		// ParentOptions.Queue is a queue name, not a qualified key.
		Parent:                    &bullmq.ParentOptions{Queue: parentName, ID: parent.ID},
		FailParentOnFailure:       true,
		IgnoreDependencyOnFailure: true,
		RemoveDependencyOnFailure: true,
		ContinueParentOnFailure:   true,
	}))

	if child.ID == "" {
		t.Fatal("empty child id")
	}

	raw := must(childQueue.Client().HGet(ctx, childQueue.Keys().JobKey(child.ID), "parent").Result())
	var storedParent map[string]any
	noErr(t, json.Unmarshal([]byte(raw), &storedParent))
	if storedParent["id"] != parent.ID {
		t.Fatalf("parent id %v", storedParent["id"])
	}
	if want := childQueue.Keys().Prefix() + ":" + parentName; storedParent["queueKey"] != want {
		t.Fatalf("queueKey %v want %s", storedParent["queueKey"], want)
	}
	for _, k := range []string{"fpof", "idof", "rdof", "cpof"} {
		if storedParent[k] != true {
			t.Fatalf("%s = %v, want true (parent=%s)", k, storedParent[k], raw)
		}
	}

	deps := must(parentQueue.GetDependenciesCount(ctx, parent.ID))
	if deps.Unprocessed != 1 {
		t.Fatalf("unprocessed %d", deps.Unprocessed)
	}
}

func TestGetDependenciesPending(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)
	parent := must(queue.Add(ctx, "parent", map[string]any{}, &bullmq.JobOptions{JobID: "parent-pending"}))
	qtAddChildren(t, queue, name, parent.ID, 4)

	result := must(queue.GetDependencies(ctx, parent.ID, "pending", 0, -1))
	if len(result.Items) != 4 || len(result.Jobs) != 4 || result.Total != 4 {
		t.Fatalf("items=%d jobs=%d total=%d", len(result.Items), len(result.Jobs), result.Total)
	}
	for _, item := range result.Items {
		if item.Value != nil {
			t.Fatalf("pending item has value: %+v", item)
		}
		if item.Err != "" {
			t.Fatalf("pending item has err: %+v", item)
		}
	}

	partial := must(queue.GetDependencies(ctx, parent.ID, "pending", 0, 2))
	if len(partial.Items) != 3 {
		t.Fatalf("partial items %d", len(partial.Items))
	}
	if partial.Total != 4 {
		t.Fatalf("partial total %d", partial.Total)
	}
}

func TestGetDependenciesWithEndBeforeStartReturnsEmptyPage(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)
	parent := must(queue.Add(ctx, "parent", map[string]any{}, &bullmq.JobOptions{JobID: "parent-empty-page"}))
	qtAddChildren(t, queue, name, parent.ID, 4)

	result := must(queue.GetDependencies(ctx, parent.ID, "pending", 3, 2))
	if len(result.Items) != 0 {
		t.Fatalf("items %d", len(result.Items))
	}
	if len(result.Jobs) != 0 {
		t.Fatalf("jobs %d", len(result.Jobs))
	}
	if result.Total != 4 {
		t.Fatalf("total %d", result.Total)
	}
}

func TestGetDependenciesProcessed(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: testConnection()})
	parent := must(queue.Add(ctx, "parent", map[string]any{}, &bullmq.JobOptions{JobID: "parent-processed"}))
	qtAddChildren(t, queue, name, parent.ID, 4)

	worker := newTestWorker(t, name, qtNullProcessor, &bullmq.WorkerOptions{
		Connection: testConnection(),
		DrainDelay: time.Second,
	})

	eventually(t, 10*time.Second, func() bool {
		return must(queue.GetDependenciesCount(ctx, parent.ID)).Processed >= 4
	}, "children did not complete in time")

	pending := must(queue.GetDependencies(ctx, parent.ID, "pending", 0, -1))
	if len(pending.Items) != 0 || pending.Total != 0 {
		t.Fatalf("pending items=%d total=%d", len(pending.Items), pending.Total)
	}

	processed := must(queue.GetDependencies(ctx, parent.ID, "processed", 0, -1))
	if len(processed.Items) != 4 || len(processed.Jobs) != 4 || processed.Total != 4 {
		t.Fatalf("processed items=%d jobs=%d total=%d", len(processed.Items), len(processed.Jobs), processed.Total)
	}
	// Rust asserts `item.v.is_some()`: the stored return value parsed as
	// JSON. In Go a parsed JSON null is a nil Value, so "parsed" is Err == "".
	for _, item := range processed.Items {
		if item.Err != "" {
			t.Fatalf("processed item has no parsed value: %+v", item)
		}
	}

	noErr(t, worker.Close(5*time.Second))
}

func TestGetDependenciesCrossQueueChildContext(t *testing.T) {
	ctx := context.Background()
	parentName := testQueueName()
	childName := testQueueName()
	prefix := "tenantregression"

	parentQueue := newTestQueue(t, parentName, &bullmq.QueueOptions{Connection: testConnection(), Prefix: prefix})
	childQueue := newTestQueue(t, childName, &bullmq.QueueOptions{Connection: testConnection(), Prefix: prefix})

	parent := must(parentQueue.Add(ctx, "parent", map[string]any{}, &bullmq.JobOptions{JobID: "parent-cross-context"}))
	child := must(childQueue.Add(ctx, "child", map[string]any{"x": 1}, &bullmq.JobOptions{
		JobID:  "child-cross-context",
		Parent: &bullmq.ParentOptions{Queue: parentName, ID: parent.ID},
	}))

	dependencies := must(parentQueue.GetDependencies(ctx, parent.ID, "pending", 0, -1))
	depChild := qtFindJob(t, dependencies.Jobs, child.ID, "child job should be present in dependencies jobs")

	// Regression guard: this must read from the child queue keys.
	if st := must(depChild.GetState(ctx)); st != bullmq.StateWaiting {
		t.Fatalf("state %s", st)
	}
}

func TestAddRejectsCustomJobIdWithColon(t *testing.T) {
	queue := newTestQueue(t, testQueueName(), nil)
	_, err := queue.Add(context.Background(), "job", map[string]any{}, &bullmq.JobOptions{JobID: "job:1"})
	qtExpectErrContains(t, err, "Custom Id cannot contain :")
}

func TestAddRejectsIntegerCustomJobId(t *testing.T) {
	queue := newTestQueue(t, testQueueName(), nil)
	_, err := queue.Add(context.Background(), "job", map[string]any{}, &bullmq.JobOptions{JobID: "100"})
	qtExpectErrContains(t, err, "Custom Id cannot be integers")
}

func TestGetDependenciesChildContextWithLegacyRepeatJobId(t *testing.T) {
	// Legacy repeatable ids (`repeat:<schedulerId>:<millis>`) are the one custom
	// id shape allowed to contain `:`. Prefixes and queue names cannot contain
	// `:`, so the qualified key `{prefix}:{queue}:repeat:sched:1` still splits
	// unambiguously: everything after the second separator is the id.
	ctx := context.Background()
	parentName := testQueueName()
	childName := testQueueName()

	parentQueue := newTestQueue(t, parentName, nil)
	childQueue := newTestQueue(t, childName, nil)

	parent := must(parentQueue.Add(ctx, "parent", map[string]any{}, &bullmq.JobOptions{JobID: "parent-legacy-repeat"}))

	childID := "repeat:scheduler-id:1700000000000"
	child := must(childQueue.Add(ctx, "child", map[string]any{"x": 1}, &bullmq.JobOptions{
		JobID:  childID,
		Parent: &bullmq.ParentOptions{Queue: parentName, ID: parent.ID},
	}))
	if child.ID != childID {
		t.Fatalf("child id %q", child.ID)
	}

	dependencies := must(parentQueue.GetDependencies(ctx, parent.ID, "pending", 0, -1))
	depChild := qtFindJob(t, dependencies.Jobs, childID, "child job should keep its full colon-containing id")

	// Regression guard: this must read from the child queue keys, which is only
	// possible if the colon boundary was resolved correctly.
	if st := must(depChild.GetState(ctx)); st != bullmq.StateWaiting {
		t.Fatalf("state %s", st)
	}
}

func TestGetDependenciesProcessedCrossQueueChildContext(t *testing.T) {
	ctx := context.Background()
	parentName := testQueueName()
	childName := testQueueName()
	prefix := "tenantregression"

	parentQueue := newTestQueue(t, parentName, &bullmq.QueueOptions{Connection: testConnection(), Prefix: prefix})
	childQueue := newTestQueue(t, childName, &bullmq.QueueOptions{Connection: testConnection(), Prefix: prefix})

	parent := must(parentQueue.Add(ctx, "parent", map[string]any{}, &bullmq.JobOptions{JobID: "parent-cross-processed"}))
	child := must(childQueue.Add(ctx, "child", map[string]any{"x": 2}, &bullmq.JobOptions{
		JobID:  "child-cross-processed",
		Parent: &bullmq.ParentOptions{Queue: parentName, ID: parent.ID},
	}))

	worker := newTestWorker(t, childName, qtNullProcessor, &bullmq.WorkerOptions{
		Connection: testConnection(),
		Prefix:     "tenantregression",
		DrainDelay: time.Second,
	})

	eventually(t, 10*time.Second, func() bool {
		c := must(parentQueue.GetDependenciesCount(ctx, parent.ID))
		return c.Processed >= 1 && c.Unprocessed == 0
	}, "child did not reach processed dependencies")

	dependencies := must(parentQueue.GetDependencies(ctx, parent.ID, "processed", 0, -1))
	depChild := qtFindJob(t, dependencies.Jobs, child.ID, "child job should be present in processed dependencies jobs")

	// Regression guard: this state lookup must use child queue keys.
	if st := must(depChild.GetState(ctx)); st != bullmq.StateCompleted {
		t.Fatalf("state %s", st)
	}

	// Stop worker first so the retried job remains in waiting state deterministically.
	noErr(t, worker.Close(5*time.Second))

	// Additional guard: a mutating child operation should also route to child keys.
	noErr(t, depChild.Retry(ctx, bullmq.StateCompleted, nil))
	if st := must(childQueue.GetJobState(ctx, child.ID)); st != bullmq.StateWaiting {
		t.Fatalf("state after retry %s", st)
	}
}

func TestGetDependenciesProcessedCrossPrefixChildContext(t *testing.T) {
	ctx := context.Background()
	parentName := testQueueName()
	childName := testQueueName()
	parentPrefix := "parentpref"
	childPrefix := "childpref"

	parentQueue := newTestQueue(t, parentName, &bullmq.QueueOptions{Connection: testConnection(), Prefix: parentPrefix})
	childQueue := newTestQueue(t, childName, &bullmq.QueueOptions{Connection: testConnection(), Prefix: childPrefix})

	flow := bullmq.FlowJob{
		Name:      "parent",
		QueueName: parentName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{{
			Name:      "child",
			QueueName: childName,
			Data:      map[string]any{"x": 3},
			Prefix:    childPrefix,
		}},
	}

	flowProducer := must(bullmq.NewFlowProducer(ctx, &bullmq.FlowProducerOptions{
		Connection: testConnection(),
		Prefix:     parentPrefix,
	}))
	t.Cleanup(func() { _ = flowProducer.Close() })

	tree := must(flowProducer.Add(ctx, flow))
	parentID := tree.Job.ID

	worker := newTestWorker(t, childName, qtNullProcessor, &bullmq.WorkerOptions{
		Connection: testConnection(),
		Prefix:     childPrefix,
		DrainDelay: time.Second,
	})

	eventually(t, 10*time.Second, func() bool {
		c := must(parentQueue.GetDependenciesCount(ctx, parentID))
		return c.Processed >= 1 && c.Unprocessed == 0
	}, "child did not reach processed dependencies")

	dependencies := must(parentQueue.GetDependencies(ctx, parentID, "processed", 0, -1))
	if len(dependencies.Jobs) != 1 {
		t.Fatalf("jobs %d", len(dependencies.Jobs))
	}
	depChild := dependencies.Jobs[0]

	// Regression guard: this lookup must use child queue keys (child prefix).
	if st := must(depChild.GetState(ctx)); st != bullmq.StateCompleted {
		t.Fatalf("state %s", st)
	}

	// Stop worker first so the retried job remains in waiting state deterministically.
	noErr(t, worker.Close(5*time.Second))

	// Additional guard: mutating op should also target child-prefix keys.
	noErr(t, depChild.Retry(ctx, bullmq.StateCompleted, nil))
	if st := must(childQueue.GetJobState(ctx, depChild.ID)); st != bullmq.StateWaiting {
		t.Fatalf("state after retry %s", st)
	}

	noErr(t, flowProducer.Close())
}

// Regression: a legacy child stored under a prefix other than the parent's,
// with a colon-containing id and *without* the persisted `opts.jobId` that
// newer versions write. Because prefixes and queue names may not contain `:`,
// the dependency parser must still recover the child's queue context from the
// qualified key alone.
func TestGetDependenciesLegacyForeignPrefixChildContext(t *testing.T) {
	ctx := context.Background()
	parentName := testQueueName()
	childName := testQueueName()
	parentPrefix := "legacyparentpref"
	childPrefix := "legacychildpref"

	parentQueue := newTestQueue(t, parentName, &bullmq.QueueOptions{Connection: testConnection(), Prefix: parentPrefix})
	childQueue := newTestQueue(t, childName, &bullmq.QueueOptions{Connection: testConnection(), Prefix: childPrefix})

	parent := must(parentQueue.Add(ctx, "parent", map[string]any{}, nil))
	childID := "repeat:scheduler-id:1700000000000"
	child := must(childQueue.Add(ctx, "child", map[string]any{"x": 1}, &bullmq.JobOptions{JobID: childID}))
	if child.ID != childID {
		t.Fatalf("child id %q", child.ID)
	}

	// The dependency link is wired directly in Redis: `ParentOptions` qualifies
	// the parent with the *child's* prefix, so it cannot express a parent
	// living under a different prefix. These are the keys a cross-prefix flow
	// stores.
	parentKey := parentQueue.Keys().JobKey(parent.ID)
	childKey := childQueue.Keys().JobKey(child.ID)
	parentDepsKey := parentKey + ":dependencies"
	rc := parentQueue.Client()
	noErr(t, rc.SAdd(ctx, parentDepsKey, childKey).Err())
	parentJSON, err := json.Marshal(map[string]any{
		"id":       parent.ID,
		"queueKey": parentQueue.Keys().Base(),
	})
	noErr(t, err)
	noErr(t, rc.HSet(ctx, childKey,
		"parentKey", parentKey,
		"parent", string(parentJSON),
		// Jobs added before `opts.jobId` was persisted carry no boundary
		// marker, so the colon-containing id must be recovered positionally.
		"opts", "{}",
	).Err())

	dependencies := must(parentQueue.GetDependencies(ctx, parent.ID, "pending", 0, -1))
	if len(dependencies.Jobs) != 1 {
		t.Fatalf("jobs %d", len(dependencies.Jobs))
	}
	depChild := dependencies.Jobs[0]
	if depChild.ID != childID {
		t.Fatalf("dep child id %q", depChild.ID)
	}

	// Regression guard: this lookup must use the child queue keys (child
	// prefix, child queue name), not a mis-split of the qualified key.
	if st := must(depChild.GetState(ctx)); st != bullmq.StateWaiting {
		t.Fatalf("state %s", st)
	}
}

func TestAddBulkJobs(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)
	jobs := must(queue.AddBulk(ctx, []bullmq.BulkJob{
		{Name: "job-1", Data: map[string]any{"n": 1}},
		{Name: "job-2", Data: map[string]any{"n": 2}},
		{Name: "job-3", Data: map[string]any{"n": 3}},
	}))
	if len(jobs) != 3 {
		t.Fatalf("len %d", len(jobs))
	}
	for i, want := range []string{"job-1", "job-2", "job-3"} {
		if jobs[i].Name != want {
			t.Fatalf("jobs[%d].Name = %q", i, jobs[i].Name)
		}
	}
}

func TestAddBulkEmpty(t *testing.T) {
	queue := newTestQueue(t, testQueueName(), nil)
	jobs := must(queue.AddBulk(context.Background(), []bullmq.BulkJob{}))
	if len(jobs) != 0 {
		t.Fatalf("len %d", len(jobs))
	}
}

func TestAddBulkWithOptions(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)
	jobs := must(queue.AddBulk(ctx, []bullmq.BulkJob{
		{Name: "normal", Data: map[string]any{}},
		{Name: "delayed", Data: map[string]any{}, Opts: &bullmq.JobOptions{Delay: 60 * time.Second}},
		{Name: "priority", Data: map[string]any{}, Opts: &bullmq.JobOptions{Priority: 5}},
	}))

	if len(jobs) != 3 {
		t.Fatalf("len %d", len(jobs))
	}
	if jobs[0].Name != "normal" {
		t.Fatalf("jobs[0] %q", jobs[0].Name)
	}
	if jobs[1].Name != "delayed" || jobs[1].Delay != 60*time.Second {
		t.Fatalf("jobs[1] %q delay %s", jobs[1].Name, jobs[1].Delay)
	}
	if jobs[2].Name != "priority" || jobs[2].Priority() != 5 {
		t.Fatalf("jobs[2] %q priority %d", jobs[2].Name, jobs[2].Priority())
	}

	counts := must(queue.GetJobCounts(ctx))
	if counts.Waiting != 1 || counts.Delayed != 1 || counts.Prioritized != 1 {
		t.Fatalf("counts %+v", counts)
	}
}

func TestGetJob(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)
	added := must(queue.Add(ctx, "fetch-test", map[string]any{"hello": "world"}, nil))

	fetched := must(queue.GetJob(ctx, added.ID))
	if fetched == nil {
		t.Fatal("job not found")
	}
	if fetched.ID != added.ID || fetched.Name != "fetch-test" {
		t.Fatalf("fetched %s %q", fetched.ID, fetched.Name)
	}
}

func TestGetJobNotFound(t *testing.T) {
	queue := newTestQueue(t, testQueueName(), nil)
	if result := must(queue.GetJob(context.Background(), "nonexistent-id")); result != nil {
		t.Fatalf("expected nil, got %+v", result)
	}
}

func TestRemoveJob(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)
	job := must(queue.Add(ctx, "to-remove", map[string]any{}, nil))

	if !must(queue.Remove(ctx, job.ID)) {
		t.Fatal("remove returned false")
	}
	if fetched := must(queue.GetJob(ctx, job.ID)); fetched != nil {
		t.Fatal("job still exists")
	}
}

func TestRemoveNonexistentJob(t *testing.T) {
	queue := newTestQueue(t, testQueueName(), nil)
	// Removing a job that does not exist is not an error and reports success,
	// since there is nothing left to remove (mirrors Node.js semantics).
	if !must(queue.Remove(context.Background(), "does-not-exist")) {
		t.Fatal("remove returned false")
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Pause / Resume / Drain / Obliterate
// ═══════════════════════════════════════════════════════════════════════════

func TestPauseResume(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	if must(queue.IsPaused(ctx)) {
		t.Fatal("paused initially")
	}
	noErr(t, queue.Pause(ctx))
	if !must(queue.IsPaused(ctx)) {
		t.Fatal("not paused after pause")
	}
	noErr(t, queue.Resume(ctx))
	if must(queue.IsPaused(ctx)) {
		t.Fatal("paused after resume")
	}
}

func TestDrain(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	for i := 0; i < 5; i++ {
		must(queue.Add(ctx, "drain-job-"+string(rune('0'+i)), map[string]any{}, nil))
	}

	noErr(t, queue.Drain(ctx, false))

	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 0 {
		t.Fatalf("waiting %d", c.Waiting)
	}
}

func TestDrainWithDelayedJobs(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	for i := 0; i < 3; i++ {
		must(queue.Add(ctx, "normal-"+string(rune('0'+i)), map[string]any{}, nil))
	}
	for i := 0; i < 2; i++ {
		must(queue.Add(ctx, "delayed-"+string(rune('0'+i)), map[string]any{}, &bullmq.JobOptions{Delay: 60 * time.Second}))
	}

	noErr(t, queue.Drain(ctx, false))
	c := must(queue.GetJobCounts(ctx))
	if c.Waiting != 0 || c.Delayed != 2 {
		t.Fatalf("after drain(false): %+v", c)
	}

	noErr(t, queue.Drain(ctx, true))
	if c := must(queue.GetJobCounts(ctx)); c.Delayed != 0 {
		t.Fatalf("after drain(true): %+v", c)
	}
}

func TestObliterate(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	for i := 0; i < 10; i++ {
		must(queue.Add(ctx, "obliterate-"+string(rune('0'+i)), map[string]any{}, nil))
	}

	noErr(t, queue.Obliterate(ctx, true, 1000))

	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 0 {
		t.Fatalf("waiting %d", c.Waiting)
	}
}

func TestQueueCloseIsIdempotent(t *testing.T) {
	queue, err := bullmq.NewQueue(context.Background(), testQueueName(), &bullmq.QueueOptions{Connection: testConnection()})
	noErr(t, err)
	_ = queue.Close()
	_ = queue.Close() // second close should not panic
}

// ═══════════════════════════════════════════════════════════════════════════
// Job Data & Options
// ═══════════════════════════════════════════════════════════════════════════

func TestJobDataTypes(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	data := map[string]any{
		"string": "hello",
		"number": 42,
		"float":  3.15,
		"bool":   true,
		"null":   nil,
		"array":  []any{1, 2, 3},
		"nested": map[string]any{"key": "value"},
	}

	job := must(queue.Add(ctx, "complex-data", data, nil))
	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched == nil {
		t.Fatal("job not found")
	}
	var got any
	noErr(t, fetched.DecodeData(&got))
	if want := qtJSON(t, data); !reflect.DeepEqual(got, want) {
		t.Fatalf("data %v, want %v", got, want)
	}
}

func TestJobWithDelay(t *testing.T) {
	queue := newTestQueue(t, testQueueName(), nil)
	job := must(queue.Add(context.Background(), "delayed-job", map[string]any{}, &bullmq.JobOptions{Delay: 5 * time.Second}))
	if job.Delay != 5*time.Second {
		t.Fatalf("delay %s", job.Delay)
	}
}

func TestJobWithPriority(t *testing.T) {
	queue := newTestQueue(t, testQueueName(), nil)
	job := must(queue.Add(context.Background(), "priority-job", map[string]any{}, &bullmq.JobOptions{Priority: 10}))
	if job.Priority() != 10 {
		t.Fatalf("priority %d", job.Priority())
	}
}

func TestJobWithAttempts(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)
	job := must(queue.Add(ctx, "retry-job", map[string]any{}, &bullmq.JobOptions{Attempts: 3}))

	fetched := must(queue.GetJob(ctx, job.ID))
	if fetched == nil {
		t.Fatal("job not found")
	}
	if fetched.Opts.Attempts != 3 {
		t.Fatalf("attempts %d", fetched.Opts.Attempts)
	}
}

func TestAddJobLifo(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	noErr(t, queue.Pause(ctx))

	for i := 0; i < 4; i++ {
		must(queue.Add(ctx, "lifo-"+string(rune('0'+i)), map[string]any{"order": i}, &bullmq.JobOptions{LIFO: true}))
	}

	if c := must(queue.GetJobCounts(ctx)); c.Waiting != 4 {
		t.Fatalf("waiting %d", c.Waiting)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Getters / State
// ═══════════════════════════════════════════════════════════════════════════

func TestGetJobStateWaiting(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)
	job := must(queue.Add(ctx, "state-test", map[string]any{}, nil))
	if st := must(queue.GetJobState(ctx, job.ID)); st != bullmq.StateWaiting {
		t.Fatalf("state %s", st)
	}
}

func TestGetJobStateDelayed(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)
	job := must(queue.Add(ctx, "delayed-state", map[string]any{}, &bullmq.JobOptions{Delay: 60 * time.Second}))
	if st := must(queue.GetJobState(ctx, job.ID)); st != bullmq.StateDelayed {
		t.Fatalf("state %s", st)
	}
}

func TestGetJobStatePrioritized(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)
	job := must(queue.Add(ctx, "priority-state", map[string]any{}, &bullmq.JobOptions{Priority: 5}))
	st := must(queue.GetJobState(ctx, job.ID))
	if st != bullmq.StateWaiting && st != bullmq.StatePrioritized {
		t.Fatalf("Expected Waiting or Prioritized, got %s", st)
	}
}

func TestGetJobCountsMultipleStates(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	for i := 0; i < 3; i++ {
		must(queue.Add(ctx, "wait-"+string(rune('0'+i)), map[string]any{}, nil))
	}
	for i := 0; i < 2; i++ {
		must(queue.Add(ctx, "delayed-"+string(rune('0'+i)), map[string]any{}, &bullmq.JobOptions{Delay: 60 * time.Second}))
	}

	c := must(queue.GetJobCounts(ctx))
	if c.Waiting != 3 || c.Delayed != 2 || c.Active != 0 || c.Completed != 0 || c.Failed != 0 {
		t.Fatalf("counts %+v", c)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Unit Tests (keys, types)
// ═══════════════════════════════════════════════════════════════════════════

func TestQueueKeys(t *testing.T) {
	keys := bullmq.NewQueueKeys("my-queue", "")
	checks := []struct{ got, want string }{
		{keys.Base(), "bull:my-queue"},
		{keys.Wait(), "bull:my-queue:wait"},
		{keys.Active(), "bull:my-queue:active"},
		{keys.Delayed(), "bull:my-queue:delayed"},
		{keys.Completed(), "bull:my-queue:completed"},
		{keys.Failed(), "bull:my-queue:failed"},
		{keys.Stalled(), "bull:my-queue:stalled"},
		{keys.Meta(), "bull:my-queue:meta"},
		{keys.Events(), "bull:my-queue:events"},
		{keys.Marker(), "bull:my-queue:marker"},
		{keys.ID(), "bull:my-queue:id"},
		{keys.JobKey("123"), "bull:my-queue:123"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("got %q, want %q", c.got, c.want)
		}
	}
}

func TestQueueKeysCustomPrefix(t *testing.T) {
	keys := bullmq.NewQueueKeys("tasks", "myapp")
	if keys.Base() != "myapp:tasks" {
		t.Errorf("base %q", keys.Base())
	}
	if keys.Wait() != "myapp:tasks:wait" {
		t.Errorf("wait %q", keys.Wait())
	}
}

// TestJobStateParsing (Rust `JobState::from_redis_str`) lives in
// queue_jobstate_internal_test.go: the Go parser (`parseJobState`) is
// unexported, so it can only be tested from package bullmq.

func TestJobStateDisplay(t *testing.T) {
	// Rust `JobState::as_str` maps Waiting to the Redis list name "wait". The
	// Go JobState is a string type whose values are the Node.js state names,
	// so the waiting state renders as "waiting" (the Redis list name is
	// QueueKeys.Wait()).
	if string(bullmq.StateWaiting) != "waiting" {
		t.Errorf("waiting %q", bullmq.StateWaiting)
	}
	if string(bullmq.StateActive) != "active" {
		t.Errorf("active %q", bullmq.StateActive)
	}
	if string(bullmq.StateCompleted) != "completed" {
		t.Errorf("completed %q", bullmq.StateCompleted)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// State-list getters (getWaiting / getActive / getJobs / getRanges / counts)
// ═══════════════════════════════════════════════════════════════════════════

// Node.js: "should get waiting jobs"
func TestGetWaitingJobs(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))
	must(queue.Add(ctx, "test", map[string]any{"baz": "qux"}, nil))

	jobs := must(queue.GetWaiting(ctx, 0, -1))
	if len(jobs) != 2 {
		t.Fatalf("len %d", len(jobs))
	}
	if v := qtDecodeData(t, jobs[0])["foo"]; v != "bar" {
		t.Fatalf("jobs[0].foo = %v", v)
	}
	if v := qtDecodeData(t, jobs[1])["baz"]; v != "qux" {
		t.Fatalf("jobs[1].baz = %v", v)
	}
}

// Node.js: "should get all waiting jobs when no range is provided"
func TestGetWaitingJobsFullRange(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))
	must(queue.Add(ctx, "test", map[string]any{"baz": "qux"}, nil))
	must(queue.Add(ctx, "test", map[string]any{"bar": "qux"}, nil))
	must(queue.Add(ctx, "test", map[string]any{"baz": "xuq"}, nil))

	allJobs := must(queue.GetWaiting(ctx, 0, -1))
	if len(allJobs) != 4 {
		t.Fatalf("len %d", len(allJobs))
	}
	want := []struct{ k, v string }{{"foo", "bar"}, {"baz", "qux"}, {"bar", "qux"}, {"baz", "xuq"}}
	for i, w := range want {
		if v := qtDecodeData(t, allJobs[i])[w.k]; v != w.v {
			t.Fatalf("jobs[%d].%s = %v, want %s", i, w.k, v, w.v)
		}
	}
}

// Node.js: "should get paused jobs" (getWaiting also returns paused jobs)
func TestGetPausedJobsViaWaiting(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	noErr(t, queue.Pause(ctx))
	must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))
	must(queue.Add(ctx, "test", map[string]any{"baz": "qux"}, nil))

	if jobs := must(queue.GetWaiting(ctx, 0, -1)); len(jobs) != 2 {
		t.Fatalf("len %d", len(jobs))
	}
}

// getJobs with explicit waiting type
func TestGetJobsByTypeWaiting(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	for i := 0; i < 5; i++ {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, nil))
	}

	if jobs := must(queue.GetJobs(ctx, []string{"waiting"}, 0, -1, true)); len(jobs) != 5 {
		t.Fatalf("waiting len %d", len(jobs))
	}

	// Default (empty types) returns everything too.
	if all := must(queue.GetJobs(ctx, []string{}, 0, -1, true)); len(all) != 5 {
		t.Fatalf("all len %d", len(all))
	}
}

func TestGetJobsSkipsMissingHashAndPreservesWaitingOrder(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	first := must(queue.Add(ctx, "first", map[string]any{"idx": 1}, nil))
	missing := must(queue.Add(ctx, "missing", map[string]any{"idx": 2}, nil))
	third := must(queue.Add(ctx, "third", map[string]any{"idx": 3}, nil))

	noErr(t, queue.Client().Del(ctx, queue.Keys().JobKey(missing.ID)).Err())

	jobs := must(queue.GetJobs(ctx, []string{"waiting"}, 0, 1, true))
	ids := make([]string, 0, len(jobs))
	for _, j := range jobs {
		ids = append(ids, j.ID)
	}
	if want := []string{first.ID, third.ID}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("ids %v, want %v", ids, want)
	}
}

// get_ranges sanitizes types: querying `waiting` must also include `paused`
// jobs (where waiting jobs are parked while the queue is paused).
func TestGetRangesWaitingIncludesPaused(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	for i := 0; i < 3; i++ {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, nil))
	}

	// Pausing moves waiting jobs into the paused list.
	noErr(t, queue.Pause(ctx))

	ids := must(queue.GetRanges(ctx, []string{"waiting"}, 0, -1, true))
	if len(ids) != 3 {
		t.Fatalf("waiting query should include paused jobs after pause: got %d", len(ids))
	}
}

// getPrioritized
func TestGetPrioritizedJobs(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	for i := 1; i <= 3; i++ {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, &bullmq.JobOptions{Priority: i}))
	}

	if p := must(queue.GetPrioritized(ctx, 0, -1)); len(p) != 3 {
		t.Fatalf("len %d", len(p))
	}
	if c := must(queue.GetPrioritizedCount(ctx)); c != 3 {
		t.Fatalf("count %d", c)
	}
}

// getDelayed
func TestGetDelayedJobs(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	for i := 0; i < 3; i++ {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, &bullmq.JobOptions{Delay: 60 * time.Second}))
	}

	if d := must(queue.GetDelayed(ctx, 0, -1)); len(d) != 3 {
		t.Fatalf("len %d", len(d))
	}
	if c := must(queue.GetDelayedCount(ctx)); c != 3 {
		t.Fatalf("count %d", c)
	}
}

// Node.js: ".count > retries count considering prioritized jobs"
func TestCountConsideringPrioritized(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	for i := 0; i < 8; i++ {
		must(queue.Add(ctx, "test", map[string]any{"idx": i}, &bullmq.JobOptions{Priority: i + 1}))
	}
	must(queue.Add(ctx, "test", map[string]any{}, nil))

	if c := must(queue.Count(ctx)); c != 9 {
		t.Fatalf("count %d", c)
	}
}

// qtCheckCountsPerPriority asserts the 42-job priority distribution.
func qtCheckCountsPerPriority(t *testing.T, counts map[int]int64) {
	t.Helper()
	want := map[int]int64{0: 11, 1: 11, 2: 10, 3: 10}
	for p, n := range want {
		got, ok := counts[p]
		if !ok || got != n {
			t.Fatalf("priority %d: got %d (present=%v), want %d; all=%v", p, got, ok, n, counts)
		}
	}
}

// Node.js: ".getCountsPerPriority > returns job counts per priority"
func TestGetCountsPerPriority(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	for i := 0; i < 42; i++ {
		must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{Priority: i % 4}))
	}

	qtCheckCountsPerPriority(t, must(queue.GetCountsPerPriority(ctx, 0, 1, 2, 3)))
}

// Node.js: ".getCountsPerPriority > when queue is paused"
func TestGetCountsPerPriorityWhenPaused(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	noErr(t, queue.Pause(ctx))
	for i := 0; i < 42; i++ {
		must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{Priority: i % 4}))
	}

	qtCheckCountsPerPriority(t, must(queue.GetCountsPerPriority(ctx, 0, 1, 2, 3)))
}

// get_job_counts_by_types + individual count getters
func TestGetJobCountsByTypes(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	for i := 0; i < 3; i++ {
		must(queue.Add(ctx, "test", map[string]any{}, nil))
	}
	must(queue.Add(ctx, "test", map[string]any{}, &bullmq.JobOptions{Delay: 60 * time.Second}))

	if c := must(queue.GetWaitingCount(ctx)); c != 3 {
		t.Fatalf("waiting count %d", c)
	}
	if c := must(queue.GetDelayedCount(ctx)); c != 1 {
		t.Fatalf("delayed count %d", c)
	}

	m := must(queue.GetJobCountsByTypes(ctx, "waiting", "delayed"))
	if v, ok := m["waiting"]; !ok || v != 3 {
		t.Fatalf("map %v", m)
	}
	if v, ok := m["delayed"]; !ok || v != 1 {
		t.Fatalf("map %v", m)
	}

	// Total across types.
	if total := must(queue.GetJobCountByTypes(ctx, "waiting", "delayed")); total != 4 {
		t.Fatalf("total %d", total)
	}
}

// Worker-based getCompleted / getFailed
func TestGetCompletedAndFailedJobs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, &bullmq.QueueOptions{Connection: testConnection()})

	// One job that completes, one that fails.
	must(queue.Add(ctx, "ok", map[string]any{"ok": true}, nil))
	must(queue.Add(ctx, "fail", map[string]any{"ok": false}, &bullmq.JobOptions{Attempts: 1}))

	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if job.Name == "fail" {
			return nil, errors.New("boom")
		}
		return map[string]any{"done": true}, nil
	}, &bullmq.WorkerOptions{
		Connection: testConnection(),
		DrainDelay: time.Second,
	})

	// Wait for both jobs to settle.
	eventually(t, 10*time.Second, func() bool {
		return must(queue.GetCompletedCount(ctx)) >= 1 && must(queue.GetFailedCount(ctx)) >= 1
	}, "jobs did not settle")

	completed := must(queue.GetCompleted(ctx, 0, -1))
	if len(completed) != 1 || completed[0].Name != "ok" {
		t.Fatalf("completed %d", len(completed))
	}

	failed := must(queue.GetFailed(ctx, 0, -1))
	if len(failed) != 1 || failed[0].Name != "fail" {
		t.Fatalf("failed %d", len(failed))
	}

	// getJobs with multiple types returns the union.
	if both := must(queue.GetJobs(ctx, []string{"completed", "failed"}, 0, -1, false)); len(both) != 2 {
		t.Fatalf("both %d", len(both))
	}

	noErr(t, worker.Close(5*time.Second))
}

// ═══════════════════════════════════════════════════════════════════════════
// Paired getters (rate-limit ttl, global concurrency, global rate limit)
// ═══════════════════════════════════════════════════════════════════════════

func TestGetRateLimitTtl(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	// Not rate limited yet: with no maxJobs and no key, this mirrors Redis PTTL
	// semantics and returns -2 (key does not exist).
	if ttl := must(queue.GetRateLimitTTL(ctx, 0)); ttl != -2 {
		t.Fatalf("ttl %d", ttl)
	}

	// Apply a manual rate limit; TTL should now be positive.
	noErr(t, queue.RateLimit(ctx, 5*time.Second))
	ttl := must(queue.GetRateLimitTTL(ctx, 0))
	if ttl <= 0 || ttl > 5000 {
		t.Fatalf("unexpected ttl: %d", ttl)
	}
}

func TestGetGlobalConcurrency(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	check := func(wantOK bool, want int64) {
		t.Helper()
		v, ok, err := queue.GetGlobalConcurrency(ctx)
		noErr(t, err)
		if ok != wantOK || (ok && v != want) {
			t.Fatalf("global concurrency = (%d, %v), want (%d, %v)", v, ok, want, wantOK)
		}
	}

	check(false, 0)
	noErr(t, queue.SetGlobalConcurrency(ctx, 5))
	check(true, 5)
	noErr(t, queue.RemoveGlobalConcurrency(ctx))
	check(false, 0)
}

func TestGetGlobalRateLimit(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	check := func(wantOK bool, wantMax int64, wantDur time.Duration) {
		t.Helper()
		mx, d, ok, err := queue.GetGlobalRateLimit(ctx)
		noErr(t, err)
		if ok != wantOK || (ok && (mx != wantMax || d != wantDur)) {
			t.Fatalf("global rate limit = (%d, %s, %v), want (%d, %s, %v)", mx, d, ok, wantMax, wantDur, wantOK)
		}
	}

	check(false, 0, 0)
	noErr(t, queue.SetGlobalRateLimit(ctx, 100, time.Second))
	check(true, 100, time.Second)
	noErr(t, queue.RemoveGlobalRateLimit(ctx))
	check(false, 0, 0)
}
