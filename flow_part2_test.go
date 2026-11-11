package bullmq_test

// Port of bullmq_rust/tests/flow_test.rs (second part: lines 3204-6110).

import (
	"bullmq"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// f2Prefix is the key prefix used by the flow tests (Rust "bf-test").
const f2Prefix = "bf-test"

// f2Queue creates a queue with the flow-test prefix (auto cleanup).
func f2Queue(t *testing.T, name string) *bullmq.Queue {
	t.Helper()
	return newTestQueue(t, name, &bullmq.QueueOptions{Prefix: f2Prefix})
}

// f2Worker creates a worker with the flow-test prefix (auto close).
func f2Worker(t *testing.T, name string, p bullmq.Processor, manual bool) *bullmq.Worker {
	t.Helper()
	return newTestWorker(t, name, p, &bullmq.WorkerOptions{Prefix: f2Prefix, ManualStart: manual})
}

// f2FlowProducer mirrors Rust test_flow_producer (closed at cleanup).
func f2FlowProducer(t *testing.T) *bullmq.FlowProducer {
	t.Helper()
	f, err := bullmq.NewFlowProducer(context.Background(), &bullmq.FlowProducerOptions{
		Connection: testConnection(),
		Prefix:     f2Prefix,
	})
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// f2Data decodes the job data as a JSON object.
func f2Data(job *bullmq.Job) map[string]any {
	m := map[string]any{}
	_ = job.DecodeData(&m)
	return m
}

// f2Signal is a stand-in for the Rust two-party Barrier: the processor
// notifies, the test awaits with a timeout.
func f2Signal() chan struct{} { return make(chan struct{}, 16) }

func f2Notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func f2Await(t *testing.T, ch chan struct{}, d time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(d):
		t.Fatalf("%s", msg)
	}
}

func f2State(t *testing.T, q *bullmq.Queue, id string) bullmq.JobState {
	t.Helper()
	s, err := q.GetJobState(context.Background(), id)
	if err != nil {
		t.Fatalf("GetJobState(%s): %v", id, err)
	}
	return s
}

func f2WantState(t *testing.T, q *bullmq.Queue, id string, want bullmq.JobState, msg string) {
	t.Helper()
	if got := f2State(t, q, id); got != want {
		t.Fatalf("job %s state = %s, want %s %s", id, got, want, msg)
	}
}

func f2IsWrongTypeErr(msg string) bool {
	return strings.Contains(msg, "WRONGTYPE") ||
		strings.Contains(msg, "wrong kind of value") ||
		strings.Contains(msg, "only supported for sets and hashes")
}

func TestShouldMoveParentToDelayedAfterChildrenComplete(t *testing.T) {
	ctx := context.Background()
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	_ = f2Queue(t, childQueueName)
	parentQueue := f2Queue(t, parentQueueName)

	// Child worker
	childWorker := f2Worker(t, childQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		idx, _ := f2Data(job)["idx"].(float64)
		return map[string]any{"result": int(idx)}, nil
	}, false)

	// Parent worker verifies children values
	barrier := f2Signal()
	var parentOK atomic.Bool
	parentWorker := f2Worker(t, parentQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		vals, err := job.GetChildrenValues(ctx)
		if err != nil {
			t.Errorf("GetChildrenValues: %v", err)
		}
		parentOK.Store(len(vals) == 1)
		f2Notify(barrier)
		return nil, nil
	}, false)

	flow := f2FlowProducer(t)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "root-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Opts:      &bullmq.JobOptions{Delay: 3000 * time.Millisecond},
		Children: []bullmq.FlowJob{{
			Name:      "child-job",
			QueueName: childQueueName,
			Data:      map[string]any{"idx": 0, "foo": "bar"},
		}},
	}))

	// Parent state should be waiting-children initially
	f2WantState(t, parentQueue, tree.Job.ID, bullmq.StateWaitingChildren, "initially")

	// Wait for child to complete
	time.Sleep(500 * time.Millisecond)

	// After child completes, parent should be delayed (has delay opt)
	f2WantState(t, parentQueue, tree.Job.ID, bullmq.StateDelayed, "after child completes")

	// Wait for parent to be processed (after delay expires)
	f2Await(t, barrier, 15*time.Second, "timed out waiting for parent")
	if !parentOK.Load() {
		t.Fatalf("parent did not see exactly one child value")
	}

	noErr(t, childWorker.Close(5*time.Second))
	noErr(t, parentWorker.Close(5*time.Second))
}

// Node.js: ".addBulk should process jobs"
func TestShouldAddBulkAndProcessFlows(t *testing.T) {
	ctx := context.Background()
	childQueueName := testQueueName()
	rootQueueName := testQueueName()
	_ = f2Queue(t, childQueueName)
	rootQueue := f2Queue(t, rootQueueName)

	var rootProcessed atomic.Int32

	// Child worker
	childWorker := f2Worker(t, childQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		idx, _ := f2Data(job)["idx"].(float64)
		return map[string]any{"value": int(idx)}, nil
	}, true)

	// Root worker verifies children values
	barrier := f2Signal()
	rootWorker := f2Worker(t, rootQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		vals, err := job.GetChildrenValues(ctx)
		if err != nil {
			t.Errorf("GetChildrenValues: %v", err)
		}
		if len(vals) != 1 {
			t.Errorf("children values len = %d, want 1", len(vals))
		}
		count := rootProcessed.Add(1)
		if count == 2 {
			f2Notify(barrier)
		}
		return count, nil
	}, true)

	flow := f2FlowProducer(t)
	trees := must(flow.AddBulk(ctx, []bullmq.FlowJob{
		{
			Name:      "root-job-1",
			QueueName: rootQueueName,
			Data:      map[string]any{},
			Children: []bullmq.FlowJob{{
				Name:      "child-job",
				QueueName: childQueueName,
				Data:      map[string]any{"idx": 0, "foo": "bar"},
			}},
		},
		{
			Name:      "root-job-2",
			QueueName: rootQueueName,
			Data:      map[string]any{},
			Children: []bullmq.FlowJob{{
				Name:      "child-job",
				QueueName: childQueueName,
				Data:      map[string]any{"idx": 1, "foo": "baz"},
			}},
		},
	}))

	if len(trees) != 2 {
		t.Fatalf("trees len = %d", len(trees))
	}
	if trees[0].Children == nil || trees[1].Children == nil {
		t.Fatalf("trees should have children")
	}

	f2WantState(t, rootQueue, trees[0].Job.ID, bullmq.StateWaitingChildren, "")
	f2WantState(t, rootQueue, trees[1].Job.ID, bullmq.StateWaitingChildren, "")

	noErr(t, rootWorker.Run())
	noErr(t, childWorker.Run())

	// Wait for both roots to process
	f2Await(t, barrier, 10*time.Second, "timed out")

	noErr(t, childWorker.Close(5*time.Second))
	noErr(t, rootWorker.Close(5*time.Second))
}

// Node.js: "should ignore parent if a child has already failed and another one fails afterwards"
func TestShouldIgnoreParentWhenMultipleCpofChildrenFail(t *testing.T) {
	ctx := context.Background()
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	_ = f2Queue(t, childQueueName)
	_ = f2Queue(t, parentQueueName)

	cpof := &bullmq.JobOptions{ContinueParentOnFailure: true}
	flow := f2FlowProducer(t)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-ok-1", QueueName: childQueueName, Data: map[string]any{"foo": "baz"}},
			{Name: "child-fail-1", QueueName: childQueueName, Data: map[string]any{"foo": "bar"}, Opts: cpof},
			{Name: "child-fail-2", QueueName: childQueueName, Data: map[string]any{"foo": "bar"}, Opts: cpof},
			{Name: "child-ok-2", QueueName: childQueueName, Data: map[string]any{"foo": "baz"}},
			{Name: "child-ok-3", QueueName: childQueueName, Data: map[string]any{"foo": "baz"}},
		},
	}))

	childToFail1 := tree.Children[1].Job.ID
	childToFail2 := tree.Children[2].Job.ID

	// Parent worker checks failed children
	barrier := f2Signal()
	var parentCheckOK atomic.Bool
	childQualifiedName := fmt.Sprintf("%s:%s", f2Prefix, childQueueName)

	parentWorker := f2Worker(t, parentQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		failed, err := job.GetFailedChildrenValues(ctx)
		if err != nil {
			t.Errorf("GetFailedChildrenValues: %v", err)
		}
		childKey := fmt.Sprintf("%s:%s", childQualifiedName, childToFail1)
		// At least the first failing child should be tracked
		parentCheckOK.Store(failed[childKey] == "failed")
		f2Notify(barrier)
		return nil, nil
	}, false)

	// Child worker: fails the cpof children, succeeds others
	childWorker := f2Worker(t, childQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if job.ID == childToFail2 || job.ID == childToFail1 {
			return nil, errors.New("failed")
		}
		return "ok", nil
	}, false)

	f2Await(t, barrier, 10*time.Second, "timed out waiting for parent")
	if !parentCheckOK.Load() {
		t.Fatalf("parent did not see first failed child")
	}

	noErr(t, childWorker.Close(5*time.Second))
	noErr(t, parentWorker.Close(5*time.Second))
}

// f2DeepFailureCascade runs the shared body of the rdof/idof + fpof cascade
// tests and returns the parent queue and the flow tree.
func f2DeepFailureCascade(t *testing.T, childOpts *bullmq.JobOptions) (*bullmq.Queue, *bullmq.Queue, bullmq.JobNode, []*bullmq.Worker) {
	t.Helper()
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	grandchildQueueName := testQueueName()
	parentQueue := f2Queue(t, parentQueueName)
	childQueue := f2Queue(t, childQueueName)
	_ = f2Queue(t, grandchildQueueName)

	// Grandchild worker: "bar" data fails, "baz" succeeds
	var gcCounter atomic.Int32
	grandchildWorker := f2Worker(t, grandchildQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		gcCounter.Add(1)
		if f2Data(job)["foo"] == "bar" {
			time.Sleep(100 * time.Millisecond)
			return nil, errors.New("failed")
		}
		return "ok", nil
	}, false)

	// Child worker processes normally
	childWorker := f2Worker(t, childQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return "child-done", nil
	}, false)

	flow := f2FlowProducer(t)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{{
			Name:      "child-job",
			QueueName: childQueueName,
			Data:      map[string]any{"foo": "qux"},
			Opts:      childOpts,
			Children: []bullmq.FlowJob{
				{
					Name:      "grandchild-fail",
					QueueName: grandchildQueueName,
					Data:      map[string]any{"foo": "bar"},
					Opts:      &bullmq.JobOptions{FailParentOnFailure: true},
				},
				{
					Name:      "grandchild-ok",
					QueueName: grandchildQueueName,
					Data:      map[string]any{"foo": "baz"},
				},
			},
		}},
	}))
	return parentQueue, childQueue, tree, []*bullmq.Worker{grandchildWorker, childWorker}
}

// Node.js: "removeDependencyOnFailure with deep chain" - grandchild fpof fails, child rdof, parent moves to wait
func TestShouldMoveGrandparentToWaitWithRdofAndFpofCascade(t *testing.T) {
	parentQueue, childQueue, tree, workers := f2DeepFailureCascade(t, &bullmq.JobOptions{RemoveDependencyOnFailure: true})

	// Wait for cascade: grandchild fails → child fails (fpof) → parent moves to wait (rdof)
	time.Sleep(3 * time.Second)

	// Child should be failed (grandchild with fpof failed)
	f2WantState(t, childQueue, tree.Children[0].Job.ID, bullmq.StateFailed, "child should be failed due to fpof")

	// Parent (grandparent) should be waiting (rdof removes dependency on failure)
	f2WantState(t, parentQueue, tree.Job.ID, bullmq.StateWaiting, "parent should move to waiting due to rdof")

	for _, w := range workers {
		noErr(t, w.Close(5*time.Second))
	}
}

// Node.js: "ignoreDependencyOnFailure with deep chain" - grandchild fpof fails, child idof, parent moves to wait
func TestShouldMoveGrandparentToWaitWithIdofAndFpofCascade(t *testing.T) {
	ctx := context.Background()
	parentQueue, childQueue, tree, workers := f2DeepFailureCascade(t, &bullmq.JobOptions{IgnoreDependencyOnFailure: true})

	// Wait for cascade
	time.Sleep(3 * time.Second)

	// Child should be failed (grandchild with fpof failed)
	f2WantState(t, childQueue, tree.Children[0].Job.ID, bullmq.StateFailed, "child should be failed due to fpof")

	// Parent should be waiting (idof ignores the dependency failure)
	f2WantState(t, parentQueue, tree.Job.ID, bullmq.StateWaiting, "parent should move to waiting due to idof")

	// Verify ignored children failures are tracked on parent
	ignored := must(parentQueue.GetFailedChildrenValues(ctx, tree.Job.ID))
	if len(ignored) != 1 {
		t.Fatalf("should have 1 ignored failure, got %v", ignored)
	}

	for _, w := range workers {
		noErr(t, w.Close(5*time.Second))
	}
}

// Node.js: "when last child call removeChildDependency" - "moves parent to wait"
func TestShouldMoveParentToWaitWhenLastChildRemovesDependency(t *testing.T) {
	ctx := context.Background()
	queueName := testQueueName()
	queue := f2Queue(t, queueName)

	flow := f2FlowProducer(t)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent",
		QueueName: queueName,
		Data:      map[string]any{},
		Children:  []bullmq.FlowJob{{Name: "child0", QueueName: queueName, Data: map[string]any{}}},
	}))

	child := tree.Children[0]
	parentKey := child.Job.ParentKey
	if parentKey == "" {
		t.Fatalf("child has no parent key")
	}

	broken := must(queue.RemoveChildDependency(ctx, child.Job.ID, parentKey))
	if !broken {
		t.Fatalf("dependency should be broken")
	}

	f2WantState(t, queue, tree.Job.ID, bullmq.StateWaiting, "")

	// Process parent - verify no dependencies
	barrier := f2Signal()
	var depsOK atomic.Bool
	worker := f2Worker(t, queueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if job.Name == "parent" {
			c, err := job.GetDependenciesCount(ctx)
			if err != nil {
				t.Errorf("GetDependenciesCount: %v", err)
			}
			depsOK.Store(err == nil && c.Processed == 0 && c.Unprocessed == 0 && c.Ignored == 0 && c.Failed == 0)
			f2Notify(barrier)
		}
		return nil, nil
	}, false)

	f2Await(t, barrier, 10*time.Second, "timed out")
	if !depsOK.Load() {
		t.Fatalf("parent should have no dependencies")
	}

	noErr(t, worker.Close(5*time.Second))
}

// Node.js: "when there are pending children" - "keeps parent in waiting-children state"
func TestShouldKeepParentWaitingWhenRemovingOneOfManyDependencies(t *testing.T) {
	ctx := context.Background()
	queueName := testQueueName()
	queue := f2Queue(t, queueName)

	flow := f2FlowProducer(t)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent",
		QueueName: queueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child0", QueueName: queueName, Data: map[string]any{}},
			{Name: "child1", QueueName: queueName, Data: map[string]any{}},
		},
	}))

	child0 := tree.Children[0]
	parentKey := child0.Job.ParentKey
	if parentKey == "" {
		t.Fatalf("child has no parent key")
	}

	broken := must(queue.RemoveChildDependency(ctx, child0.Job.ID, parentKey))
	if !broken {
		t.Fatalf("dependency should be broken")
	}

	f2WantState(t, queue, tree.Job.ID, bullmq.StateWaitingChildren, "")

	// Process remaining child and parent
	barrier := f2Signal()
	var processedOK atomic.Bool
	worker := f2Worker(t, queueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if job.Name == "parent" {
			c, err := job.GetDependenciesCount(ctx)
			if err != nil {
				t.Errorf("GetDependenciesCount: %v", err)
			}
			processedOK.Store(err == nil && c.Unprocessed == 0 && c.Processed == 1)
			f2Notify(barrier)
		}
		return "done", nil
	}, false)

	f2Await(t, barrier, 10*time.Second, "timed out")
	if !processedOK.Load() {
		t.Fatalf("parent should have 1 processed and 0 unprocessed dependencies")
	}

	noErr(t, worker.Close(5*time.Second))
}

// Node.js: "when parent does not exist" - "throws an error"
func TestShouldErrorWhenRemovingDependencyWithMissingParent(t *testing.T) {
	ctx := context.Background()
	queueName := testQueueName()
	queue := f2Queue(t, queueName)

	flow := f2FlowProducer(t)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent",
		QueueName: queueName,
		Data:      map[string]any{},
		Children:  []bullmq.FlowJob{{Name: "child0", QueueName: queueName, Data: map[string]any{}}},
	}))

	// Remove parent job first
	must(queue.Remove(ctx, tree.Job.ID))

	child0 := tree.Children[0]
	parentKey := child0.Job.ParentKey
	if parentKey == "" {
		t.Fatalf("child has no parent key")
	}

	if _, err := queue.RemoveChildDependency(ctx, child0.Job.ID, parentKey); err == nil {
		t.Fatalf("should error when parent is missing")
	}
}

// Node.js: "when child does not exist" - "throws an error"
func TestShouldErrorWhenRemovingDependencyWithMissingChild(t *testing.T) {
	ctx := context.Background()
	queueName := testQueueName()
	queue := f2Queue(t, queueName)

	flow := f2FlowProducer(t)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent",
		QueueName: queueName,
		Data:      map[string]any{},
		Children:  []bullmq.FlowJob{{Name: "child0", QueueName: queueName, Data: map[string]any{}}},
	}))

	child0 := tree.Children[0]
	parentKey := child0.Job.ParentKey
	if parentKey == "" {
		t.Fatalf("child has no parent key")
	}

	// Remove child job first
	must(queue.Remove(ctx, child0.Job.ID))

	if _, err := queue.RemoveChildDependency(ctx, child0.Job.ID, parentKey); err == nil {
		t.Fatalf("should error when child is missing")
	}
}

// ─── Deduplication tests ───────────────────────────────────────────────────────

// Node.js: "should return deduplicated root job id when flow has no children"
func TestShouldReturnDeduplicatedRootJobIdWhenFlowHasNoChildren(t *testing.T) {
	ctx := context.Background()
	queueName := testQueueName()
	queue := f2Queue(t, queueName)

	flow := f2FlowProducer(t)
	dedupID := "dedup-root-without-children"

	add := func(order int) bullmq.JobNode {
		return must(flow.Add(ctx, bullmq.FlowJob{
			Name:      "root",
			QueueName: queueName,
			Data:      map[string]any{"order": order},
			Opts: &bullmq.JobOptions{
				Deduplication: &bullmq.DeduplicationOptions{ID: dedupID},
				Delay:         1000 * time.Millisecond,
			},
		}))
	}
	firstTree := add(1)
	secondTree := add(2)

	if secondTree.Job.ID != firstTree.Job.ID {
		t.Fatalf("second id %q != first id %q", secondTree.Job.ID, firstTree.Job.ID)
	}

	storedID, ok, err := queue.GetDeduplicationJobID(ctx, dedupID)
	noErr(t, err)
	if !ok || storedID != firstTree.Job.ID {
		t.Fatalf("stored dedup id = %q (ok=%v), want %q", storedID, ok, firstTree.Job.ID)
	}
}

// Node.js: "should deduplicate root parent job when added again with same deduplication id"
func TestShouldDeduplicateRootParentJobWithSameDedupId(t *testing.T) {
	ctx := context.Background()
	queueName := testQueueName()
	queue := f2Queue(t, queueName)

	flow := f2FlowProducer(t)
	dedupID := "dedup-parent-id"

	firstTree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent",
		QueueName: queueName,
		Data:      map[string]any{"order": 1},
		Opts: &bullmq.JobOptions{
			JobID:         "parent1",
			Deduplication: &bullmq.DeduplicationOptions{ID: dedupID},
		},
		Children: []bullmq.FlowJob{{Name: "child1", QueueName: queueName, Data: map[string]any{"value": "first"}}},
	}))

	// Add second flow with same deduplication id
	secondTree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent",
		QueueName: queueName,
		Data:      map[string]any{"order": 2},
		Opts: &bullmq.JobOptions{
			JobID:         "parent2",
			Deduplication: &bullmq.DeduplicationOptions{ID: dedupID},
		},
		Children: []bullmq.FlowJob{{Name: "child2", QueueName: queueName, Data: map[string]any{"value": "second"}}},
	}))

	if firstTree.Job.ID != "parent1" {
		t.Fatalf("first id = %q", firstTree.Job.ID)
	}
	if secondTree.Job.ID != "parent1" {
		t.Fatalf("second id = %q", secondTree.Job.ID)
	}

	// Verify only first parent exists
	if must(queue.GetJob(ctx, "parent1")) == nil {
		t.Fatalf("parent1 should exist")
	}
	if must(queue.GetJob(ctx, "parent2")) != nil {
		t.Fatalf("parent2 should not exist")
	}
}

// Node.js: "should return deduplicated id for nested flows"
func TestShouldReturnDeduplicatedIdForNestedFlows(t *testing.T) {
	ctx := context.Background()
	queueName := testQueueName()
	_ = f2Queue(t, queueName)

	flow := f2FlowProducer(t)
	dedupID := "dedup-nested-root"

	add := func(order int, suffix string) bullmq.JobNode {
		return must(flow.Add(ctx, bullmq.FlowJob{
			Name:      "parent",
			QueueName: queueName,
			Data:      map[string]any{"order": order},
			Opts:      &bullmq.JobOptions{Deduplication: &bullmq.DeduplicationOptions{ID: dedupID}},
			Children: []bullmq.FlowJob{{
				Name:      "child-" + suffix,
				QueueName: queueName,
				Data:      map[string]any{},
				Children: []bullmq.FlowJob{{
					Name:      "grandchild-" + suffix,
					QueueName: queueName,
					Data:      map[string]any{},
				}},
			}},
		}))
	}
	firstTree := add(1, "1")
	secondTree := add(2, "2")

	if secondTree.Job.ID != firstTree.Job.ID {
		t.Fatalf("second id %q != first id %q", secondTree.Job.ID, firstTree.Job.ID)
	}
}

// Node.js: "should return deduplicated ids from addBulk when roots share deduplication id"
func TestShouldReturnDeduplicatedIdsFromAddBulk(t *testing.T) {
	ctx := context.Background()
	queueName := testQueueName()
	queue := f2Queue(t, queueName)

	flow := f2FlowProducer(t)
	dedupID := "dedup-bulk-root"
	opts := func() *bullmq.JobOptions {
		return &bullmq.JobOptions{
			Deduplication: &bullmq.DeduplicationOptions{ID: dedupID},
			Delay:         1000 * time.Millisecond,
		}
	}

	trees := must(flow.AddBulk(ctx, []bullmq.FlowJob{
		{Name: "root-1", QueueName: queueName, Data: map[string]any{"idx": 1}, Opts: opts()},
		{Name: "root-2", QueueName: queueName, Data: map[string]any{"idx": 2}, Opts: opts()},
	}))

	if trees[1].Job.ID != trees[0].Job.ID {
		t.Fatalf("trees[1] id %q != trees[0] id %q", trees[1].Job.ID, trees[0].Job.ID)
	}

	storedID, ok, err := queue.GetDeduplicationJobID(ctx, dedupID)
	noErr(t, err)
	if !ok || storedID != trees[0].Job.ID {
		t.Fatalf("stored dedup id = %q (ok=%v), want %q", storedID, ok, trees[0].Job.ID)
	}
}

// Node.js: "should return same deduplicated id for concurrent add calls"
func TestShouldReturnSameDeduplicatedIdForConcurrentAddCalls(t *testing.T) {
	ctx := context.Background()
	queueName := testQueueName()
	_ = f2Queue(t, queueName)

	flow := f2FlowProducer(t)
	dedupID := "dedup-concurrent-root"
	const numberOfCalls = 8

	type result struct {
		id  string
		err error
	}
	results := make(chan result, numberOfCalls)
	var wg sync.WaitGroup
	for i := 0; i < numberOfCalls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tree, err := flow.Add(ctx, bullmq.FlowJob{
				Name:      fmt.Sprintf("root-%d", i),
				QueueName: queueName,
				Data:      map[string]any{"index": i},
				Opts: &bullmq.JobOptions{
					Deduplication: &bullmq.DeduplicationOptions{ID: dedupID},
					Delay:         1000 * time.Millisecond,
				},
			})
			if err != nil {
				results <- result{err: err}
				return
			}
			results <- result{id: tree.Job.ID}
		}(i)
	}
	wg.Wait()
	close(results)

	ids := map[string]struct{}{}
	for r := range results {
		noErr(t, r.err)
		ids[r.id] = struct{}{}
	}
	if len(ids) != 1 {
		t.Fatalf("all concurrent adds should return same dedup id, got %v", ids)
	}
}

// ─── Step jobs tests (moveToWaitingChildren) ────────────────────────────────

// Node.js: "should wait children as one step of the parent job"
func TestShouldWaitChildrenAsOneStepOfParentJob(t *testing.T) {
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f2Queue(t, parentQueueName)
	_ = f2Queue(t, childQueueName)

	flow := f2FlowProducer(t)

	// Child worker - just completes
	childWorker := f2Worker(t, childQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return "child-done", nil
	}, false)

	// Parent worker - step-based processor
	barrier := f2Signal()
	var returnValue atomic.Value
	parentWorker := f2Worker(t, parentQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		step, _ := f2Data(job)["step"].(float64)
		switch step {
		case 0:
			// Step 0: Add a child dynamically
			if _, err := flow.Add(ctx, bullmq.FlowJob{
				Name:      "child-job",
				QueueName: childQueueName,
				Data:      map[string]any{},
				Opts: &bullmq.JobOptions{Parent: &bullmq.ParentOptions{
					ID:    job.ID,
					Queue: job.QueueQualifiedName(),
				}},
				Prefix: f2Prefix,
			}); err != nil {
				t.Errorf("flow.Add child: %v", err)
				return nil, err
			}

			// Move to step 1 and wait for children
			if err := job.UpdateData(ctx, map[string]any{"step": 1}); err != nil {
				t.Errorf("UpdateData: %v", err)
				return nil, err
			}
			moved, err := job.MoveToWaitingChildren(ctx, "")
			if err != nil {
				t.Errorf("MoveToWaitingChildren: %v", err)
				return nil, err
			}
			if moved {
				return nil, bullmq.ErrWaitingChildren
			}
			// The child completed before we could move the parent
			// to waiting-children, so there are no pending
			// dependencies left to wait on — finish directly.
			returnValue.Store("finished")
			f2Notify(barrier)
			return "finished", nil
		case 1:
			// Step 1: Children are done, complete
			returnValue.Store("finished")
			f2Notify(barrier)
			return "finished", nil
		default:
			t.Errorf("unexpected step %v", step)
			return nil, errors.New("unexpected step")
		}
	}, false)

	// Add the parent job
	must(parentQueue.Add(ctx, "test", map[string]any{"step": 0}, nil))

	f2Await(t, barrier, 15*time.Second, "timed out waiting for parent to complete")
	if v, _ := returnValue.Load().(string); v != "finished" {
		t.Fatalf("return value = %q", v)
	}

	noErr(t, parentWorker.Close(5*time.Second))
	noErr(t, childWorker.Close(5*time.Second))
}

// Node.js: "should fail parent when trying to move it to waiting children"
// When parent calls moveToWaitingChildren but a child with failParentOnFailure has already failed,
// the Lua script returns -9 (JobHasFailedChildren).
func TestShouldFailParentWhenChildWithFpofFailedBeforeMoveToWaitingChildren(t *testing.T) {
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f2Queue(t, parentQueueName)
	_ = f2Queue(t, childQueueName)

	flow := f2FlowProducer(t)

	// Child worker - fails immediately
	childWorker := f2Worker(t, childQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, errors.New("child failed")
	}, false)

	// Parent worker - adds child with fpof, waits, then tries moveToWaitingChildren
	barrier := f2Signal()
	var parentFailed atomic.Bool
	parentWorker := f2Worker(t, parentQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		step, _ := f2Data(job)["step"].(float64)
		if step != 0 {
			return nil, nil
		}
		// Add child with failParentOnFailure
		if _, err := flow.Add(ctx, bullmq.FlowJob{
			Name:      "child-job",
			QueueName: childQueueName,
			Data:      map[string]any{},
			Opts: &bullmq.JobOptions{
				Parent: &bullmq.ParentOptions{
					ID:    job.ID,
					Queue: job.QueueQualifiedName(),
				},
				FailParentOnFailure: true,
			},
			Prefix: f2Prefix,
		}); err != nil {
			t.Errorf("flow.Add child: %v", err)
			return nil, err
		}

		if err := job.UpdateData(ctx, map[string]any{"step": 1}); err != nil {
			t.Errorf("UpdateData: %v", err)
			return nil, err
		}
		// Wait a bit for child to fail
		time.Sleep(500 * time.Millisecond)
		// Try to move to waiting children - should fail with -9
		_, err := job.MoveToWaitingChildren(ctx, "")
		if err != nil && bullmq.IsUnrecoverable(err) && strings.Contains(err.Error(), "at least one failed child") {
			parentFailed.Store(true)
			f2Notify(barrier)
			// Return error so worker records failure
			return nil, errors.New("job has failed children")
		}
		t.Errorf("expected JobHasFailedChildren, got: %v", err)
		return nil, fmt.Errorf("expected JobHasFailedChildren, got: %v", err)
	}, false)

	must(parentQueue.Add(ctx, "test", map[string]any{"step": 0}, nil))

	f2Await(t, barrier, 15*time.Second, "timed out")
	if !parentFailed.Load() {
		t.Fatalf("parent should have failed")
	}

	noErr(t, parentWorker.Close(5*time.Second))
	noErr(t, childWorker.Close(5*time.Second))
}

// Node.js: "should wait children as one step with grandchildren"
// Parent adds a child that has grandchildren, calls moveToWaitingChildren,
// grandchildren complete first, then child, then parent resumes.
func TestShouldWaitChildrenAsOneStepWithGrandchildren(t *testing.T) {
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	grandchildQueueName := testQueueName()
	parentQueue := f2Queue(t, parentQueueName)
	_ = f2Queue(t, childQueueName)
	_ = f2Queue(t, grandchildQueueName)

	flow := f2FlowProducer(t)

	// Grandchild worker
	grandchildWorker := f2Worker(t, grandchildQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return "gc-done", nil
	}, false)

	// Child worker
	childWorker := f2Worker(t, childQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return "child-done", nil
	}, false)

	// Parent worker - step-based
	barrier := f2Signal()
	parentWorker := f2Worker(t, parentQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		step, _ := f2Data(job)["step"].(float64)
		switch step {
		case 0:
			// Add child with grandchildren
			if _, err := flow.Add(ctx, bullmq.FlowJob{
				Name:      "child-job",
				QueueName: childQueueName,
				Data:      map[string]any{},
				Opts: &bullmq.JobOptions{Parent: &bullmq.ParentOptions{
					ID:    job.ID,
					Queue: job.QueueQualifiedName(),
				}},
				Prefix: f2Prefix,
				Children: []bullmq.FlowJob{
					{Name: "grandchild-0", QueueName: grandchildQueueName, Data: map[string]any{"idx": 0}, Prefix: f2Prefix},
					{Name: "grandchild-1", QueueName: grandchildQueueName, Data: map[string]any{"idx": 1}, Prefix: f2Prefix},
				},
			}); err != nil {
				t.Errorf("flow.Add child: %v", err)
				return nil, err
			}

			if err := job.UpdateData(ctx, map[string]any{"step": 1}); err != nil {
				t.Errorf("UpdateData: %v", err)
				return nil, err
			}
			moved, err := job.MoveToWaitingChildren(ctx, "")
			if err != nil || !moved {
				t.Errorf("MoveToWaitingChildren = %v, %v; want true, nil", moved, err)
				return nil, errors.New("not moved")
			}
			return nil, bullmq.ErrWaitingChildren
		case 1:
			// Children are done
			f2Notify(barrier)
			return "all-done", nil
		default:
			t.Errorf("unexpected step %v", step)
			return nil, errors.New("unexpected step")
		}
	}, false)

	must(parentQueue.Add(ctx, "test", map[string]any{"step": 0}, nil))

	f2Await(t, barrier, 15*time.Second, "timed out waiting for parent to complete")

	noErr(t, parentWorker.Close(5*time.Second))
	noErr(t, childWorker.Close(5*time.Second))
	noErr(t, grandchildWorker.Close(5*time.Second))
}

// ─── Error handling tests ──────────────────────────────────────────────────────

// Node.js: "when job already have a parent" - "throws an error"
func TestShouldErrorWhenJobAlreadyHasParent(t *testing.T) {
	ctx := context.Background()
	queueName := testQueueName()
	queue := f2Queue(t, queueName)

	flow := f2FlowProducer(t)

	// Create a flow: parent "tue" with child "mon"
	must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "tue",
		QueueName: queueName,
		Data:      map[string]any{},
		Opts:      &bullmq.JobOptions{JobID: "tue"},
		Children: []bullmq.FlowJob{{
			Name:      "mon",
			QueueName: queueName,
			Data:      map[string]any{},
			Opts:      &bullmq.JobOptions{JobID: "mon"},
		}},
	}))

	// Add a standalone job "wed"
	must(queue.Add(ctx, "wed", map[string]any{}, &bullmq.JobOptions{JobID: "wed"}))

	// Try to add "mon" again as a child of "wed" - should fail since "mon" already has parent
	_, err := queue.Add(ctx, "mon", map[string]any{}, &bullmq.JobOptions{
		JobID: "mon",
		Parent: &bullmq.ParentOptions{
			ID:    "wed",
			Queue: fmt.Sprintf("%s:%s", f2Prefix, queueName),
		},
	})
	if err == nil {
		t.Fatalf("should error when job already has a parent")
	}
}

// ─── getFlow tests ─────────────────────────────────────────────────────────────

// Node.js: "should get a flow tree"
func TestShouldGetAFlowTree(t *testing.T) {
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	_ = f2Queue(t, parentQueueName)
	_ = f2Queue(t, childQueueName)

	flow := f2FlowProducer(t)
	originalTree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "root-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{{
			Name:      "child-job",
			QueueName: childQueueName,
			Data:      map[string]any{"idx": 0, "foo": "bar"},
			Children: []bullmq.FlowJob{{
				Name:      "child-job",
				QueueName: childQueueName,
				Data:      map[string]any{"idx": 1, "foo": "baz"},
				Children: []bullmq.FlowJob{{
					Name:      "child-job",
					QueueName: childQueueName,
					Data:      map[string]any{"idx": 2, "foo": "qux"},
				}},
			}},
		}},
	}))

	tree := must(flow.GetFlow(ctx, bullmq.GetFlowOptions{
		ID:        originalTree.Job.ID,
		QueueName: parentQueueName,
		Prefix:    f2Prefix,
	}))

	// Verify tree structure
	if s := must(tree.Job.GetState(ctx)); s != bullmq.StateWaitingChildren {
		t.Fatalf("root state = %s", s)
	}
	if tree.Children == nil {
		t.Fatalf("root should have children")
	}
	children := tree.Children
	if len(children) != 1 {
		t.Fatalf("children len = %d", len(children))
	}

	if s := must(children[0].Job.GetState(ctx)); s != bullmq.StateWaitingChildren {
		t.Fatalf("child state = %s", s)
	}
	if children[0].Job.ID == "" {
		t.Fatalf("child id empty")
	}
	if f := f2Data(children[0].Job)["foo"]; f != "bar" {
		t.Fatalf("child foo = %v", f)
	}
	if children[0].Job.QueueName != childQueueName {
		t.Fatalf("child queue name = %q", children[0].Job.QueueName)
	}
	if children[0].Children == nil {
		t.Fatalf("child should have children")
	}
	grandchildren := children[0].Children
	if len(grandchildren) != 1 {
		t.Fatalf("grandchildren len = %d", len(grandchildren))
	}

	if grandchildren[0].Job.ID == "" {
		t.Fatalf("grandchild id empty")
	}
	if f := f2Data(grandchildren[0].Job)["foo"]; f != "baz" {
		t.Fatalf("grandchild foo = %v", f)
	}
	if grandchildren[0].Job.QueueName != childQueueName {
		t.Fatalf("grandchild queue name = %q", grandchildren[0].Job.QueueName)
	}

	if grandchildren[0].Children == nil {
		t.Fatalf("grandchild should have children")
	}
	greatGrandchildren := grandchildren[0].Children
	if len(greatGrandchildren) != 1 {
		t.Fatalf("great-grandchildren len = %d", len(greatGrandchildren))
	}
	if f := f2Data(greatGrandchildren[0].Job)["foo"]; f != "qux" {
		t.Fatalf("great-grandchild foo = %v", f)
	}
}

func TestShouldPropagateGetFlowDependencyErrors(t *testing.T) {
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f2Queue(t, parentQueueName)
	_ = f2Queue(t, childQueueName)

	flow := f2FlowProducer(t)
	originalTree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "root-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children:  []bullmq.FlowJob{{Name: "child-job", QueueName: childQueueName, Data: map[string]any{}}},
	}))

	depsKey := parentQueue.Keys().JobKey(originalTree.Job.ID) + ":dependencies"
	c := parentQueue.Client()
	noErr(t, c.Del(ctx, depsKey).Err())
	noErr(t, c.Set(ctx, depsKey, "not-a-set", 0).Err())

	_, err := flow.GetFlow(ctx, bullmq.GetFlowOptions{
		ID:        originalTree.Job.ID,
		QueueName: parentQueueName,
		Prefix:    f2Prefix,
	})
	if err == nil {
		t.Fatalf("get_flow should propagate Redis errors")
	}
	if !f2IsWrongTypeErr(err.Error()) {
		t.Fatalf("expected Redis wrong-type / pagination error, got: %v", err)
	}
}

func TestShouldIgnoreMissingChildJobsWhenLoadingFlow(t *testing.T) {
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	_ = f2Queue(t, parentQueueName)
	childQueue := f2Queue(t, childQueueName)

	flow := f2FlowProducer(t)
	originalTree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "root-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children:  []bullmq.FlowJob{{Name: "child-job", QueueName: childQueueName, Data: map[string]any{}}},
	}))

	childID := originalTree.Children[0].Job.ID
	noErr(t, childQueue.Client().Del(ctx, childQueue.Keys().JobKey(childID)).Err())

	result := must(flow.GetFlow(ctx, bullmq.GetFlowOptions{
		ID:        originalTree.Job.ID,
		QueueName: parentQueueName,
		Prefix:    f2Prefix,
	}))

	if result.Job.ID != originalTree.Job.ID {
		t.Fatalf("root id = %q, want %q", result.Job.ID, originalTree.Job.ID)
	}
	if result.Children != nil {
		t.Fatalf("children should be none, got %d", len(result.Children))
	}
}

func TestShouldPropagateChildLoadingErrorsWhenLoadingFlow(t *testing.T) {
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	_ = f2Queue(t, parentQueueName)
	childQueue := f2Queue(t, childQueueName)

	flow := f2FlowProducer(t)
	originalTree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "root-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children:  []bullmq.FlowJob{{Name: "child-job", QueueName: childQueueName, Data: map[string]any{}}},
	}))

	childID := originalTree.Children[0].Job.ID
	childDepsKey := childQueue.Keys().JobKey(childID) + ":dependencies"
	c := childQueue.Client()
	noErr(t, c.Del(ctx, childDepsKey).Err())
	noErr(t, c.Set(ctx, childDepsKey, "not-a-set", 0).Err())

	_, err := flow.GetFlow(ctx, bullmq.GetFlowOptions{
		ID:        originalTree.Job.ID,
		QueueName: parentQueueName,
		Prefix:    f2Prefix,
	})
	if err == nil {
		t.Fatalf("get_flow should propagate child-loading errors")
	}
	if !f2IsWrongTypeErr(err.Error()) {
		t.Fatalf("expected Redis wrong-type / pagination error, got: %v", err)
	}
}

// Node.js: "should get part of flow tree"
func TestShouldGetPartOfFlowTree(t *testing.T) {
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	_ = f2Queue(t, parentQueueName)
	_ = f2Queue(t, childQueueName)

	flow := f2FlowProducer(t)
	originalTree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "root-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{
				Name:      "child-job",
				QueueName: childQueueName,
				Data:      map[string]any{"idx": 0, "foo": "bar"},
				Children: []bullmq.FlowJob{{
					Name:      "child-job",
					QueueName: childQueueName,
					Data:      map[string]any{"idx": 1, "foo": "baz"},
					Children: []bullmq.FlowJob{{
						Name:      "child-job",
						QueueName: childQueueName,
						Data:      map[string]any{"idx": 2, "foo": "qux"},
					}},
				}},
			},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 3, "foo": "bax"}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 4, "foo": "baz"}},
		},
	}))

	tree := must(flow.GetFlow(ctx, bullmq.GetFlowOptions{
		ID:          originalTree.Job.ID,
		QueueName:   parentQueueName,
		Prefix:      f2Prefix,
		Depth:       2,
		MaxChildren: 2,
	}))

	// Verify partial tree
	if tree.Children == nil {
		t.Fatalf("root should have children")
	}
	children := tree.Children
	// With max_children=2, we should get at most 2 children
	if len(children) < 2 {
		t.Fatalf("children len = %d, want >= 2", len(children))
	}

	// At depth=2, children's children should NOT have their own children resolved
	for _, child := range children {
		for _, gc := range child.Children {
			if gc.Children != nil {
				t.Fatalf("depth limit should prevent deeper traversal")
			}
		}
	}
}

func TestShouldRejectColonInFlowQueueNames(t *testing.T) {
	ctx := context.Background()
	flow := f2FlowProducer(t)

	_, err := flow.Add(ctx, bullmq.FlowJob{
		Name:      "root-job",
		QueueName: "invalid:root",
		Data:      map[string]any{},
	})
	if err == nil || !strings.Contains(err.Error(), "Queue name cannot contain :") {
		t.Fatalf("got: %v", err)
	}

	_, err = flow.Add(ctx, bullmq.FlowJob{
		Name:      "root-job",
		QueueName: testQueueName(),
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{{
			Name:      "child-job",
			QueueName: "invalid:child",
			Data:      map[string]any{},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "Queue name cannot contain :") {
		t.Fatalf("got: %v", err)
	}

	_, err = flow.GetFlow(ctx, bullmq.GetFlowOptions{
		ID:        "1",
		QueueName: "invalid:root",
		Prefix:    f2Prefix,
	})
	if err == nil || !strings.Contains(err.Error(), "Queue name cannot contain :") {
		t.Fatalf("got: %v", err)
	}
}

func TestFlowAddRejectsInvalidCustomJobIds(t *testing.T) {
	ctx := context.Background()
	flow := f2FlowProducer(t)

	_, err := flow.Add(ctx, bullmq.FlowJob{
		Name:      "root-job",
		QueueName: testQueueName(),
		Data:      map[string]any{},
		Opts:      &bullmq.JobOptions{JobID: "100"},
	})
	if err == nil || !strings.Contains(err.Error(), "Custom Id cannot be integers") {
		t.Fatalf("got: %v", err)
	}

	_, err = flow.Add(ctx, bullmq.FlowJob{
		Name:      "root-job",
		QueueName: testQueueName(),
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{{
			Name:      "child-job",
			QueueName: testQueueName(),
			Data:      map[string]any{},
			Opts:      &bullmq.JobOptions{JobID: "42"},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "Custom Id cannot be integers") {
		t.Fatalf("got: %v", err)
	}
}

func TestGetFlowReturnsChildrenWithLegacyRepeatJobIds(t *testing.T) {
	// Legacy repeatable ids (`repeat:<schedulerId>:<millis>`) are the one custom
	// id shape allowed to contain `:`. Prefixes and queue names never do, so the
	// child key `{prefix}:{queue}:repeat:sched:1` must be split from the left or
	// the child resolves to the wrong queue and is silently dropped.
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	_ = f2Queue(t, parentQueueName)
	_ = f2Queue(t, childQueueName)

	childJobID := "repeat:scheduler-id:1700000000000"
	flow := f2FlowProducer(t)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "root-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{{
			Name:      "child-job",
			QueueName: childQueueName,
			Data:      map[string]any{"idx": 0},
			Opts:      &bullmq.JobOptions{JobID: childJobID},
		}},
	}))

	fetched := must(flow.GetFlow(ctx, bullmq.GetFlowOptions{
		ID:        tree.Job.ID,
		QueueName: parentQueueName,
		Prefix:    f2Prefix,
	}))

	children := fetched.Children
	if children == nil {
		t.Fatalf("child with legacy repeat id should be returned")
	}
	if len(children) != 1 {
		t.Fatalf("children len = %d", len(children))
	}
	if children[0].Job.ID != childJobID {
		t.Fatalf("child id = %q", children[0].Job.ID)
	}
	if children[0].Job.Name != "child-job" {
		t.Fatalf("child name = %q", children[0].Job.Name)
	}
	if d := f2Data(children[0].Job); !reflect.DeepEqual(d, map[string]any{"idx": float64(0)}) {
		t.Fatalf("child data = %v", d)
	}
}

// ─── Remove tests ──────────────────────────────────────────────────────────────

// Node.js: "should remove all children when removing a parent"
func TestShouldRemoveAllChildrenWhenRemovingParent(t *testing.T) {
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f2Queue(t, parentQueueName)
	childQueue := f2Queue(t, childQueueName)

	flow := f2FlowProducer(t)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"}},
			{
				Name:      "child-job",
				QueueName: childQueueName,
				Data:      map[string]any{"idx": 1, "foo": "baz"},
				Children: []bullmq.FlowJob{{
					Name:      "child-job",
					QueueName: childQueueName,
					Data:      map[string]any{"idx": 2, "foo": "qux"},
				}},
			},
		},
	}))

	// Verify initial states
	f2WantState(t, parentQueue, tree.Job.ID, bullmq.StateWaitingChildren, "")

	child0 := tree.Children[0]
	child1 := tree.Children[1]
	f2WantState(t, childQueue, child0.Job.ID, bullmq.StateWaiting, "")
	f2WantState(t, childQueue, child1.Job.ID, bullmq.StateWaitingChildren, "")

	// Remove parent (cascades to children)
	must(parentQueue.Remove(ctx, tree.Job.ID))

	// All jobs should be gone
	if must(parentQueue.GetJob(ctx, tree.Job.ID)) != nil {
		t.Fatalf("parent should be removed")
	}
	if must(childQueue.GetJob(ctx, child0.Job.ID)) != nil {
		t.Fatalf("child0 should be removed")
	}
	if must(childQueue.GetJob(ctx, child1.Job.ID)) != nil {
		t.Fatalf("child1 should be removed")
	}
	grandchild := child1.Children[0]
	if must(childQueue.GetJob(ctx, grandchild.Job.ID)) != nil {
		t.Fatalf("grandchild should be removed")
	}
}

// Node.js: "should remove from parent dependencies and move parent to wait"
func TestShouldRemoveChildAndMoveParentToWait(t *testing.T) {
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f2Queue(t, parentQueueName)
	childQueue := f2Queue(t, childQueueName)

	flow := f2FlowProducer(t)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "root-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{{
			Name:      "child-job",
			QueueName: childQueueName,
			Data:      map[string]any{"foo": "bar"},
			Children: []bullmq.FlowJob{{
				Name:      "child-job",
				QueueName: childQueueName,
				Data:      map[string]any{"foo": "baz"},
				Children: []bullmq.FlowJob{{
					Name:      "child-job",
					QueueName: childQueueName,
					Data:      map[string]any{"foo": "qux"},
				}},
			}},
		}},
	}))

	child := tree.Children[0]
	grandchild := child.Children[0]
	greatGrandchild := grandchild.Children[0]

	// Remove deepest child - grandchild should move to waiting
	f2WantState(t, childQueue, grandchild.Job.ID, bullmq.StateWaitingChildren, "")
	must(childQueue.Remove(ctx, greatGrandchild.Job.ID))
	f2WantState(t, childQueue, grandchild.Job.ID, bullmq.StateWaiting, "")

	// Remove grandchild - child should move to waiting
	f2WantState(t, childQueue, child.Job.ID, bullmq.StateWaitingChildren, "")
	must(childQueue.Remove(ctx, grandchild.Job.ID))
	f2WantState(t, childQueue, child.Job.ID, bullmq.StateWaiting, "")

	// Remove child - parent should move to waiting
	f2WantState(t, parentQueue, tree.Job.ID, bullmq.StateWaitingChildren, "")
	must(childQueue.Remove(ctx, child.Job.ID))
	f2WantState(t, parentQueue, tree.Job.ID, bullmq.StateWaiting, "")
}

// Node.js: "should only move parent to wait when all children have been removed"
func TestShouldOnlyMoveParentToWaitWhenAllChildrenRemoved(t *testing.T) {
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f2Queue(t, parentQueueName)
	childQueue := f2Queue(t, childQueueName)

	flow := f2FlowProducer(t)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"foo": "bar"}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"foo": "baz"}},
		},
	}))

	f2WantState(t, parentQueue, tree.Job.ID, bullmq.StateWaitingChildren, "")

	children := tree.Children

	// Remove first child - parent should still be waiting-children
	must(childQueue.Remove(ctx, children[0].Job.ID))
	f2WantState(t, childQueue, children[0].Job.ID, bullmq.StateUnknown, "")
	f2WantState(t, parentQueue, tree.Job.ID, bullmq.StateWaitingChildren, "")

	// Remove second child - parent should now move to waiting
	must(childQueue.Remove(ctx, children[1].Job.ID))
	f2WantState(t, childQueue, children[1].Job.ID, bullmq.StateUnknown, "")
	f2WantState(t, parentQueue, tree.Job.ID, bullmq.StateWaiting, "")
}

// Node.js: "does not remove any children when removing a parent" (removeChildren: false)
func TestShouldNotRemoveChildrenWhenRemoveWithoutChildren(t *testing.T) {
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f2Queue(t, parentQueueName)
	childQueue := f2Queue(t, childQueueName)

	flow := f2FlowProducer(t)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"foo": "bar"}},
			{
				Name:      "child-job",
				QueueName: childQueueName,
				Data:      map[string]any{"foo": "baz"},
				Children: []bullmq.FlowJob{{
					Name:      "child-job",
					QueueName: childQueueName,
					Data:      map[string]any{"foo": "qux"},
				}},
			},
		},
	}))

	// Remove parent without removing children
	must(parentQueue.RemoveWithoutChildren(ctx, tree.Job.ID))

	// Parent should be gone
	if must(parentQueue.GetJob(ctx, tree.Job.ID)) != nil {
		t.Fatalf("parent should be removed")
	}

	// Children should still exist
	children := tree.Children
	if must(childQueue.GetJob(ctx, children[0].Job.ID)) == nil {
		t.Fatalf("child0 should still exist")
	}
	if must(childQueue.GetJob(ctx, children[1].Job.ID)) == nil {
		t.Fatalf("child1 should still exist")
	}

	// Verify state - children remain in their respective states
	f2WantState(t, childQueue, children[0].Job.ID, bullmq.StateWaiting, "")
	f2WantState(t, childQueue, children[1].Job.ID, bullmq.StateWaitingChildren, "")

	// Parent state should be unknown (removed)
	f2WantState(t, parentQueue, tree.Job.ID, bullmq.StateUnknown, "")
}

// Node.js: "should remove unprocessed children"
func TestShouldRemoveUnprocessedChildren(t *testing.T) {
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f2Queue(t, parentQueueName)
	childQueue := f2Queue(t, childQueueName)

	flow := f2FlowProducer(t)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 1, "foo": "bar"}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 2, "foo": "bar"}},
			{
				Name:      "child-job",
				QueueName: childQueueName,
				Data:      map[string]any{"idx": 3, "foo": "baz"},
				Children: []bullmq.FlowJob{{
					Name:      "child-job",
					QueueName: childQueueName,
					Data:      map[string]any{"idx": 4, "foo": "qux"},
				}},
			},
		},
	}))

	// All children except the one with sub-children should be in waiting
	children := tree.Children
	f2WantState(t, childQueue, children[0].Job.ID, bullmq.StateWaiting, "")
	f2WantState(t, childQueue, children[3].Job.ID, bullmq.StateWaitingChildren, "")

	// Remove unprocessed children of parent
	noErr(t, parentQueue.RemoveUnprocessedChildren(ctx, tree.Job.ID))

	// All children should now be removed
	for _, child := range children {
		if must(childQueue.GetJob(ctx, child.Job.ID)) != nil {
			t.Fatalf("child %s should be removed", child.Job.ID)
		}
	}

	// Grandchild should also be removed
	grandchild := children[3].Children
	if must(childQueue.GetJob(ctx, grandchild[0].Job.ID)) != nil {
		t.Fatalf("grandchild should be removed")
	}

	// Parent should still exist; with all children removed it moves to waiting
	f2WantState(t, parentQueue, tree.Job.ID, bullmq.StateWaiting, "")
}

// Node.js: "should not remove completed children"
func TestShouldNotRemoveCompletedChildrenWithRemoveUnprocessed(t *testing.T) {
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f2Queue(t, parentQueueName)
	childQueue := f2Queue(t, childQueueName)

	flow := f2FlowProducer(t)
	const numChildren = 6
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 1, "foo": "bar"}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 2, "foo": "bar"}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 3, "foo": "bar"}},
			{
				Name:      "child-job",
				QueueName: childQueueName,
				Data:      map[string]any{"idx": 4, "foo": "baz"},
				Children: []bullmq.FlowJob{{
					Name:      "child-job",
					QueueName: childQueueName,
					Data:      map[string]any{"idx": 5, "foo": "qux"},
				}},
			},
		},
	}))

	// Process all children with a worker
	parentWorker := f2Worker(t, parentQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	}, false)
	childWorker := f2Worker(t, childQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		time.Sleep(10 * time.Millisecond)
		return nil, nil
	}, false)

	// Wait for parent to complete (meaning all children completed)
	time.Sleep(2000 * time.Millisecond)

	counts := must(childQueue.GetJobCounts(ctx))
	if counts.Completed != numChildren {
		t.Fatalf("all children should be completed: %d", counts.Completed)
	}

	// Try to remove unprocessed children - should not remove any since all are completed
	noErr(t, parentQueue.RemoveUnprocessedChildren(ctx, tree.Job.ID))

	countsAfter := must(childQueue.GetJobCounts(ctx))
	if countsAfter.Completed != numChildren {
		t.Fatalf("all children should still be completed: %d", countsAfter.Completed)
	}

	_ = childWorker.Close(1 * time.Second)
	_ = parentWorker.Close(1 * time.Second)
}

// Node.js: "removes all children when removing a parent" (after processing)
func TestShouldRemoveAllChildrenAfterProcessing(t *testing.T) {
	ctx := context.Background()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f2Queue(t, parentQueueName)
	childQueue := f2Queue(t, childQueueName)

	flow := f2FlowProducer(t)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"}},
			{
				Name:      "child-job",
				QueueName: childQueueName,
				Data:      map[string]any{"idx": 1, "foo": "baz"},
				Children: []bullmq.FlowJob{{
					Name:      "child-job",
					QueueName: childQueueName,
					Data:      map[string]any{"idx": 2, "foo": "qux"},
				}},
			},
		},
	}))

	// Process all jobs
	parentWorker := f2Worker(t, parentQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	}, false)
	childWorker := f2Worker(t, childQueueName, func(ctx context.Context, job *bullmq.Job) (any, error) {
		time.Sleep(10 * time.Millisecond)
		return nil, nil
	}, false)

	// Wait for all processing
	time.Sleep(2000 * time.Millisecond)

	// Remove the parent with children
	must(parentQueue.Remove(ctx, tree.Job.ID))

	// Parent should be gone
	if must(parentQueue.GetJob(ctx, tree.Job.ID)) != nil {
		t.Fatalf("parent should be removed")
	}

	// All children should be removed too
	children := tree.Children
	for _, child := range children {
		if must(childQueue.GetJob(ctx, child.Job.ID)) != nil {
			t.Fatalf("child %s should be removed", child.Job.ID)
		}
	}

	// Grandchild too
	grandchild := children[1].Children
	if must(childQueue.GetJob(ctx, grandchild[0].Job.ID)) != nil {
		t.Fatalf("grandchild should be removed")
	}

	counts := must(childQueue.GetJobCounts(ctx))
	if counts.Completed != 0 {
		t.Fatalf("completed = %d, want 0", counts.Completed)
	}

	_ = childWorker.Close(1 * time.Second)
	_ = parentWorker.Close(1 * time.Second)
}
