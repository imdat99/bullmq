package bullmq_test

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/imdat99/bullmq"
)

// f3Prefix is the key prefix used by the flow tests (Rust "bf-test").
const f3Prefix = "bf-test"

// f3Queue creates a queue with prefix, obliterated and closed at cleanup
// (Rust test_queue_with_prefix).
func f3Queue(t *testing.T, name, prefix string) *bullmq.Queue {
	t.Helper()
	return newTestQueue(t, name, &bullmq.QueueOptions{Prefix: prefix})
}

// f3Flow creates a flow producer with prefix, closed at cleanup (Rust
// test_flow_producer).
func f3Flow(t *testing.T, prefix string) *bullmq.FlowProducer {
	t.Helper()
	f, err := bullmq.NewFlowProducer(context.Background(), &bullmq.FlowProducerOptions{
		Connection: testConnection(),
		Prefix:     prefix,
	})
	if err != nil {
		t.Fatalf("NewFlowProducer: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

// f3Worker creates a worker on the prefixed queue, closed at cleanup. opts
// may be nil; its Prefix is overwritten with prefix.
func f3Worker(t *testing.T, name, prefix string, p bullmq.Processor, opts *bullmq.WorkerOptions) *bullmq.Worker {
	t.Helper()
	if opts == nil {
		opts = &bullmq.WorkerOptions{}
	}
	opts.Prefix = prefix
	return newTestWorker(t, name, p, opts)
}

// f3Sleep sleeps d or until ctx is cancelled.
func f3Sleep(ctx context.Context, d time.Duration) {
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-tm.C:
	case <-ctx.Done():
	}
}

// f3WaitState polls the state of job id every interval until it equals want
// or timeout elapses (Rust timeout(.., loop { get_job_state .. })).
func f3WaitState(t *testing.T, q *bullmq.Queue, id string, want bullmq.JobState, timeout, interval time.Duration, msg string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for {
		st, err := q.GetJobState(ctx, id)
		noErr(t, err)
		if st == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s (state %q, want %q)", msg, st, want)
		}
		time.Sleep(interval)
	}
}

// f3WaitGone polls until job id no longer exists or timeout elapses.
func f3WaitGone(t *testing.T, q *bullmq.Queue, id string, timeout time.Duration, msg string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for {
		j, err := q.GetJob(ctx, id)
		noErr(t, err)
		if j == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s", msg)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// f3State returns the state of job id, failing the test on error.
func f3State(t *testing.T, q *bullmq.Queue, id string) bullmq.JobState {
	t.Helper()
	st, err := q.GetJobState(context.Background(), id)
	noErr(t, err)
	return st
}

// f3Data decodes the job data as a JSON object.
func f3Data(t *testing.T, j *bullmq.Job) map[string]any {
	t.Helper()
	var m map[string]any
	if err := j.DecodeData(&m); err != nil {
		t.Fatalf("decode data: %v", err)
	}
	return m
}

// f3Idx returns data["idx"] as an int (0 when absent), like Rust
// job.data()["idx"].as_u64().unwrap_or(0).
func f3Idx(j *bullmq.Job) int {
	var m map[string]any
	if err := j.DecodeData(&m); err != nil {
		return 0
	}
	if f, ok := m["idx"].(float64); ok {
		return int(f)
	}
	return 0
}

// f3Signal performs a processor-side
// non-blocking send on ch (the Rust Barrier(2) handshake).
func f3Signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// f3WaitSignal waits for a signal on ch for at most timeout.
func f3WaitSignal(t *testing.T, ch chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("%s", msg)
	}
}

func f3Null(ctx context.Context, job *bullmq.Job) (any, error) { return nil, nil }

// Node.js: "removes all children when removing a parent, but not grandparent"
func TestShouldRemoveChildrenButNotGrandparent(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	grandparentQueueName := testQueueName()
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	grandparentQueue := f3Queue(t, grandparentQueueName, prefix)
	parentQueue := f3Queue(t, parentQueueName, prefix)
	childQueue := f3Queue(t, childQueueName, prefix)

	flow := f3Flow(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "grandparent-job",
		QueueName: grandparentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{{
			Name:      "parent-job",
			QueueName: parentQueueName,
			Data:      map[string]any{},
			Children: []bullmq.FlowJob{
				{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"}},
				{
					Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 1, "foo": "baz"},
					Children: []bullmq.FlowJob{
						{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 2, "foo": "qux"}},
					},
				},
			},
		}},
	}))

	// Process children
	parentWorker := f3Worker(t, parentQueueName, prefix, f3Null, nil)
	childWorker := f3Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		f3Sleep(ctx, 10*time.Millisecond)
		return nil, nil
	}, nil)

	// Wait for parent to complete (children all done)
	time.Sleep(2000 * time.Millisecond)

	// Remove the parent (not grandparent)
	parentNode := tree.Children
	must(parentQueue.Remove(ctx, parentNode[0].Job.ID))

	// Parent should be gone
	if j := must(parentQueue.GetJob(ctx, parentNode[0].Job.ID)); j != nil {
		t.Fatalf("parent should be removed")
	}

	// All children should be removed
	for _, child := range parentNode[0].Children {
		if j := must(childQueue.GetJob(ctx, child.Job.ID)); j != nil {
			t.Fatalf("child %s should be removed", child.Job.ID)
		}
	}

	counts := must(childQueue.GetJobCounts(ctx))
	if counts.Completed != 0 {
		t.Fatalf("completed = %d, want 0", counts.Completed)
	}

	// Grandparent should still exist and be in waiting state (since its child was removed)
	if st := f3State(t, grandparentQueue, tree.Job.ID); st != bullmq.StateWaiting {
		t.Fatalf("grandparent state = %s, want waiting", st)
	}

	_ = flow.Close()
	_ = childWorker.Close(time.Second)
	_ = parentWorker.Close(time.Second)
}

// Node.js: "should not remove anything if there is a locked job in the tree"
func TestShouldNotRemoveLockedJobInTree(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f3Queue(t, parentQueueName, prefix)
	childQueue := f3Queue(t, childQueueName, prefix)

	flow := f3Flow(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 1, "foo": "baz"}},
		},
	}))

	// Start a worker that will hold a lock on a job (process slowly)
	picked := make(chan struct{}, 1)
	childWorker := f3Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		// Signal that we picked up a job
		f3Signal(picked)
		// Hold the lock by sleeping
		f3Sleep(ctx, 5000*time.Millisecond)
		return nil, nil
	}, nil)

	// Wait for worker to pick up a job (lock it)
	f3WaitSignal(t, picked, 30*time.Second, "worker did not pick up a job")
	time.Sleep(100 * time.Millisecond)

	// Try to remove parent - returns false because a child is locked
	// (a locked job/dependency is a normal "not removed" outcome, not an error).
	removed := must(parentQueue.Remove(ctx, tree.Job.ID))
	if removed {
		t.Fatalf("remove should return false when child is locked")
	}

	// All jobs should still exist
	if st := f3State(t, parentQueue, tree.Job.ID); st != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s, want waiting-children", st)
	}

	children := tree.Children
	child0State := f3State(t, childQueue, children[0].Job.ID)
	// One should be active (locked)
	child1State := f3State(t, childQueue, children[1].Job.ID)
	if child0State != bullmq.StateActive && child1State != bullmq.StateActive {
		t.Fatalf("at least one child should be active (%s, %s)", child0State, child1State)
	}

	_ = flow.Close()
	_ = childWorker.Close(time.Second)
}

// Node.js: "should remove from parent dependencies and move parent to wait"
func TestShouldRemoveFromParentDepsAndMoveToWait(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f3Queue(t, parentQueueName, prefix)
	childQueue := f3Queue(t, childQueueName, prefix)

	flow := f3Flow(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "root-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{{
			Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"},
			Children: []bullmq.FlowJob{{
				Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 1, "foo": "baz"},
				Children: []bullmq.FlowJob{{
					Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 2, "foo": "qux"},
				}},
			}},
		}},
	}))

	// Remove from deepest child upwards:
	// tree -> children[0] -> children[0] -> children[0] (leaf)
	childL1 := tree.Children[0]
	childL2 := childL1.Children[0]
	childL3 := childL2.Children[0]

	// l2 is waiting-children, remove l3 should move l2 to waiting
	if st := f3State(t, childQueue, childL2.Job.ID); st != bullmq.StateWaitingChildren {
		t.Fatalf("l2 state = %s", st)
	}
	must(childQueue.Remove(ctx, childL3.Job.ID))
	if st := f3State(t, childQueue, childL2.Job.ID); st != bullmq.StateWaiting {
		t.Fatalf("l2 state after = %s", st)
	}

	// l1 is waiting-children, remove l2 should move l1 to waiting
	if st := f3State(t, childQueue, childL1.Job.ID); st != bullmq.StateWaitingChildren {
		t.Fatalf("l1 state = %s", st)
	}
	must(childQueue.Remove(ctx, childL2.Job.ID))
	if st := f3State(t, childQueue, childL1.Job.ID); st != bullmq.StateWaiting {
		t.Fatalf("l1 state after = %s", st)
	}

	// root is waiting-children, remove l1 should move root to waiting
	if st := f3State(t, parentQueue, tree.Job.ID); st != bullmq.StateWaitingChildren {
		t.Fatalf("root state = %s", st)
	}
	must(childQueue.Remove(ctx, childL1.Job.ID))
	if st := f3State(t, parentQueue, tree.Job.ID); st != bullmq.StateWaiting {
		t.Fatalf("root state after = %s", st)
	}

	_ = flow.Close()
}

// Node.js: "when retrying a failed child > should update parent dependencies reference"
func TestShouldUpdateParentDepsWhenRetryingFailedChild(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	_ = f3Queue(t, parentQueueName, prefix)
	childQueue := f3Queue(t, childQueueName, prefix)

	// Parent worker signals completion via barrier
	barrier := make(chan struct{}, 1)
	parentWorker := f3Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		f3Signal(barrier)
		return nil, nil
	}, nil)

	// Child worker: on idx 0, update data to idx 1 and fail; on idx 1 succeed
	childWorker := f3Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		f3Sleep(ctx, 10*time.Millisecond)
		if f3Idx(job) == 0 {
			if err := job.UpdateData(ctx, map[string]any{"idx": 1, "foo": "baz"}); err != nil {
				return nil, err
			}
			return nil, fmt.Errorf("error")
		}
		return nil, nil
	}, nil)

	flow := f3Flow(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"}},
		},
	}))

	childID := tree.Children[0].Job.ID

	// Wait for child to fail
	f3WaitState(t, childQueue, childID, bullmq.StateFailed, 10*time.Second, 50*time.Millisecond, "timed out waiting for child to fail")

	// Retry the failed child
	failedChild := must(childQueue.GetJob(ctx, childID))
	noErr(t, failedChild.Retry(ctx, bullmq.StateFailed, nil))

	// Wait for parent to be processed
	f3WaitSignal(t, barrier, 10*time.Second, "timed out waiting for parent to process")

	// One child should be completed
	counts := must(childQueue.GetJobCounts(ctx))
	if counts.Completed != 1 {
		t.Fatalf("completed = %d, want 1", counts.Completed)
	}

	_ = flow.Close()
	noErr(t, childWorker.Close(5*time.Second))
	noErr(t, parentWorker.Close(5*time.Second))
}

// Node.js: "when retrying a completed child > should update parent dependencies reference"
func TestShouldUpdateParentDepsWhenRetryingCompletedChild(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	_ = f3Queue(t, parentQueueName, prefix)
	childQueue := f3Queue(t, childQueueName, prefix)

	// Parent worker signals completion via barrier
	barrier := make(chan struct{}, 1)
	parentWorker := f3Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		f3Signal(barrier)
		return nil, nil
	}, nil)

	// Child worker: updates data to idx+2 and completes
	childWorker := f3Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		idx := f3Idx(job)
		if err := job.UpdateData(ctx, map[string]any{"idx": idx + 2}); err != nil {
			return nil, err
		}
		f3Sleep(ctx, 200*time.Millisecond)
		return nil, nil
	}, nil)

	flow := f3Flow(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 1}},
		},
	}))

	child0ID := tree.Children[0].Job.ID

	// Wait for first child to complete (idx -> idx+2)
	f3WaitState(t, childQueue, child0ID, bullmq.StateCompleted, 10*time.Second, 20*time.Millisecond, "timed out waiting for child to complete")

	// Retry the completed child before the parent finishes
	completedChild := must(childQueue.GetJob(ctx, child0ID))
	noErr(t, completedChild.Retry(ctx, bullmq.StateCompleted, nil))

	// Wait for parent to be processed
	f3WaitSignal(t, barrier, 15*time.Second, "timed out waiting for parent to process")

	// Both children should be completed
	counts := must(childQueue.GetJobCounts(ctx))
	if counts.Completed != 2 {
		t.Fatalf("completed = %d, want 2", counts.Completed)
	}

	_ = flow.Close()
	noErr(t, childWorker.Close(5*time.Second))
	noErr(t, parentWorker.Close(5*time.Second))
}

// add_bulk surfaces partial failures as an error (idiomatic Rust: a negative
// status code anywhere in the bulk is reported rather than silently returning
// a partially added result).
func TestShouldReturnErrorWhenSomeAddBulkCommandsFail(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	queueName := testQueueName()
	_ = f3Queue(t, queueName, prefix)

	flow := f3Flow(t, prefix)
	queueKey := fmt.Sprintf("%s:%s", prefix, queueName)

	_, err := flow.AddBulk(ctx, []bullmq.FlowJob{
		{
			Name:      "valid-root",
			QueueName: queueName,
			Data:      map[string]any{},
			Opts: &bullmq.JobOptions{
				Deduplication: &bullmq.DeduplicationOptions{ID: "dedup-valid-on-partial-failure"},
				Delay:         1000 * time.Millisecond,
			},
		},
		{
			Name:      "invalid-root",
			QueueName: queueName,
			Data:      map[string]any{},
			Opts: &bullmq.JobOptions{
				Parent: &bullmq.ParentOptions{ID: "missing-parent", Queue: queueKey},
			},
		},
	})

	if err == nil {
		t.Fatalf("add_bulk should error when any node fails to add")
	}
	if !strings.Contains(err.Error(), "Missing key for parent job") {
		t.Fatalf("error should mention the missing parent key, got: %v", err)
	}

	_ = flow.Close()
}

// Node.js: "throws an error instead of silently dropping the job" (non-existing parent)
func TestShouldThrowErrorWhenAddWithNonExistingParent(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	queueName := testQueueName()
	queue := f3Queue(t, queueName, prefix)

	flow := f3Flow(t, prefix)
	missingParentID := "missing-parent-" + uuid.NewString()
	queueKey := fmt.Sprintf("%s:%s", prefix, queueName)
	parentKey := fmt.Sprintf("%s:%s", queueKey, missingParentID)

	_, err := flow.Add(ctx, bullmq.FlowJob{
		Name:      "orphan-child",
		QueueName: queueName,
		Data:      map[string]any{"foo": "bar"},
		Opts: &bullmq.JobOptions{
			Parent: &bullmq.ParentOptions{ID: missingParentID, Queue: queueKey},
			JobID:  "orphan-child-id",
		},
	})

	if err == nil {
		t.Fatalf("add should fail with missing parent")
	}
	if !strings.Contains(err.Error(), "Missing key for parent job "+parentKey) {
		t.Fatalf("error message should mention missing parent key, got: %v", err)
	}

	// The job should NOT have been added to Redis.
	if j := must(queue.GetJob(ctx, "orphan-child-id")); j != nil {
		t.Fatalf("orphan job should not exist")
	}

	_ = flow.Close()
}

// Node.js: "should get a flow tree using default prefix from FlowProducer"
func TestShouldGetAFlowTreeUsingDefaultPrefix(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f3Queue(t, parentQueueName, prefix)
	_ = f3Queue(t, childQueueName, prefix)

	flow := f3Flow(t, prefix)
	originalTree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "root-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{{
			Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"},
			Children: []bullmq.FlowJob{{
				Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 1, "foo": "baz"},
				Children: []bullmq.FlowJob{{
					Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 2, "foo": "qux"},
				}},
			}},
		}},
	}))

	// Call GetFlow WITHOUT specifying a prefix - should fall back to producer default
	tree := must(flow.GetFlow(ctx, bullmq.GetFlowOptions{
		ID:        originalTree.Job.ID,
		QueueName: parentQueueName,
	}))

	if st := f3State(t, parentQueue, tree.Job.ID); st != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s", st)
	}

	if tree.Children == nil {
		t.Fatalf("children should be set")
	}
	children := tree.Children
	if len(children) != 1 {
		t.Fatalf("children len = %d", len(children))
	}

	if children[0].Job.ID == "" {
		t.Fatalf("empty child id")
	}
	if foo := f3Data(t, children[0].Job)["foo"]; foo != "bar" {
		t.Fatalf("child foo = %v", foo)
	}
	if children[0].Job.QueueName != childQueueName {
		t.Fatalf("child queue name = %q", children[0].Job.QueueName)
	}
	if children[0].Children == nil {
		t.Fatalf("grandchildren should be set")
	}

	grandchildren := children[0].Children
	if len(grandchildren) != 1 {
		t.Fatalf("grandchildren len = %d", len(grandchildren))
	}
	if grandchildren[0].Job.ID == "" {
		t.Fatalf("empty grandchild id")
	}
	if grandchildren[0].Job.QueueName != childQueueName {
		t.Fatalf("grandchild queue name = %q", grandchildren[0].Job.QueueName)
	}
	if foo := f3Data(t, grandchildren[0].Job)["foo"]; foo != "baz" {
		t.Fatalf("grandchild foo = %v", foo)
	}

	great := grandchildren[0].Children
	if len(great) != 1 {
		t.Fatalf("great len = %d", len(great))
	}
	if great[0].Job.ID == "" {
		t.Fatalf("empty great id")
	}
	if foo := f3Data(t, great[0].Job)["foo"]; foo != "qux" {
		t.Fatalf("great foo = %v", foo)
	}

	_ = flow.Close()
}

// Node.js: "when parent has removeOnComplete as true > removes processed data"
func TestShouldRemoveProcessedDataWhenParentRemoveOnComplete(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f3Queue(t, parentQueueName, prefix)
	_ = f3Queue(t, childQueueName, prefix)

	// Child worker returns a value based on idx
	childWorker := f3Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return map[string]any{"value": f3Idx(job)}, nil
	}, nil)

	flow := f3Flow(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Opts:      &bullmq.JobOptions{RemoveOnComplete: bullmq.RemoveAll()},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 1, "foo": "baz"}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 2, "foo": "qux"}},
		},
	}))

	parentID := tree.Job.ID

	// Wait for all children to complete (parent moves to waiting)
	f3WaitState(t, parentQueue, parentID, bullmq.StateWaiting, 10*time.Second, 50*time.Millisecond, "timed out waiting for parent to be ready")

	// Fetch the parent job and inspect its processed dependencies
	parentJob := must(parentQueue.GetJob(ctx, parentID))
	deps := must(parentJob.GetDependencies(ctx, 0, 0, 100))
	if deps.NextProcessedCursor != 0 {
		t.Fatalf("next processed cursor = %d", deps.NextProcessedCursor)
	}
	if len(deps.Processed) != 3 {
		t.Fatalf("should have 3 processed children, got %d", len(deps.Processed))
	}

	// Now process the parent with a worker (removeOnComplete=true removes it)
	parentWorker := f3Worker(t, parentQueueName, prefix, f3Null, nil)

	// Wait for parent to be removed after completion
	f3WaitGone(t, parentQueue, parentID, 10*time.Second, "timed out waiting for parent to be removed")

	// Processed dependencies should now be empty (job + its processed hash removed)
	depsAfter := must(parentJob.GetDependencies(ctx, 0, 0, 100))
	if len(depsAfter.Processed) != 0 {
		t.Fatalf("processed after = %d, want 0", len(depsAfter.Processed))
	}

	_ = flow.Close()
	noErr(t, childWorker.Close(5*time.Second))
	noErr(t, parentWorker.Close(5*time.Second))
}

// Node.js: "when removeOnFail is true in last pending child > moves parent to wait without getting stuck"
func TestShouldMoveParentToWaitWithoutGettingStuckWithRemoveOnFailChild(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	queueName := testQueueName()
	queue := f3Queue(t, queueName, prefix)

	// Single worker on the queue: child0 fails, everything else succeeds.
	worker := f3Worker(t, queueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if job.Name == "child0" {
			return nil, fmt.Errorf("fail")
		}
		return nil, nil
	}, nil)

	flow := f3Flow(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent",
		QueueName: queueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child0", QueueName: queueName, Data: map[string]any{}, Opts: &bullmq.JobOptions{RemoveOnFail: bullmq.RemoveAll()}},
			{Name: "child1", QueueName: queueName, Data: map[string]any{}},
		},
	}))

	parentID := tree.Job.ID

	// Wait for the parent to complete (it should NOT get stuck)
	f3WaitState(t, queue, parentID, bullmq.StateCompleted, 10*time.Second, 50*time.Millisecond, "parent got stuck and did not complete")

	// The parent should have exactly 1 processed dependency (child1)
	parentJob := must(queue.GetJob(ctx, parentID))
	counts := must(parentJob.GetDependenciesCount(ctx))
	if counts.Processed != 1 {
		t.Fatalf("processed = %d, want 1", counts.Processed)
	}

	_ = flow.Close()
	noErr(t, worker.Close(5*time.Second))
}

// Node.js: "processes parent jobs added while a child job is active"
func TestShouldProcessParentAddedWhileChildIsActive(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	queueName := testQueueName()
	queue := f3Queue(t, queueName, prefix)

	// Worker sleeps ~1s per job so the child stays active long enough.
	worker := f3Worker(t, queueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		f3Sleep(ctx, 1000*time.Millisecond)
		return nil, nil
	}, nil)

	flow := f3Flow(t, prefix)

	// Add standalone job "mon" (no children). The worker picks it up and stays busy.
	must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "mon",
		QueueName: queueName,
		Data:      map[string]any{},
		Opts:      &bullmq.JobOptions{JobID: "mon"},
	}))

	// Let the worker pick up "mon" (it is now active and sleeping).
	time.Sleep(500 * time.Millisecond)

	// Add a parent "tue" that depends on the still-active "mon".
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "tue",
		QueueName: queueName,
		Data:      map[string]any{},
		Opts:      &bullmq.JobOptions{JobID: "tue"},
		Children: []bullmq.FlowJob{
			{Name: "mon", QueueName: queueName, Data: map[string]any{}, Opts: &bullmq.JobOptions{JobID: "mon"}},
		},
	}))

	// Eventually "tue" should be processed and completed.
	f3WaitState(t, queue, tree.Job.ID, bullmq.StateCompleted, 10*time.Second, 50*time.Millisecond, "tue did not complete")

	_ = flow.Close()
	noErr(t, worker.Close(5*time.Second))
}

// Node.js: "when child already existed and it is re-added with same parentId > moves parent to wait if child is already completed"
func TestShouldMoveParentToWaitIfChildAlreadyCompleted(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	queueName := testQueueName()
	queue := f3Queue(t, queueName, prefix)

	worker := f3Worker(t, queueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		f3Sleep(ctx, 250*time.Millisecond)
		return nil, nil
	}, nil)

	flow := f3Flow(t, prefix)

	// First flow: "tue" (removeOnComplete) with child "mon".
	first := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "tue",
		QueueName: queueName,
		Data:      map[string]any{},
		Opts:      &bullmq.JobOptions{JobID: "tue", RemoveOnComplete: bullmq.RemoveAll()},
		Children: []bullmq.FlowJob{
			{Name: "mon", QueueName: queueName, Data: map[string]any{}, Opts: &bullmq.JobOptions{JobID: "mon"}},
		},
	}))

	// Wait for first "tue" to complete (and be removed).
	f3WaitGone(t, queue, first.Job.ID, 10*time.Second, "first tue did not complete/remove")

	// Re-add "tue" with the already-completed child "mon".
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "tue",
		QueueName: queueName,
		Data:      map[string]any{},
		Opts:      &bullmq.JobOptions{JobID: "tue"},
		Children: []bullmq.FlowJob{
			{Name: "mon", QueueName: queueName, Data: map[string]any{}, Opts: &bullmq.JobOptions{JobID: "mon"}},
		},
	}))

	// The re-added "tue" should move to wait and complete.
	f3WaitState(t, queue, tree.Job.ID, bullmq.StateCompleted, 10*time.Second, 50*time.Millisecond, "re-added tue did not complete")

	_ = flow.Close()
	noErr(t, worker.Close(5*time.Second))
}

func TestShouldReleaseExternalParentForLeafAddAndAddBulk(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	childQueue := f3Queue(t, childQueueName, prefix)
	parentQueue := f3Queue(t, parentQueueName, prefix)

	childWorker := f3Worker(t, childQueueName, prefix, f3Null, nil)

	for _, id := range []string{"completed-add-child", "completed-bulk-child"} {
		must(childQueue.Add(ctx, "completed-child", map[string]any{}, &bullmq.JobOptions{JobID: id}))
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		addState := f3State(t, childQueue, "completed-add-child")
		bulkState := f3State(t, childQueue, "completed-bulk-child")
		if addState == bullmq.StateCompleted && bulkState == bullmq.StateCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("children should complete")
		}
		time.Sleep(50 * time.Millisecond)
	}

	addParent := must(parentQueue.Add(ctx, "add-parent", map[string]any{}, nil))
	bulkParent := must(parentQueue.Add(ctx, "bulk-parent", map[string]any{}, nil))
	for _, parent := range []*bullmq.Job{addParent, bulkParent} {
		c := parentQueue.Client()
		noErr(t, c.LRem(ctx, parentQueue.Keys().Wait(), 0, parent.ID).Err())
		noErr(t, c.Do(ctx, "ZADD", parentQueue.Keys().WaitingChildren(), 0, parent.ID).Err())
	}

	flow := f3Flow(t, prefix)
	makeFlow := func(jobID, parentID string) bullmq.FlowJob {
		return bullmq.FlowJob{
			Name:      "completed-child",
			QueueName: childQueueName,
			Data:      map[string]any{},
			Opts: &bullmq.JobOptions{
				JobID:  jobID,
				Parent: &bullmq.ParentOptions{ID: parentID, Queue: parentQueueName},
			},
		}
	}

	must(flow.Add(ctx, makeFlow("completed-add-child", addParent.ID)))
	if st := f3State(t, parentQueue, addParent.ID); st != bullmq.StateWaiting {
		t.Fatalf("add parent state = %s, want waiting", st)
	}

	must(flow.AddBulk(ctx, []bullmq.FlowJob{makeFlow("completed-bulk-child", bulkParent.ID)}))
	if st := f3State(t, parentQueue, bulkParent.ID); st != bullmq.StateWaiting {
		t.Fatalf("bulk parent state = %s, want waiting", st)
	}

	_ = flow.Close()
	noErr(t, childWorker.Close(5*time.Second))
}

// Node.js: "when child is retrieved from getJob > does not restore the parent reference"
func TestShouldNotRestoreParentReferenceAfterRemoveChildDependency(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	queueName := testQueueName()
	queue := f3Queue(t, queueName, prefix)

	flow := f3Flow(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent",
		QueueName: queueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child0", QueueName: queueName, Data: map[string]any{}},
		},
	}))

	child := tree.Children[0]
	childID := child.Job.ID
	parentKey := child.Job.ParentKey
	if parentKey == "" {
		t.Fatalf("child should have a parent key")
	}

	broken := must(queue.RemoveChildDependency(ctx, childID, parentKey))
	if !broken {
		t.Fatalf("dependency should be broken")
	}

	// Re-fetch the child from Redis: it should no longer carry a parent reference.
	childJob := must(queue.GetJob(ctx, childID))
	if childJob.Parent != nil {
		t.Fatalf("parent should be cleared")
	}
	if childJob.ParentKey != "" {
		t.Fatalf("parent_key should be cleared")
	}

	_ = flow.Close()
}

// f3OrderProcessor returns a processor that counts processed jobs and checks
// that the n-th job has data.order == n (priority order).
func f3OrderProcessor(counter *atomic.Int32, orderOK *atomic.Bool) bullmq.Processor {
	return func(ctx context.Context, job *bullmq.Job) (any, error) {
		n := counter.Add(1)
		f3Sleep(ctx, 25*time.Millisecond)
		var d struct {
			Order int `json:"order"`
		}
		_ = job.DecodeData(&d)
		if int(n) != d.Order {
			orderOK.Store(false)
		}
		return map[string]any{"value": d.Order}, nil
	}
}

// Node.js: "when priority option is provided > should process children before the parent prioritizing jobs per queueName"
func TestShouldProcessChildrenBeforeParentPrioritizingPerQueueName(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	grandQueueName := testQueueName()
	parentQueue := f3Queue(t, parentQueueName, prefix)
	_ = f3Queue(t, childQueueName, prefix)
	_ = f3Queue(t, grandQueueName, prefix)

	// Grandchildren worker (autorun disabled) - asserts priority order.
	var gcCounter atomic.Int32
	var gcOrderOK atomic.Bool
	gcOrderOK.Store(true)
	grandWorker := f3Worker(t, grandQueueName, prefix, f3OrderProcessor(&gcCounter, &gcOrderOK), &bullmq.WorkerOptions{ManualStart: true})

	// Children worker (autorun disabled) - asserts priority order.
	var cCounter atomic.Int32
	var cOrderOK atomic.Bool
	cOrderOK.Store(true)
	childWorker := f3Worker(t, childQueueName, prefix, f3OrderProcessor(&cCounter, &cOrderOK), &bullmq.WorkerOptions{ManualStart: true})

	flow := f3Flow(t, prefix)
	makeChild := func(order int, q string) bullmq.FlowJob {
		return bullmq.FlowJob{
			Name:      "child-job",
			QueueName: q,
			Data:      map[string]any{"order": order},
			Opts:      &bullmq.JobOptions{Priority: order},
		}
	}

	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			makeChild(1, childQueueName),
			makeChild(2, childQueueName),
			{
				Name:      "child-job",
				QueueName: childQueueName,
				Data:      map[string]any{"order": 3},
				Opts:      &bullmq.JobOptions{Priority: 3},
				Children: []bullmq.FlowJob{
					makeChild(1, grandQueueName),
					makeChild(2, grandQueueName),
					makeChild(3, grandQueueName),
				},
			},
		},
	}))

	if st := f3State(t, parentQueue, tree.Job.ID); st != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s", st)
	}

	// Process grandchildren first.
	noErr(t, grandWorker.Run())
	eventually(t, 10*time.Second, func() bool { return gcCounter.Load() >= 3 }, "grandchildren did not all process")
	if !gcOrderOK.Load() {
		t.Fatalf("grandchildren not processed in priority order")
	}

	// Then process children.
	noErr(t, childWorker.Run())
	eventually(t, 10*time.Second, func() bool { return cCounter.Load() >= 3 }, "children did not all process")
	if !cOrderOK.Load() {
		t.Fatalf("children not processed in priority order")
	}

	// Parent should now have all 3 dependencies processed.
	f3WaitState(t, parentQueue, tree.Job.ID, bullmq.StateWaiting, 10*time.Second, 50*time.Millisecond, "parent did not become ready")

	parentJob := must(parentQueue.GetJob(ctx, tree.Job.ID))
	deps := must(parentJob.GetDependencies(ctx, 0, 0, 100))
	if deps.NextProcessedCursor != 0 {
		t.Fatalf("next processed cursor = %d", deps.NextProcessedCursor)
	}
	if len(deps.Processed) != 3 {
		t.Fatalf("processed = %d, want 3", len(deps.Processed))
	}

	_ = flow.Close()
	noErr(t, grandWorker.Close(5*time.Second))
	noErr(t, childWorker.Close(5*time.Second))
}

// Node.js: "when continually adding jobs > adds jobs that do not exists"
func TestShouldAddJobsThatDoNotExistContinually(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	queueName := testQueueName()
	queue := f3Queue(t, queueName, prefix)

	worker := f3Worker(t, queueName, prefix, f3Null, nil)

	flow := f3Flow(t, prefix)
	node := func(name string, children ...bullmq.FlowJob) bullmq.FlowJob {
		return bullmq.FlowJob{
			Name:      name,
			QueueName: queueName,
			Data:      map[string]any{},
			Opts:      &bullmq.JobOptions{JobID: name},
			Children:  children,
		}
	}

	// tue depends on mon
	must(flow.Add(ctx, node("tue", node("mon"))))

	// wed depends on tue
	must(flow.Add(ctx, node("wed", node("tue"))))

	// Wait for wed to complete
	f3WaitState(t, queue, "wed", bullmq.StateCompleted, 10*time.Second, 50*time.Millisecond, "wed did not complete")

	// thu depends on wed (already completed)
	tree := must(flow.Add(ctx, node("thu", node("wed"))))

	f3WaitState(t, queue, tree.Job.ID, bullmq.StateCompleted, 10*time.Second, 50*time.Millisecond, "thu did not complete")

	_ = flow.Close()
	noErr(t, worker.Close(5*time.Second))
}

// Node.js: "when moving jobs from wait to active continuing > begins with attemptsMade as 1"
func TestShouldBeginWithAttemptsMadeAsOneInFlow(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	queueName := testQueueName()
	queue := f3Queue(t, queueName, prefix)

	// Processor verifies attempts_made + 1 == opts.attempts for task2/task3.
	var attemptsOK atomic.Bool
	attemptsOK.Store(true)
	var completed atomic.Int32

	worker := f3Worker(t, queueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if job.Name == "task2" || job.Name == "task3" {
			expected := job.Opts.Attempts
			if job.AttemptsMade+1 != expected {
				attemptsOK.Store(false)
			}
			completed.Add(1)
		}
		return nil, nil
	}, nil)

	flow := f3Flow(t, prefix)
	optsOneAttempt := func() *bullmq.JobOptions {
		return &bullmq.JobOptions{Attempts: 1, Backoff: bullmq.FixedBackoff(1000 * time.Millisecond)}
	}

	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "task3",
		QueueName: queueName,
		Data:      map[string]any{"status": "plan"},
		Opts:      optsOneAttempt(),
		Children: []bullmq.FlowJob{{
			Name:      "task2",
			QueueName: queueName,
			Data:      map[string]any{},
			Opts:      optsOneAttempt(),
			Children: []bullmq.FlowJob{{
				Name:      "task3",
				QueueName: queueName,
				Data:      map[string]any{"status": "proposal"},
				Opts:      optsOneAttempt(),
			}},
		}},
	}))

	if st := f3State(t, queue, tree.Job.ID); st != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s", st)
	}

	// Wait for all 3 jobs to complete.
	eventually(t, 15*time.Second, func() bool {
		return must(queue.GetJobCounts(ctx)).Completed >= 3
	}, "not all jobs completed")
	if !attemptsOK.Load() {
		t.Fatalf("attempts_made + 1 should equal opts.attempts")
	}

	if c := must(queue.GetJobCounts(ctx)).Completed; c != 3 {
		t.Fatalf("completed = %d, want 3", c)
	}

	_ = flow.Close()
	noErr(t, worker.Close(5*time.Second))
}

// f3PendingDepsProcessor is the step processor of the "pending dependencies"
// tests: at step 0 it adds a child whose parent is itself, then updates step
// to 1 and returns. Because the child is still pending, moveToFinished
// should fail the parent.
func f3PendingDepsProcessor(flow *bullmq.FlowProducer, childQueueName, prefix string, failParentOnFailure bool, pause time.Duration) bullmq.Processor {
	return func(ctx context.Context, job *bullmq.Job) (any, error) {
		var d struct {
			Step int `json:"step"`
		}
		_ = job.DecodeData(&d)
		if d.Step != 0 {
			return nil, nil
		}
		qualifiedName := job.QueueQualifiedName()
		if _, err := flow.Add(ctx, bullmq.FlowJob{
			Name:      "child-job",
			QueueName: childQueueName,
			Data:      map[string]any{},
			Opts: &bullmq.JobOptions{
				Parent:              &bullmq.ParentOptions{ID: job.ID, Queue: qualifiedName},
				FailParentOnFailure: failParentOnFailure,
			},
			Prefix: prefix,
		}); err != nil {
			return nil, fmt.Errorf("flow add: %w", err)
		}
		if err := job.UpdateData(ctx, map[string]any{"step": 1}); err != nil {
			return nil, fmt.Errorf("update data: %w", err)
		}
		if pause > 0 {
			f3Sleep(ctx, pause)
		}
		// Return OK: triggers moveToFinished which should fail with
		// pending dependencies (the child is still pending).
		return nil, nil
	}
}

// Node.js: "when parent has pending children to be processed when trying to move it to completed
//
//	> should fail parent with pending dependencies error"
func TestShouldFailParentWithPendingDependenciesError(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	queueName := testQueueName()
	childQueueName := testQueueName()
	queue := f3Queue(t, queueName, prefix)
	_ = f3Queue(t, childQueueName, prefix)

	flow := f3Flow(t, prefix)

	worker := f3Worker(t, queueName, prefix, f3PendingDepsProcessor(flow, childQueueName, prefix, false, 0), nil)

	job := must(queue.Add(ctx, "test", map[string]any{"step": 0}, nil))
	jobID := job.ID

	// Wait for the parent to be moved to failed.
	f3WaitState(t, queue, jobID, bullmq.StateFailed, 10*time.Second, 50*time.Millisecond, "parent did not move to failed")

	failedJob := must(queue.GetJob(ctx, jobID))
	want := fmt.Sprintf("Job %s has pending dependencies. moveToFinished", jobID)
	if failedJob.FailedReason != want {
		t.Fatalf("failed reason = %q, want %q", failedJob.FailedReason, want)
	}

	_ = flow.Close()
	noErr(t, worker.Close(5*time.Second))
}

// Node.js: "when parent has pending children ... (failParentOnFailure child)
//
//	> should fail parent with pending dependencies error"
func TestShouldFailParentWithPendingDependenciesErrorFpofChild(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	queueName := testQueueName()
	childQueueName := testQueueName()
	queue := f3Queue(t, queueName, prefix)
	_ = f3Queue(t, childQueueName, prefix)

	flow := f3Flow(t, prefix)

	worker := f3Worker(t, queueName, prefix, f3PendingDepsProcessor(flow, childQueueName, prefix, true, 100*time.Millisecond), nil)

	job := must(queue.Add(ctx, "test", map[string]any{"step": 0}, nil))
	jobID := job.ID

	f3WaitState(t, queue, jobID, bullmq.StateFailed, 10*time.Second, 50*time.Millisecond, "parent did not move to failed")

	failedJob := must(queue.GetJob(ctx, jobID))
	want := fmt.Sprintf("Job %s has pending dependencies. moveToFinished", jobID)
	if failedJob.FailedReason != want {
		t.Fatalf("failed reason = %q, want %q", failedJob.FailedReason, want)
	}

	_ = flow.Close()
	noErr(t, worker.Close(5*time.Second))
}

// Node.js: "when defaultJobOptions is provided > processes children before the parent"
func TestShouldProcessChildrenBeforeParentWithDefaultJobOptions(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f3Queue(t, parentQueueName, prefix)
	_ = f3Queue(t, childQueueName, prefix)

	// Child worker returns a value keyed by idx.
	childWorker := f3Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return map[string]any{"value": f3Idx(job)}, nil
	}, nil)

	// Parent worker verifies it has 3 processed dependencies.
	var depsOK atomic.Bool
	parentWorker := f3Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		deps, err := job.GetDependencies(ctx, 0, 0, 100)
		if err != nil {
			return nil, err
		}
		if deps.NextProcessedCursor == 0 && len(deps.Processed) == 3 {
			depsOK.Store(true)
		}
		return nil, nil
	}, nil)

	flow := f3Flow(t, prefix)

	// Provide defaultJobOptions removeOnComplete=true for the parent queue.
	flowOpts := &bullmq.FlowOptions{QueuesOptions: map[string]bullmq.FlowQueueOptions{
		parentQueueName: {DefaultJobOptions: bullmq.JobOptions{RemoveOnComplete: bullmq.RemoveAll()}},
	}}

	tree := must(flow.AddWithOptions(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 1, "foo": "baz"}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 2, "foo": "qux"}},
		},
	}, flowOpts))

	if st := f3State(t, parentQueue, tree.Job.ID); st != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s", st)
	}
	if len(tree.Children) != 3 {
		t.Fatalf("children len = %d", len(tree.Children))
	}

	// Parent should be processed and then removed (removeOnComplete=true).
	f3WaitGone(t, parentQueue, tree.Job.ID, 10*time.Second, "parent was not removed after completion")
	if !depsOK.Load() {
		t.Fatalf("parent did not see 3 processed deps")
	}

	if c := must(parentQueue.GetJobCounts(ctx)).Completed; c != 0 {
		t.Fatalf("completed = %d, want 0", c)
	}

	_ = flow.Close()
	noErr(t, childWorker.Close(5*time.Second))
	noErr(t, parentWorker.Close(5*time.Second))
}

// f3WaitThenMove polls the job's dependency counts until a child has failed,
// then tries to move it to waiting-children (propagating the resulting
// error).
func f3WaitThenMove(ctx context.Context, job *bullmq.Job) (any, error) {
	// Poll the job's dependency counts until a child has failed.
	for range 200 {
		counts, err := job.GetDependenciesCount(ctx)
		if err != nil {
			return nil, err
		}
		if counts.Failed >= 1 {
			break
		}
		f3Sleep(ctx, 50*time.Millisecond)
	}
	return f3MoveToWaitingChildren(ctx, job)
}

// f3MoveToWaitingChildren mirrors the Rust match on
// job.move_to_waiting_children(None).
func f3MoveToWaitingChildren(ctx context.Context, job *bullmq.Job) (any, error) {
	moved, err := job.MoveToWaitingChildren(ctx, "")
	if err != nil {
		return nil, err
	}
	if moved {
		return nil, bullmq.ErrWaitingChildren
	}
	return nil, nil
}

// f3AddChildOf adds flow node (with an external parent pointing to job) via
// flow, setting the parent options on the root node.
func f3AddChildOf(ctx context.Context, flow *bullmq.FlowProducer, job *bullmq.Job, node bullmq.FlowJob) (bullmq.JobNode, error) {
	if node.Opts == nil {
		node.Opts = &bullmq.JobOptions{}
	}
	node.Opts.Parent = &bullmq.ParentOptions{ID: job.ID, Queue: job.QueueQualifiedName()}
	n, err := flow.Add(ctx, node)
	if err != nil {
		return n, fmt.Errorf("flow add: %w", err)
	}
	return n, nil
}

// Node.js: "when parent has another parent > should fail parent and grandparent
//
//	when trying to move it to waiting children"
//
// A 3-level dynamically built flow: root (queue) -> child (children queue) ->
// grandchild (grandchildren queue). The grandchild fails with failParentOnFailure,
// which cascades: the child's moveToWaitingChildren fails with -9, the child fails
// (fpof) which marks it failed in the root's dependencies, and the root's
// moveToWaitingChildren then also fails with -9.
func TestShouldFailParentAndGrandparentWhenMovingToWaitingChildren(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	queueName := testQueueName()
	childrenQueueName := testQueueName()
	grandchildrenQueueName := testQueueName()
	queue := f3Queue(t, queueName, prefix)
	_ = f3Queue(t, childrenQueueName, prefix)
	_ = f3Queue(t, grandchildrenQueueName, prefix)

	flow := f3Flow(t, prefix)

	// Grandchild worker: fails immediately.
	grandchildrenWorker := f3Worker(t, grandchildrenQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, fmt.Errorf("fail")
	}, nil)

	// Child worker (step): adds a grandchild with fpof, then waits & moves.
	childrenWorker := f3Worker(t, childrenQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if _, err := f3AddChildOf(ctx, flow, job, bullmq.FlowJob{
			Name:      "grandchild-job",
			QueueName: grandchildrenQueueName,
			Data:      map[string]any{"idx": 0, "foo": "bar"},
			Opts:      &bullmq.JobOptions{FailParentOnFailure: true},
			Prefix:    prefix,
		}); err != nil {
			return nil, err
		}
		if err := job.UpdateData(ctx, map[string]any{"step": 1}); err != nil {
			return nil, err
		}
		return f3WaitThenMove(ctx, job)
	}, nil)

	// Root worker (step): adds a child with fpof, then waits & moves.
	worker := f3Worker(t, queueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if _, err := f3AddChildOf(ctx, flow, job, bullmq.FlowJob{
			Name:      "child-job",
			QueueName: childrenQueueName,
			Data:      map[string]any{},
			Opts:      &bullmq.JobOptions{FailParentOnFailure: true},
			Prefix:    prefix,
		}); err != nil {
			return nil, err
		}
		if err := job.UpdateData(ctx, map[string]any{"step": 1}); err != nil {
			return nil, err
		}
		return f3WaitThenMove(ctx, job)
	}, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"step": 0}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.FixedBackoff(1000 * time.Millisecond),
	}))
	jobID := job.ID

	// Root should be moved to failed with the descriptive message.
	f3WaitState(t, queue, jobID, bullmq.StateFailed, 20*time.Second, 50*time.Millisecond, "root did not move to failed")

	failedJob := must(queue.GetJob(ctx, jobID))
	want := fmt.Sprintf("Cannot complete job %s because it has at least one failed child. moveToWaitingChildren", jobID)
	if failedJob.FailedReason != want {
		t.Fatalf("failed reason = %q, want %q", failedJob.FailedReason, want)
	}

	// Root should have exactly one failed dependency.
	counts := must(failedJob.GetDependenciesCount(ctx))
	if counts.Failed != 1 || counts.Processed != 0 || counts.Unprocessed != 0 || counts.Ignored != 0 {
		t.Fatalf("dependencies count = %+v, want failed=1 only", counts)
	}

	_ = flow.Close()
	noErr(t, worker.Close(5*time.Second))
	noErr(t, childrenWorker.Close(5*time.Second))
	noErr(t, grandchildrenWorker.Close(5*time.Second))
}

// f3ChildWithFailingGrandchild is the child flow (fpof child with an fpof
// grandchild) used by the cascade tests.
func f3ChildWithFailingGrandchild(childrenQueueName, grandchildrenQueueName, prefix string) bullmq.FlowJob {
	return bullmq.FlowJob{
		Name:      "child-job",
		QueueName: childrenQueueName,
		Data:      map[string]any{},
		Opts:      &bullmq.JobOptions{FailParentOnFailure: true},
		Prefix:    prefix,
		Children: []bullmq.FlowJob{{
			Name:      "grandchild-job",
			QueueName: grandchildrenQueueName,
			Data:      map[string]any{"idx": 0, "foo": "bar"},
			Opts:      &bullmq.JobOptions{FailParentOnFailure: true},
			Prefix:    prefix,
		}},
	}
}

// Node.js: "when parent failed before moving to waiting-children > should fail
//
//	parent with last error"
//
// The parent (root) builds a child flow (with a failing grandchild via fpof) and
// then throws its own error *before* calling moveToWaitingChildren. The parent's
// failedReason must be its own last error ("fail"), not a child-related message.
func TestShouldFailParentWithLastError(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	queueName := testQueueName()
	childrenQueueName := testQueueName()
	grandchildrenQueueName := testQueueName()
	queue := f3Queue(t, queueName, prefix)
	_ = f3Queue(t, childrenQueueName, prefix)
	_ = f3Queue(t, grandchildrenQueueName, prefix)

	flow := f3Flow(t, prefix)

	// Grandchild worker fails immediately.
	grandchildrenWorker := f3Worker(t, grandchildrenQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, fmt.Errorf("fail")
	}, nil)

	// Root worker: adds the child flow then throws its own error "fail".
	worker := f3Worker(t, queueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if _, err := f3AddChildOf(ctx, flow, job, f3ChildWithFailingGrandchild(childrenQueueName, grandchildrenQueueName, prefix)); err != nil {
			return nil, err
		}
		if err := job.UpdateData(ctx, map[string]any{"step": 1}); err != nil {
			return nil, err
		}
		// Throw our own error before ever moving to waiting-children.
		return nil, fmt.Errorf("fail")
	}, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"step": 0}, nil))
	jobID := job.ID

	f3WaitState(t, queue, jobID, bullmq.StateFailed, 10*time.Second, 50*time.Millisecond, "root did not move to failed")

	failedJob := must(queue.GetJob(ctx, jobID))
	if failedJob.FailedReason != "fail" {
		t.Fatalf("failed reason = %q, want %q", failedJob.FailedReason, "fail")
	}

	_ = flow.Close()
	noErr(t, worker.Close(5*time.Second))
	noErr(t, grandchildrenWorker.Close(5*time.Second))
}

// f3StepOf returns data["step"] (0 when absent).
func f3StepOf(job *bullmq.Job) int {
	var d struct {
		Step int `json:"step"`
	}
	_ = job.DecodeData(&d)
	return d.Step
}

// Node.js: "when parent is in delayed state > should move parent to failed when
//
//	child is moved to failed"
//
// The root moves itself to the delayed state, then a failing grandchild (with
// failParentOnFailure) cascades: child fails, and the root — although delayed —
// is moved to failed with the reason `child <childKey> failed`.
func TestShouldMoveParentToFailedWhenChildFailsParentDelayed(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	queueName := testQueueName()
	childrenQueueName := testQueueName()
	grandchildrenQueueName := testQueueName()
	queue := f3Queue(t, queueName, prefix)
	_ = f3Queue(t, childrenQueueName, prefix)
	_ = f3Queue(t, grandchildrenQueueName, prefix)

	flow := f3Flow(t, prefix)
	var childIDSlot atomic.Value // string

	// Grandchild worker: sleeps then fails.
	grandchildrenWorker := f3Worker(t, grandchildrenQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		f3Sleep(ctx, 500*time.Millisecond)
		return nil, fmt.Errorf("failed")
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Children worker: no-op (children that become processable just complete).
	childrenWorker := f3Worker(t, childrenQueueName, prefix, f3Null, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Root worker: builds the child flow, then moves itself to delayed.
	worker := f3Worker(t, queueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if f3StepOf(job) == 0 {
			childTree, err := f3AddChildOf(ctx, flow, job, f3ChildWithFailingGrandchild(childrenQueueName, grandchildrenQueueName, prefix))
			if err != nil {
				return nil, err
			}
			childIDSlot.Store(childTree.Job.ID)

			if err := job.UpdateData(ctx, map[string]any{"step": 1}); err != nil {
				return nil, err
			}
			// Move ourselves to delayed (in the near future).
			if err := job.MoveToDelayed(ctx, time.Now().Add(1500*time.Millisecond)); err != nil {
				return nil, err
			}
			return nil, bullmq.ErrDelayed
		}
		// Promoted from delayed: try to move to waiting-children.
		return f3MoveToWaitingChildren(ctx, job)
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	job := must(queue.Add(ctx, "test", map[string]any{"step": 0}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.FixedBackoff(1000 * time.Millisecond),
	}))
	jobID := job.ID

	// Root should be moved to failed by the cascade.
	f3WaitState(t, queue, jobID, bullmq.StateFailed, 15*time.Second, 50*time.Millisecond, "root did not move to failed while delayed")

	childID, _ := childIDSlot.Load().(string)
	if childID == "" {
		t.Fatalf("child id was not captured")
	}
	expected := fmt.Sprintf("child %s:%s:%s failed", prefix, childrenQueueName, childID)
	failedJob := must(queue.GetJob(ctx, jobID))
	if failedJob.FailedReason != expected {
		t.Fatalf("failed reason = %q, want %q", failedJob.FailedReason, expected)
	}

	_ = flow.Close()
	noErr(t, worker.Close(5*time.Second))
	noErr(t, childrenWorker.Close(5*time.Second))
	noErr(t, grandchildrenWorker.Close(5*time.Second))
}

// Node.js: "when parent is in prioritized state > should move parent to failed
//
//	when child is moved to failed"
//
// The root rate-limits itself (moving back to the prioritized set, since it has
// a priority), then a failing grandchild cascades and the prioritized root is
// moved to failed with the reason `child <childKey> failed`.
func TestShouldMoveParentToFailedWhenChildFailsParentPrioritized(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	queueName := testQueueName()
	childrenQueueName := testQueueName()
	grandchildrenQueueName := testQueueName()
	queue := f3Queue(t, queueName, prefix)
	_ = f3Queue(t, childrenQueueName, prefix)
	_ = f3Queue(t, grandchildrenQueueName, prefix)

	flow := f3Flow(t, prefix)
	var childIDSlot atomic.Value // string

	// Grandchild worker: sleeps then fails.
	grandchildrenWorker := f3Worker(t, grandchildrenQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		f3Sleep(ctx, 500*time.Millisecond)
		return nil, fmt.Errorf("failed")
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Children worker: no-op.
	childrenWorker := f3Worker(t, childrenQueueName, prefix, f3Null, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Root worker: builds child flow then rate-limits itself (-> prioritized).
	rlQueue := queue
	worker := f3Worker(t, queueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if f3StepOf(job) == 0 {
			childTree, err := f3AddChildOf(ctx, flow, job, f3ChildWithFailingGrandchild(childrenQueueName, grandchildrenQueueName, prefix))
			if err != nil {
				return nil, err
			}
			childIDSlot.Store(childTree.Job.ID)

			if err := job.UpdateData(ctx, map[string]any{"step": 1}); err != nil {
				return nil, err
			}
			// Rate-limit ourselves: moves the job back to prioritized.
			if err := rlQueue.RateLimit(ctx, 2000*time.Millisecond); err != nil {
				return nil, err
			}
			return nil, bullmq.NewRateLimitError(2000 * time.Millisecond)
		}
		return f3MoveToWaitingChildren(ctx, job)
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	job := must(queue.Add(ctx, "test", map[string]any{"step": 0}, &bullmq.JobOptions{Priority: 10}))
	jobID := job.ID

	f3WaitState(t, queue, jobID, bullmq.StateFailed, 15*time.Second, 50*time.Millisecond, "root did not move to failed while prioritized")

	childID, _ := childIDSlot.Load().(string)
	if childID == "" {
		t.Fatalf("child id was not captured")
	}
	expected := fmt.Sprintf("child %s:%s:%s failed", prefix, childrenQueueName, childID)
	failedJob := must(queue.GetJob(ctx, jobID))
	if failedJob.FailedReason != expected {
		t.Fatalf("failed reason = %q, want %q", failedJob.FailedReason, expected)
	}

	_ = flow.Close()
	noErr(t, worker.Close(5*time.Second))
	noErr(t, childrenWorker.Close(5*time.Second))
	noErr(t, grandchildrenWorker.Close(5*time.Second))
}

// Node.js: "when removeOnComplete contains age in children and time is reached
//
//	> keeps children results in parent"
//
// Even when children are configured with removeOnComplete: { age }, their return
// values are still recorded in the parent's `:processed` hash.
func TestShouldKeepChildrenResultsInParentWithRemoveOnCompleteAge(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	queueName := testQueueName()
	_ = f3Queue(t, queueName, prefix)

	// Capture the parent's processed dependencies when it is processed.
	processedSlot := make(chan map[string]any, 1)

	// Single worker for both parent and children: returns the job name.
	// Children carry removeOnComplete: { age: 1 }.
	worker := f3Worker(t, queueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if job.Name == "parent" {
			deps, err := job.GetDependencies(ctx, 0, 0, 100)
			if err != nil {
				return nil, err
			}
			select {
			case processedSlot <- deps.Processed:
			default:
			}
		}
		return job.Name, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	flow := f3Flow(t, prefix)

	ageOpts := func() *bullmq.JobOptions {
		return &bullmq.JobOptions{RemoveOnComplete: bullmq.KeepJobsPolicy(bullmq.KeepJobs{Age: bullmq.Ptr[int64](1)})}
	}

	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent",
		QueueName: queueName,
		Data:      map[string]any{},
		Opts:      ageOpts(),
		Children: []bullmq.FlowJob{
			{Name: "child0", QueueName: queueName, Data: map[string]any{}, Opts: ageOpts()},
			{Name: "child1", QueueName: queueName, Data: map[string]any{}, Opts: ageOpts()},
		},
	}))

	// Wait for the parent to be processed (its processed deps captured).
	var processed map[string]any
	select {
	case processed = <-processedSlot:
	case <-time.After(10 * time.Second):
		t.Fatalf("parent was not processed")
	}

	if len(processed) != 2 {
		t.Fatalf("parent should have 2 processed children, got %d", len(processed))
	}

	// The processed value for each child should equal its name.
	children := tree.Children
	child0Key := fmt.Sprintf("%s:%s:%s", prefix, queueName, children[0].Job.ID)
	child1Key := fmt.Sprintf("%s:%s:%s", prefix, queueName, children[1].Job.ID)
	if v, ok := processed[child0Key]; !ok || v != "child0" {
		t.Fatalf("processed[%s] = %v (present %v), want child0", child0Key, v, ok)
	}
	if v, ok := processed[child1Key]; !ok || v != "child1" {
		t.Fatalf("processed[%s] = %v (present %v), want child1", child1Key, v, ok)
	}

	_ = flow.Close()
	noErr(t, worker.Close(5*time.Second))
}

// Node.js: "when priority is provided > processes children before the parent
//
//	respecting priority option"
//
// Children (each with a grandchild) are added with priorities; grandchildren are
// processed first, then children strictly in priority order, and finally the
// parent — which is removed on completion via per-queue defaultJobOptions.
func TestShouldProcessChildrenRespectingPriorityWithQueuesOptions(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	grandQueueName := testQueueName()
	parentQueue := f3Queue(t, parentQueueName, prefix)
	childQueue := f3Queue(t, childQueueName, prefix)
	_ = f3Queue(t, grandQueueName, prefix)

	// Grandchildren worker (autorun) — just completes.
	var gcCount atomic.Int32
	grandWorker := f3Worker(t, grandQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		gcCount.Add(1)
		f3Sleep(ctx, 50*time.Millisecond)
		return nil, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Children worker (autorun disabled) — verifies priority order via idx.
	var processedChildren atomic.Int32
	var orderOK atomic.Bool
	orderOK.Store(true)
	childWorker := f3Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		idx := 999
		var m map[string]any
		if err := job.DecodeData(&m); err == nil {
			if f, ok := m["idx"].(float64); ok {
				idx = int(f)
			}
		}
		n := int(processedChildren.Add(1) - 1)
		if idx != n {
			orderOK.Store(false)
		}
		return nil, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second, ManualStart: true})

	// Parent worker — verifies 3 processed dependencies.
	var depsOK atomic.Bool
	parentWorker := f3Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		deps, err := job.GetDependencies(ctx, 0, 0, 100)
		if err != nil {
			return nil, err
		}
		if deps.NextProcessedCursor == 0 && len(deps.Processed) == 3 {
			depsOK.Store(true)
		}
		return nil, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	flow := f3Flow(t, prefix)

	// child with priority p, data idx; one grandchild each.
	makeChild := func(idx, priority int) bullmq.FlowJob {
		return bullmq.FlowJob{
			Name:      "child-job",
			QueueName: childQueueName,
			Data:      map[string]any{"idx": idx},
			Opts:      &bullmq.JobOptions{Priority: priority},
			Children: []bullmq.FlowJob{
				{Name: "grandchild-job", QueueName: grandQueueName, Data: map[string]any{}},
			},
		}
	}

	// defaultJobOptions removeOnComplete=true for the parent queue.
	flowOpts := &bullmq.FlowOptions{QueuesOptions: map[string]bullmq.FlowQueueOptions{
		parentQueueName: {DefaultJobOptions: bullmq.JobOptions{RemoveOnComplete: bullmq.RemoveAll()}},
	}}

	tree := must(flow.AddWithOptions(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children:  []bullmq.FlowJob{makeChild(1, 2), makeChild(2, 3), makeChild(0, 1)},
	}, flowOpts))

	if st := f3State(t, parentQueue, tree.Job.ID); st != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s", st)
	}

	// Wait until all 3 children have become processable (in the prioritized set)
	// as their grandchildren complete — only then is priority ordering meaningful.
	deadline := time.Now().Add(10 * time.Second)
	for must(childQueue.GetJobCounts(ctx)).Prioritized < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("children did not all become prioritized")
		}
		time.Sleep(25 * time.Millisecond)
	}
	if gcCount.Load() < 3 {
		t.Fatalf("grandchildren did not all process")
	}

	// Now run the children worker; they should process in priority order.
	noErr(t, childWorker.Run())

	// Parent should be processed and then removed (removeOnComplete).
	f3WaitGone(t, parentQueue, tree.Job.ID, 10*time.Second, "parent was not removed after completion")
	if !orderOK.Load() {
		t.Fatalf("children not processed in priority order")
	}
	if !depsOK.Load() {
		t.Fatalf("parent did not see 3 processed deps")
	}

	if c := must(parentQueue.GetJobCounts(ctx)).Completed; c != 0 {
		t.Fatalf("completed = %d, want 0", c)
	}

	_ = flow.Close()
	noErr(t, grandWorker.Close(5*time.Second))
	noErr(t, childWorker.Close(5*time.Second))
	noErr(t, parentWorker.Close(5*time.Second))
}

// Node.js: "when removeOnFail option is provided > should remove parent when
//
//	child is moved to failed"
//
// A 3-level flow where the middle child has failParentOnFailure + removeOnFail.
// When its (failParentOnFailure) grandchild fails, the middle child is failed and
// removed, and the failure cascades to the grandparent which fails with
// `child <childKey> failed`.
func TestShouldRemoveParentWhenChildMovedToFailedRemoveOnFail(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	grandQueueName := testQueueName()
	parentQueue := f3Queue(t, parentQueueName, prefix)
	childQueue := f3Queue(t, childQueueName, prefix)
	_ = f3Queue(t, grandQueueName, prefix)

	// Grandchild worker: the first grandchild it sees fails ('failed'), the rest succeed.
	var gcCount atomic.Int32
	grandWorker := f3Worker(t, grandQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		n := gcCount.Add(1) - 1
		if n == 0 {
			f3Sleep(ctx, 200*time.Millisecond)
			return nil, fmt.Errorf("failed")
		}
		return nil, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Children worker (no-op) and parent worker (no-op).
	childrenWorker := f3Worker(t, childQueueName, prefix, f3Null, &bullmq.WorkerOptions{DrainDelay: time.Second})
	parentWorker := f3Worker(t, parentQueueName, prefix, f3Null, &bullmq.WorkerOptions{DrainDelay: time.Second})

	flow := f3Flow(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"foo": "bar"}},
			{
				Name:      "child-job",
				QueueName: childQueueName,
				Data:      map[string]any{"foo": "qux"},
				Opts:      &bullmq.JobOptions{FailParentOnFailure: true, RemoveOnFail: bullmq.RemoveAll()},
				Children: []bullmq.FlowJob{
					{Name: "child-job", QueueName: grandQueueName, Data: map[string]any{"foo": "bar"}, Opts: &bullmq.JobOptions{FailParentOnFailure: true}},
					{Name: "child-job", QueueName: grandQueueName, Data: map[string]any{"foo": "baz"}},
				},
			},
		},
	}))

	if st := f3State(t, parentQueue, tree.Job.ID); st != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s", st)
	}

	child1ID := tree.Children[1].Job.ID

	// Grandparent (root) should be moved to failed.
	f3WaitState(t, parentQueue, tree.Job.ID, bullmq.StateFailed, 15*time.Second, 50*time.Millisecond, "grandparent did not move to failed")

	// The middle child (removeOnFail) should have been removed.
	if j := must(childQueue.GetJob(ctx, child1ID)); j != nil {
		t.Fatalf("middle child should be removed")
	}

	// Grandparent failedReason references the middle child.
	expected := fmt.Sprintf("child %s:%s:%s failed", prefix, childQueueName, child1ID)
	failedJob := must(parentQueue.GetJob(ctx, tree.Job.ID))
	if failedJob.FailedReason != expected {
		t.Fatalf("failed reason = %q, want %q", failedJob.FailedReason, expected)
	}

	// Grandparent has exactly one failed dependency.
	counts := must(failedJob.GetDependenciesCount(ctx))
	if counts.Failed != 1 {
		t.Fatalf("failed deps = %d, want 1", counts.Failed)
	}

	_ = flow.Close()
	noErr(t, grandWorker.Close(5*time.Second))
	noErr(t, childrenWorker.Close(5*time.Second))
	noErr(t, parentWorker.Close(5*time.Second))
}

// Node.js: "should get paginated processed dependencies keys"
//
// Verifies that Job.GetDependencies paginates the processed children using the
// HSCAN cursor: iterating from cursor 0 with a small count eventually yields all
// 72 processed children and terminates with a 0 cursor.
func TestShouldGetPaginatedProcessedDependenciesKeys(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f3Queue(t, parentQueueName, prefix)
	_ = f3Queue(t, childQueueName, prefix)

	const numChildren = 72

	// Child worker: completes every child with a small value.
	childWorker := f3Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return map[string]any{"bar": "something"}, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second, Concurrency: 8})

	flow := f3Flow(t, prefix)
	children := make([]bullmq.FlowJob, 0, numChildren)
	for range numChildren {
		children = append(children, bullmq.FlowJob{
			Name:      "child-job",
			QueueName: childQueueName,
			Data:      map[string]any{"bar": "something"},
		})
	}

	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children:  children,
	}))

	parentID := tree.Job.ID

	// Wait for all children to be processed (parent moves to waiting).
	f3WaitState(t, parentQueue, parentID, bullmq.StateWaiting, 20*time.Second, 50*time.Millisecond, "parent did not become ready")

	parentJob := must(parentQueue.GetJob(ctx, parentID))

	// First page: at most ~50 entries, cursor should be non-zero (more to come).
	page1 := must(parentJob.GetDependencies(ctx, 0, 0, 50))
	if len(page1.Processed) < 50 {
		t.Fatalf("first page should contain at least 50 processed children, got %d", len(page1.Processed))
	}

	// Paginate fully from cursor 0 and collect all unique processed keys.
	allKeys := map[string]struct{}{}
	var cursor uint64
	for {
		page := must(parentJob.GetDependencies(ctx, cursor, 0, 50))
		for k := range page.Processed {
			allKeys[k] = struct{}{}
		}
		cursor = page.NextProcessedCursor
		if cursor == 0 {
			break
		}
	}
	if len(allKeys) != numChildren {
		t.Fatalf("pagination should yield all %d processed children, got %d", numChildren, len(allKeys))
	}

	_ = flow.Close()
	noErr(t, childWorker.Close(5*time.Second))
}

// Node.js: ".addBulk > should allow parent opts on the root job"
func TestShouldAllowParentOptsOnTheRootJobAddBulk(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	parentQueueName := testQueueName()
	grandparentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f3Queue(t, parentQueueName, prefix)
	grandparentQueue := f3Queue(t, grandparentQueueName, prefix)
	_ = f3Queue(t, childQueueName, prefix)

	// Add a standalone grandparent job.
	grandparentJob := must(grandparentQueue.Add(ctx, "grandparent", map[string]any{"foo": "bar"}, nil))
	grandparentID := grandparentJob.ID

	// Child worker returns a value keyed by idx.
	childWorker := f3Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return map[string]any{"value": f3Idx(job)}, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Parent worker verifies it has 2 processed dependencies.
	var depsOK atomic.Bool
	parentWorker := f3Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		deps, err := job.GetDependencies(ctx, 0, 0, 100)
		if err != nil {
			return nil, err
		}
		if deps.NextProcessedCursor == 0 && len(deps.Processed) == 2 {
			depsOK.Store(true)
		}
		return nil, nil
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	flow := f3Flow(t, prefix)
	trees := must(flow.AddBulk(ctx, []bullmq.FlowJob{{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Opts: &bullmq.JobOptions{
			Parent: &bullmq.ParentOptions{ID: grandparentID, Queue: grandparentQueueName},
		},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 1, "foo": "baz"}},
		},
	}}))

	tree := trees[0]
	expectedGrandparentQueueKey := fmt.Sprintf("%s:%s", prefix, grandparentQueueName)

	// The root job's parentKey should point at the grandparent.
	if want := fmt.Sprintf("%s:%s", expectedGrandparentQueueKey, grandparentID); tree.Job.ParentKey != want {
		t.Fatalf("parent key = %q, want %q", tree.Job.ParentKey, want)
	}

	if st := f3State(t, parentQueue, tree.Job.ID); st != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s", st)
	}
	if len(tree.Children) != 2 {
		t.Fatalf("children len = %d", len(tree.Children))
	}

	// Wait for the parent to be processed (children done -> parent runs).
	eventually(t, 10*time.Second, depsOK.Load, "parent was not processed with 2 deps")

	_ = flow.Close()
	noErr(t, childWorker.Close(5*time.Second))
	noErr(t, parentWorker.Close(5*time.Second))
}

// Node.js: ".remove > when there are unsuccessful children > removes all children
//
//	when removing a parent"
func TestShouldRemoveAllChildrenWhenRemovingParentWithUnsuccessfulChildren(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f3Queue(t, parentQueueName, prefix)
	childQueue := f3Queue(t, childQueueName, prefix)

	flow := f3Flow(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"}},
			{
				Name:      "child-job",
				QueueName: childQueueName,
				Data:      map[string]any{"idx": 0, "foo": "baz"},
				Opts:      &bullmq.JobOptions{FailParentOnFailure: true},
				Children: []bullmq.FlowJob{{
					Name:      "child-job",
					QueueName: childQueueName,
					Data:      map[string]any{"idx": 0, "foo": "qux"},
					Opts:      &bullmq.JobOptions{FailParentOnFailure: true},
				}},
			},
		},
	}))

	if st := f3State(t, parentQueue, tree.Job.ID); st != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s", st)
	}

	// Child worker fails every job, cascading the failure up to the root.
	childWorker := f3Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, fmt.Errorf("failure")
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Parent worker: a fpof child failure moves the parent to wait with a
	// deferred-failure marker; the worker fetch fails it for real.
	parentWorker := f3Worker(t, parentQueueName, prefix, f3Null, &bullmq.WorkerOptions{DrainDelay: time.Second})

	// Wait for the parent to fail (cascaded from the unsuccessful children).
	f3WaitState(t, parentQueue, tree.Job.ID, bullmq.StateFailed, 15*time.Second, 50*time.Millisecond, "parent did not fail from unsuccessful children")

	// Remove the parent (with children).
	must(parentQueue.Remove(ctx, tree.Job.ID))

	// Parent and all children should be gone.
	if j := must(parentQueue.GetJob(ctx, tree.Job.ID)); j != nil {
		t.Fatalf("parent should be removed")
	}
	children := tree.Children
	for _, child := range children {
		if j := must(childQueue.GetJob(ctx, child.Job.ID)); j != nil {
			t.Fatalf("child %s should be removed", child.Job.ID)
		}
	}
	grandchild := children[1].Children
	if j := must(childQueue.GetJob(ctx, grandchild[0].Job.ID)); j != nil {
		t.Fatalf("grandchild should be removed")
	}

	if c := must(childQueue.GetJobCounts(ctx)).Failed; c != 0 {
		t.Fatalf("failed = %d, want 0", c)
	}

	_ = flow.Close()
	noErr(t, childWorker.Close(5*time.Second))
	noErr(t, parentWorker.Close(5*time.Second))
}

// Job.IsWaitingChildren — a parent with pending children reports true.
func TestShouldReportIsWaitingChildrenOnParent(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f3Queue(t, parentQueueName, prefix)
	childQueue := f3Queue(t, childQueueName, prefix)

	flow := f3Flow(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0}},
		},
	}))

	// The parent (fetched with context) reports waiting-children.
	parentJob := must(parentQueue.GetJob(ctx, tree.Job.ID))
	if !must(parentJob.IsWaitingChildren(ctx)) {
		t.Fatalf("parent should be waiting-children")
	}

	// The leaf child is NOT waiting-children (it is waiting).
	child := tree.Children[0]
	childJob := must(childQueue.GetJob(ctx, child.Job.ID))
	if must(childJob.IsWaitingChildren(ctx)) {
		t.Fatalf("child should not be waiting-children")
	}

	_ = flow.Close()
}

// Job.GetIgnoredChildrenFailures — returns the ignored child failure reasons.
func TestShouldGetIgnoredChildrenFailuresViaJob(t *testing.T) {
	ctx := context.Background()
	prefix := f3Prefix
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueue := f3Queue(t, parentQueueName, prefix)
	_ = f3Queue(t, childQueueName, prefix)

	// Child worker always fails.
	childWorker := f3Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, fmt.Errorf("error")
	}, &bullmq.WorkerOptions{DrainDelay: time.Second})

	flow := f3Flow(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0}, Opts: &bullmq.JobOptions{IgnoreDependencyOnFailure: true}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 1}, Opts: &bullmq.JobOptions{IgnoreDependencyOnFailure: true}},
		},
	}))

	// Wait until both children have failed and been recorded as ignored.
	parentID := tree.Job.ID
	eventually(t, 10*time.Second, func() bool {
		return must(parentQueue.GetDependenciesCount(ctx, parentID)).Ignored >= 2
	}, "ignored children were not recorded")

	// Job.GetIgnoredChildrenFailures returns the 2 failure reasons.
	parentJob := must(parentQueue.GetJob(ctx, parentID))
	failures := must(parentJob.GetIgnoredChildrenFailures(ctx))
	if len(failures) != 2 {
		t.Fatalf("failures len = %d, want 2", len(failures))
	}
	for _, reason := range failures {
		if !strings.Contains(reason, "error") {
			t.Fatalf("unexpected reason: %s", reason)
		}
	}

	_ = flow.Close()
	noErr(t, childWorker.Close(5*time.Second))
}
