package bullmq_test

// Port of bullmq_rust/tests/flow_test.rs (part 1: tests starting before
// line 3204). Integration tests for BullMQ flows (parent-child job
// dependencies): they verify the FlowProducer works correctly, ensuring
// children are processed before parents, and that parent-child relationships
// are maintained properly.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/imdat99/bullmq"
)

// f1FlowProducer creates a FlowProducer with the test connection, closed at
// cleanup (Rust test_flow_producer).
func f1FlowProducer(t *testing.T, prefix string) *bullmq.FlowProducer {
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

// f1Queue creates a queue with a specific prefix (Rust test_queue_with_prefix).
func f1Queue(t *testing.T, name, prefix string) *bullmq.Queue {
	t.Helper()
	return newTestQueue(t, name, &bullmq.QueueOptions{Prefix: prefix})
}

// f1Worker creates a worker on the given prefix, closed at cleanup.
func f1Worker(t *testing.T, name, prefix string, p bullmq.Processor) *bullmq.Worker {
	t.Helper()
	return newTestWorker(t, name, p, &bullmq.WorkerOptions{Prefix: prefix})
}

// f1Barrier mimics a tokio Barrier of 2 shared by a processor and the test:
// the processor blocks in arrive until the test calls await (or the job
// context is cancelled, e.g. when the worker close times out).
type f1Barrier chan struct{}

func newF1Barrier() f1Barrier { return make(chan struct{}) }

// arrive is called from the processor.
func (b f1Barrier) arrive(ctx context.Context) {
	select {
	case b <- struct{}{}:
	case <-ctx.Done():
	}
}

// await is called from the test; it reports whether the processor arrived
// within timeout.
func (b f1Barrier) await(timeout time.Duration) bool {
	tm := time.NewTimer(timeout)
	defer tm.Stop()
	select {
	case <-b:
		return true
	case <-tm.C:
		return false
	}
}

// f1Data decodes job data into a map.
func f1Data(job *bullmq.Job) map[string]any {
	m := map[string]any{}
	_ = job.DecodeData(&m)
	return m
}

// f1Idx returns data["idx"] as an int (0 when absent).
func f1Idx(job *bullmq.Job) int {
	if v, ok := f1Data(job)["idx"].(float64); ok {
		return int(v)
	}
	return 0
}

// f1CheckMetaExists checks if a queue has a meta key set (Rust check_meta_exists).
func f1CheckMetaExists(ctx context.Context, q *bullmq.Queue) bool {
	_, err := q.Client().HGet(ctx, q.Keys().Meta(), "library").Result()
	return err == nil
}

func f1State(t *testing.T, q *bullmq.Queue, id string) bullmq.JobState {
	t.Helper()
	s, err := q.GetJobState(context.Background(), id)
	noErr(t, err)
	return s
}

func TestShouldProcessChildrenBeforeTheParent(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	f1Queue(t, parentQueueName, prefix)

	var childrenProcessed atomic.Int32
	// Track processing order
	var parentSawChildrenDone atomic.Bool

	values := []map[string]any{
		{"idx": 0, "foo": "bar"},
		{"idx": 1, "foo": "baz"},
		{"idx": 2, "foo": "qux"},
	}

	// Child worker
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		childrenProcessed.Add(1)
		return map[string]any{"value": f1Idx(job)}, nil
	})

	// Parent worker
	barrier := newF1Barrier()
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		// By the time parent is processed, all children should be done
		parentSawChildrenDone.Store(childrenProcessed.Load() == 3)
		barrier.arrive(ctx)
		return nil, nil
	})

	// Add the flow
	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: values[0]},
			{Name: "child-job", QueueName: childQueueName, Data: values[1]},
			{Name: "child-job", QueueName: childQueueName, Data: values[2]},
		},
	}))

	// Verify tree structure
	if tree.Job.ID == "" {
		t.Fatal("empty root id")
	}
	if len(tree.Children) != 3 {
		t.Fatalf("children = %d, want 3", len(tree.Children))
	}

	// Verify parent-child relationships
	for _, child := range tree.Children {
		p := child.Job.Parent
		if p == nil {
			t.Fatal("child has no parent")
		}
		if p.ID != tree.Job.ID {
			t.Fatalf("parent id = %q, want %q", p.ID, tree.Job.ID)
		}
		if want := fmt.Sprintf("%s:%s", prefix, parentQueueName); p.QueueKey != want {
			t.Fatalf("parent queue key = %q, want %q", p.QueueKey, want)
		}
	}

	// Wait for parent to be processed
	if !barrier.await(10 * time.Second) {
		t.Fatal("timed out waiting for parent to process")
	}

	// Verify children were all processed before parent
	if !parentSawChildrenDone.Load() {
		t.Fatal("parent did not see all children done")
	}
}

func TestShouldProcessParentWhenChildrenIsAnEmptyArray(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	queueName := testQueueName()
	f1Queue(t, queueName, prefix)

	barrier := newF1Barrier()
	f1Worker(t, queueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		barrier.arrive(ctx)
		return nil, nil
	})

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: queueName,
		Data:      map[string]any{},
		Children:  []bullmq.FlowJob{},
	}))

	// With empty children, parent should be processable immediately
	if tree.Job.ID == "" {
		t.Fatal("empty root id")
	}
	if len(tree.Children) != 0 {
		t.Fatalf("children = %d, want 0", len(tree.Children))
	}

	// Wait for parent to be processed
	if !barrier.await(5 * time.Second) {
		t.Fatal("parent should be processed with empty children")
	}
}

func TestShouldAllowPassingCustomJobIdInOptions(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	f1Queue(t, parentQueueName, prefix)

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Opts:      &bullmq.JobOptions{JobID: "my-parent-id"},
		Children: []bullmq.FlowJob{
			{
				Name: "child-job", QueueName: childQueueName, Data: map[string]any{"foo": "bar"},
				Opts: &bullmq.JobOptions{JobID: "my-child-id-1"},
			},
			{
				Name: "child-job", QueueName: childQueueName, Data: map[string]any{"foo": "baz"},
				Opts: &bullmq.JobOptions{JobID: "my-child-id-2"},
			},
		},
	}))

	if tree.Job.ID != "my-parent-id" {
		t.Fatalf("root id = %q", tree.Job.ID)
	}
	if len(tree.Children) != 2 {
		t.Fatalf("children = %d, want 2", len(tree.Children))
	}
	if tree.Children[0].Job.ID != "my-child-id-1" {
		t.Fatalf("child0 id = %q", tree.Children[0].Job.ID)
	}
	if tree.Children[1].Job.ID != "my-child-id-2" {
		t.Fatalf("child1 id = %q", tree.Children[1].Job.ID)
	}
}

func TestShouldProcessAChainOfJobs(t *testing.T) {
	// Tests nested flows: grandchild -> child -> parent
	ctx := context.Background()
	prefix := "bf-test"
	grandchildQueueName := testQueueName()
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, grandchildQueueName, prefix)
	f1Queue(t, childQueueName, prefix)
	f1Queue(t, parentQueueName, prefix)

	var mu sync.Mutex
	var processingOrder []string
	push := func(s string) {
		mu.Lock()
		processingOrder = append(processingOrder, s)
		mu.Unlock()
	}

	f1Worker(t, grandchildQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		push("grandchild:" + job.Name)
		return map[string]any{"from": "grandchild"}, nil
	})
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		push("child:" + job.Name)
		return map[string]any{"from": "child"}, nil
	})
	barrier := newF1Barrier()
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		push("parent:" + job.Name)
		barrier.arrive(ctx)
		return nil, nil
	})

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{{
			Name:      "child-job",
			QueueName: childQueueName,
			Data:      map[string]any{},
			Children: []bullmq.FlowJob{{
				Name:      "grandchild-job",
				QueueName: grandchildQueueName,
				Data:      map[string]any{},
			}},
		}},
	}))

	// Verify tree structure
	if len(tree.Children) != 1 {
		t.Fatalf("children = %d, want 1", len(tree.Children))
	}
	if len(tree.Children[0].Children) != 1 {
		t.Fatalf("grandchildren = %d, want 1", len(tree.Children[0].Children))
	}

	// Wait for parent to be processed
	if !barrier.await(10 * time.Second) {
		t.Fatal("timed out waiting for chain to process")
	}

	// Verify processing order: grandchild -> child -> parent
	mu.Lock()
	order := append([]string(nil), processingOrder...)
	mu.Unlock()
	if len(order) != 3 {
		t.Fatalf("order = %v, want 3 entries", order)
	}
	if !strings.HasPrefix(order[0], "grandchild:") ||
		!strings.HasPrefix(order[1], "child:") ||
		!strings.HasPrefix(order[2], "parent:") {
		t.Fatalf("unexpected order %v", order)
	}
}

func TestParentStateShouldBeWaitingChildrenAfterAdd(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	childQueue := f1Queue(t, childQueueName, prefix)
	parentQueue := f1Queue(t, parentQueueName, prefix)

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"foo": "bar"}},
		},
	}))

	// Verify parent is in waiting-children state
	parentJob := must(parentQueue.GetJob(ctx, tree.Job.ID))
	if parentJob == nil {
		t.Fatal("parent job not found")
	}
	// Check state via Redis
	if s := f1State(t, parentQueue, tree.Job.ID); s != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s, want waiting-children", s)
	}

	// Verify child is in wait state
	if s := f1State(t, childQueue, tree.Children[0].Job.ID); s != bullmq.StateWaiting {
		t.Fatalf("child state = %s, want waiting", s)
	}
}

func TestShouldNotProcessParentIfChildFails(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	parentQueue := f1Queue(t, parentQueueName, prefix)

	var parentProcessed atomic.Bool

	// Child worker that always fails
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, errors.New("child failed")
	})
	// Parent worker — should NOT be called
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		parentProcessed.Store(true)
		return nil, nil
	})

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{}},
		},
	}))

	// Wait a bit for child to fail
	time.Sleep(500 * time.Millisecond)

	// Parent should NOT have been processed
	if parentProcessed.Load() {
		t.Fatal("parent should not have been processed")
	}

	// Parent should still be in waiting-children state
	if s := f1State(t, parentQueue, tree.Job.ID); s != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s, want waiting-children", s)
	}
}

func TestShouldFailParentWhenChildWithFailParentOnFailureFails(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	parentQueue := f1Queue(t, parentQueueName, prefix)

	// Child worker that fails permanently
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, bullmq.NewUnrecoverableError("child error")
	})
	// Parent worker — needed so the deferred failure is processed
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, nil
	})

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{
				Name: "child-job", QueueName: childQueueName, Data: map[string]any{},
				Opts: &bullmq.JobOptions{FailParentOnFailure: true},
			},
		},
	}))

	// Wait for child to fail and propagate to parent, then parent worker picks it up
	time.Sleep(2000 * time.Millisecond)

	// Parent should have moved to failed state
	if s := f1State(t, parentQueue, tree.Job.ID); s != bullmq.StateFailed {
		t.Fatalf("parent state = %s, want failed", s)
	}
}

func TestShouldMoveParentToWaitWhenIgnoreDependencyOnFailure(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	f1Queue(t, parentQueueName, prefix)

	barrier := newF1Barrier()

	// Child worker that always fails
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, errors.New("child error")
	})
	// Parent worker — should be called since child failures are ignored
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		barrier.arrive(ctx)
		return nil, nil
	})

	flow := f1FlowProducer(t, prefix)
	must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{
				Name: "child-job", QueueName: childQueueName, Data: map[string]any{},
				Opts: &bullmq.JobOptions{IgnoreDependencyOnFailure: true},
			},
		},
	}))

	// Parent should eventually be processed (child failure is ignored)
	if !barrier.await(10 * time.Second) {
		t.Fatal("parent should be processed when child failure is ignored")
	}
}

func TestShouldMoveParentToWaitWhenRemoveDependencyOnFailure(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	f1Queue(t, parentQueueName, prefix)

	barrier := newF1Barrier()

	// Child worker that always fails
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, errors.New("child error")
	})
	// Parent worker — should be called since child dependency is removed on failure
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		barrier.arrive(ctx)
		return nil, nil
	})

	flow := f1FlowProducer(t, prefix)
	must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{
				Name: "child-job", QueueName: childQueueName, Data: map[string]any{},
				Opts: &bullmq.JobOptions{RemoveDependencyOnFailure: true},
			},
		},
	}))

	// Parent should eventually be processed
	if !barrier.await(10 * time.Second) {
		t.Fatal("parent should be processed when child dependency removed on failure")
	}
}

func TestShouldProcessParentWhenChildWithRemoveOnFailFails(t *testing.T) {
	// When removeOnFail is true and child is the last pending child,
	// parent should still move to wait
	ctx := context.Background()
	prefix := "bf-test"
	queueName := testQueueName()
	f1Queue(t, queueName, prefix)

	barrier := newF1Barrier()
	f1Worker(t, queueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if job.Name == "child0" {
			return nil, errors.New("fail")
		}
		if job.Name == "parent" {
			barrier.arrive(ctx)
		}
		return nil, nil
	})

	flow := f1FlowProducer(t, prefix)
	must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent",
		QueueName: queueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{
				Name: "child0", QueueName: queueName, Data: map[string]any{},
				Opts: &bullmq.JobOptions{RemoveOnFail: bullmq.RemoveAll()},
			},
			{Name: "child1", QueueName: queueName, Data: map[string]any{}},
		},
	}))

	// Parent should eventually be processed (child0 removed on fail, child1 succeeds)
	if !barrier.await(10 * time.Second) {
		t.Fatal("parent should be processed when removeOnFail child is removed")
	}
}

func TestShouldAddBulkFlows(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	parentQueue := f1Queue(t, parentQueueName, prefix)

	flow := f1FlowProducer(t, prefix)
	trees := must(flow.AddBulk(ctx, []bullmq.FlowJob{
		{
			Name:      "parent-1",
			QueueName: parentQueueName,
			Data:      map[string]any{"flow": 1},
			Children: []bullmq.FlowJob{
				{Name: "child-1", QueueName: childQueueName, Data: map[string]any{"child": 1}},
			},
		},
		{
			Name:      "parent-2",
			QueueName: parentQueueName,
			Data:      map[string]any{"flow": 2},
			Children: []bullmq.FlowJob{
				{Name: "child-2", QueueName: childQueueName, Data: map[string]any{"child": 2}},
			},
		},
	}))

	if len(trees) != 2 {
		t.Fatalf("trees = %d, want 2", len(trees))
	}
	for i, tr := range trees {
		if tr.Job.ID == "" {
			t.Fatalf("flow %d: empty id", i)
		}
		if len(tr.Children) != 1 {
			t.Fatalf("flow %d: children = %d, want 1", i, len(tr.Children))
		}
	}

	// Both parents should be in waiting-children state
	if s := f1State(t, parentQueue, trees[0].Job.ID); s != bullmq.StateWaitingChildren {
		t.Fatalf("state1 = %s", s)
	}
	if s := f1State(t, parentQueue, trees[1].Job.ID); s != bullmq.StateWaitingChildren {
		t.Fatalf("state2 = %s", s)
	}
}

func TestShouldAddMetaKeyToBothParentsAndChildren(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	childQueue := f1Queue(t, childQueueName, prefix)
	parentQueue := f1Queue(t, parentQueueName, prefix)

	flow := f1FlowProducer(t, prefix)
	must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{}},
		},
	}))

	// Both queues should have meta key set
	if !f1CheckMetaExists(ctx, parentQueue) {
		t.Fatal("parent meta missing")
	}
	if !f1CheckMetaExists(ctx, childQueue) {
		t.Fatal("child meta missing")
	}
}

func TestShouldProcessParentWithDelayAfterChildrenComplete(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	f1Queue(t, parentQueueName, prefix)

	barrier := newF1Barrier()

	// Child worker
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return map[string]any{"done": true}, nil
	})
	// Parent worker
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		barrier.arrive(ctx)
		return nil, nil
	})

	flow := f1FlowProducer(t, prefix)
	must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Opts:      &bullmq.JobOptions{Delay: 500 * time.Millisecond},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{}},
		},
	}))

	// Parent should be processed after delay + children complete
	if !barrier.await(10 * time.Second) {
		t.Fatal("parent with delay should be processed after children complete")
	}
}

func TestShouldProcessChildrenWithPriority(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	f1Queue(t, parentQueueName, prefix)

	var mu sync.Mutex
	var processingOrder []string
	barrier := newF1Barrier()

	// Child worker (processes sequentially since concurrency=1)
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		name, _ := f1Data(job)["name"].(string)
		mu.Lock()
		processingOrder = append(processingOrder, name)
		mu.Unlock()
		return nil, nil
	})
	// Parent worker
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		barrier.arrive(ctx)
		return nil, nil
	})

	flow := f1FlowProducer(t, prefix)
	must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{
				Name: "low-priority", QueueName: childQueueName, Data: map[string]any{"name": "low"},
				Opts: &bullmq.JobOptions{Priority: 10},
			},
			{
				Name: "high-priority", QueueName: childQueueName, Data: map[string]any{"name": "high"},
				Opts: &bullmq.JobOptions{Priority: 1},
			},
		},
	}))

	// Wait for parent to process (means all children done)
	if !barrier.await(10 * time.Second) {
		t.Fatal("timed out waiting for priority flow")
	}

	// Verify high priority child was processed first
	mu.Lock()
	order := append([]string(nil), processingOrder...)
	mu.Unlock()
	if len(order) != 2 || order[0] != "high" || order[1] != "low" {
		t.Fatalf("order = %v, want [high low]", order)
	}
}

func TestShouldContinueParentOnFailure(t *testing.T) {
	// continueParentOnFailure: when child fails, parent should still be processed
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	f1Queue(t, parentQueueName, prefix)

	barrier := newF1Barrier()

	// Child worker that always fails
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, errors.New("child failed")
	})
	// Parent worker
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		barrier.arrive(ctx)
		return nil, nil
	})

	flow := f1FlowProducer(t, prefix)
	must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{
				Name: "child-job", QueueName: childQueueName, Data: map[string]any{},
				Opts: &bullmq.JobOptions{ContinueParentOnFailure: true},
			},
		},
	}))

	// Parent should be processed even though child failed
	if !barrier.await(10 * time.Second) {
		t.Fatal("parent should be processed with continueParentOnFailure")
	}
}

func TestShouldProcessParentWithMultipleChildrenWhereSomeFail(t *testing.T) {
	// Multiple children: some succeed, some fail (with ignoreDependencyOnFailure)
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	f1Queue(t, parentQueueName, prefix)

	barrier := newF1Barrier()

	// Child worker: fails if idx is even
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		idx := f1Idx(job)
		if idx%2 == 0 {
			return nil, errors.New("even idx fails")
		}
		return map[string]any{"processed": idx}, nil
	})
	// Parent worker
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		barrier.arrive(ctx)
		return nil, nil
	})

	flow := f1FlowProducer(t, prefix)
	must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{
				Name: "child-0", QueueName: childQueueName, Data: map[string]any{"idx": 0},
				Opts: &bullmq.JobOptions{IgnoreDependencyOnFailure: true},
			},
			{Name: "child-1", QueueName: childQueueName, Data: map[string]any{"idx": 1}},
			{
				Name: "child-2", QueueName: childQueueName, Data: map[string]any{"idx": 2},
				Opts: &bullmq.JobOptions{IgnoreDependencyOnFailure: true},
			},
		},
	}))

	// Parent should be processed: child-0 and child-2 fail (ignored), child-1 succeeds
	if !barrier.await(10 * time.Second) {
		t.Fatal("parent should process when some children fail with ignoreDependencyOnFailure")
	}
}

// Node.js: "keeps children results in parent" (when removeOnComplete is true in children)
func TestShouldKeepChildrenResultsInParentWhenRemoveOnComplete(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	queueName := testQueueName()
	queue := f1Queue(t, queueName, prefix)

	barrier := newF1Barrier()

	// Worker returns job name
	f1Worker(t, queueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		name := job.Name
		if name == "parent" {
			// Verify we can access processed dependencies inside the processor
			childrenValues, err := job.GetChildrenValues(ctx)
			if err != nil {
				t.Errorf("GetChildrenValues: %v", err)
				return nil, err
			}
			if len(childrenValues) != 2 {
				t.Errorf("children values = %v, want 2 entries", childrenValues)
				return nil, errors.New("bad children values")
			}
			barrier.arrive(ctx)
		}
		return name, nil
	})

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent",
		QueueName: queueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{
				Name: "child0", QueueName: queueName, Data: map[string]any{},
				Opts: &bullmq.JobOptions{RemoveOnComplete: bullmq.RemoveAll()},
			},
			{
				Name: "child1", QueueName: queueName, Data: map[string]any{},
				Opts: &bullmq.JobOptions{RemoveOnComplete: bullmq.RemoveAll()},
			},
		},
	}))

	// Wait for parent to complete
	if !barrier.await(10 * time.Second) {
		t.Fatal("timed out waiting for parent to process")
	}

	// Verify that dependencies are stored (via queue method)
	deps := must(queue.GetDependenciesCount(ctx, tree.Job.ID))
	if deps.Processed != 2 {
		t.Fatalf("processed = %d, want 2", deps.Processed)
	}
}

// Node.js: "should process children before the parent" (full - with getChildrenValues)
func TestShouldProcessChildrenBeforeParentWithChildrenValues(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	parentQueue := f1Queue(t, parentQueueName, prefix)

	values := []map[string]any{
		{"bar": "something"},
		{"baz": "something"},
		{"qux": "something"},
	}

	// Child worker returns values[idx]
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		v, ok := f1Data(job)["idx"].(float64)
		if !ok {
			return nil, errors.New("missing idx")
		}
		return values[int(v)], nil
	})

	// Parent worker verifies children values
	barrier := newF1Barrier()
	var parentValuesOK atomic.Bool
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		childrenValues, err := job.GetChildrenValues(ctx)
		if err != nil {
			return nil, err
		}
		// Should have 3 processed children
		parentValuesOK.Store(len(childrenValues) == 3)
		barrier.arrive(ctx)
		return nil, nil
	})

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 1, "foo": "baz"}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 2, "foo": "qux"}},
		},
	}))

	if len(tree.Children) != 3 {
		t.Fatalf("children = %d, want 3", len(tree.Children))
	}

	// Verify parent state
	if s := f1State(t, parentQueue, tree.Job.ID); s != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s, want waiting-children", s)
	}

	// Wait for parent processing
	if !barrier.await(10 * time.Second) {
		t.Fatal("timed out waiting for parent to process")
	}
	if !parentValuesOK.Load() {
		t.Fatal("parent did not see 3 children values")
	}
}

// Node.js: getDependenciesCount when removeOnFail:true in one child
func TestShouldGetDependenciesCountWithRemoveOnFail(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	queueName := testQueueName()
	f1Queue(t, queueName, prefix)

	barrier := newF1Barrier()
	var depsCountOK atomic.Bool

	f1Worker(t, queueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if job.Name == "parent" {
			count, err := job.GetDependenciesCount(ctx)
			if err != nil {
				return nil, err
			}
			// (Rust note) The TS test in the "when removeOnFail is true"
			// describe is actually about removeOnComplete: child0 has
			// removeOnComplete=true; both children complete, so at least one
			// processed dependency must be tracked.
			depsCountOK.Store(count.Processed >= 1)
			barrier.arrive(ctx)
		}
		return job.Name, nil
	})

	flow := f1FlowProducer(t, prefix)
	must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent",
		QueueName: queueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{
				Name: "child0", QueueName: queueName, Data: map[string]any{},
				Opts: &bullmq.JobOptions{RemoveOnComplete: bullmq.RemoveAll()},
			},
			{Name: "child1", QueueName: queueName, Data: map[string]any{}},
		},
	}))

	if !barrier.await(10 * time.Second) {
		t.Fatal("timed out waiting for parent")
	}
	if !depsCountOK.Load() {
		t.Fatal("dependencies count processed < 1")
	}
}

// Node.js: "when ignoreDependencyOnFailure is provided" - "moves parent to wait after children fail"
func TestShouldMoveParentToWaitWithIgnoredChildrenFailures(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	parentQueue := f1Queue(t, parentQueueName, prefix)

	// Parent processor checks that ignored dependencies are tracked
	barrier := newF1Barrier()
	var ignoredCountOK atomic.Bool
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		deps, err := job.GetDependenciesCount(ctx)
		if err != nil {
			return nil, err
		}
		// All 3 children fail with ignoreDependencyOnFailure
		ignoredCountOK.Store(deps.Ignored == 3)
		barrier.arrive(ctx)
		return nil, nil
	})

	// Child worker always fails
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, errors.New("error")
	})

	ignore := func() *bullmq.JobOptions { return &bullmq.JobOptions{IgnoreDependencyOnFailure: true} }
	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"}, Opts: ignore()},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 1, "foo": "baz"}, Opts: ignore()},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 2, "foo": "qux"}, Opts: ignore()},
		},
	}))

	if s := f1State(t, parentQueue, tree.Job.ID); s != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s, want waiting-children", s)
	}

	// Wait for parent to be processed
	if !barrier.await(10 * time.Second) {
		t.Fatal("timed out waiting for parent to process")
	}
	if !ignoredCountOK.Load() {
		t.Fatal("parent did not see 3 ignored dependencies")
	}

	// Verify via queue method
	deps := must(parentQueue.GetDependenciesCount(ctx, tree.Job.ID))
	if deps.Ignored != 3 {
		t.Fatalf("ignored = %d, want 3", deps.Ignored)
	}

	// Verify failed children values
	failed := must(parentQueue.GetFailedChildrenValues(ctx, tree.Job.ID))
	if len(failed) != 3 {
		t.Fatalf("failed children values = %v, want 3 entries", failed)
	}
}

// Node.js: "should allow parent opts on the root job"
func TestShouldAllowParentOptsOnTheRootJob(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	grandparentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	parentQueue := f1Queue(t, parentQueueName, prefix)
	grandparentQueue := f1Queue(t, grandparentQueueName, prefix)

	// Add a grandparent job first
	grandparentJob := must(grandparentQueue.Add(ctx, "grandparent", map[string]any{"foo": "bar"}, nil))
	grandparentJobID := grandparentJob.ID

	// Child worker
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return map[string]any{"value": f1Idx(job)}, nil
	})

	// Parent worker verifies children values
	barrier := newF1Barrier()
	var parentOK atomic.Bool
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		childrenValues, err := job.GetChildrenValues(ctx)
		if err != nil {
			return nil, err
		}
		parentOK.Store(len(childrenValues) == 2)
		barrier.arrive(ctx)
		return nil, nil
	})

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Opts: &bullmq.JobOptions{Parent: &bullmq.ParentOptions{
			ID:    grandparentJobID,
			Queue: grandparentQueueName,
		}},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"}},
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 1, "foo": "baz"}},
		},
	}))

	// Verify parent key points to grandparent
	expectedParentKey := fmt.Sprintf("%s:%s:%s", prefix, grandparentQueueName, grandparentJobID)
	if tree.Job.ParentKey != expectedParentKey {
		t.Fatalf("parent key = %q, want %q", tree.Job.ParentKey, expectedParentKey)
	}

	// Verify parent state
	if s := f1State(t, parentQueue, tree.Job.ID); s != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s, want waiting-children", s)
	}
	if len(tree.Children) != 2 {
		t.Fatalf("children = %d, want 2", len(tree.Children))
	}

	// Wait for parent to process
	if !barrier.await(10 * time.Second) {
		t.Fatal("timed out waiting for parent to process")
	}
	if !parentOK.Load() {
		t.Fatal("parent did not see 2 children values")
	}
}

// Node.js: "retries a job after a delay if a fixed backoff is given" (adapted for fixed backoff)
func TestShouldRetryChildWithFixedBackoffInFlow(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	parentQueue := f1Queue(t, parentQueueName, prefix)

	// Child worker fails on first attempt, succeeds on second
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if job.AttemptsMade < 1 {
			return nil, errors.New("Not yet!")
		}
		return map[string]any{"result": "ok"}, nil
	})

	// Parent worker
	barrier := newF1Barrier()
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		barrier.arrive(ctx)
		return nil, nil
	})

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{
				Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"},
				Opts: &bullmq.JobOptions{Attempts: 3, Backoff: bullmq.FixedBackoff(200 * time.Millisecond)},
			},
		},
	}))

	if len(tree.Children) != 1 {
		t.Fatalf("children = %d, want 1", len(tree.Children))
	}

	// Parent state should be waiting-children
	if s := f1State(t, parentQueue, tree.Job.ID); s != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s, want waiting-children", s)
	}

	// Wait for parent to process (child retries then succeeds, then parent runs)
	if !barrier.await(10 * time.Second) {
		t.Fatal("timed out waiting for parent to process")
	}
}

// Node.js: "should not process parent until queue is unpaused"
func TestShouldNotProcessParentUntilQueueIsUnpaused(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	parentQueue := f1Queue(t, parentQueueName, prefix)

	// Pause parent queue before adding flow
	noErr(t, parentQueue.Pause(ctx))

	// Child worker
	childBarrier := newF1Barrier()
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		childBarrier.arrive(ctx)
		return "child-done", nil
	})

	// Parent worker
	parentBarrier := newF1Barrier()
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		parentBarrier.arrive(ctx)
		return nil, nil
	})

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"}},
		},
	}))
	if len(tree.Children) == 0 {
		t.Fatal("expected children")
	}

	// Wait for child to process
	if !childBarrier.await(10 * time.Second) {
		t.Fatal("timed out waiting for child")
	}

	// Give time for state propagation
	time.Sleep(500 * time.Millisecond)

	// Parent should be in waiting or paused list (queue is paused, so jobs land in paused list)
	counts := must(parentQueue.GetJobCounts(ctx))
	if counts.Waiting+counts.Paused != 1 {
		t.Fatalf("parent should be in waiting or paused state, got waiting=%d, paused=%d", counts.Waiting, counts.Paused)
	}

	// Resume parent queue
	noErr(t, parentQueue.Resume(ctx))

	// Wait for parent to process
	if !parentBarrier.await(10 * time.Second) {
		t.Fatal("timed out waiting for parent after resume")
	}

	// After processing, waiting should be 0
	counts = must(parentQueue.GetJobCounts(ctx))
	if counts.Waiting != 0 {
		t.Fatalf("waiting = %d, want 0", counts.Waiting)
	}
}

// Node.js: "should get unprocessed dependencies"
func TestShouldGetUnprocessedDependencies(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	parentQueue := f1Queue(t, parentQueueName, prefix)

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child0", QueueName: childQueueName, Data: map[string]any{}},
			{Name: "child1", QueueName: childQueueName, Data: map[string]any{}},
			{Name: "child2", QueueName: childQueueName, Data: map[string]any{}},
		},
	}))

	// Before any processing, all 3 children should be unprocessed
	deps := must(parentQueue.GetDependenciesCount(ctx, tree.Job.ID))
	if deps.Unprocessed != 3 || deps.Processed != 0 {
		t.Fatalf("deps = %+v, want unprocessed=3 processed=0", deps)
	}

	unprocessed := must(parentQueue.GetUnprocessedDependencies(ctx, tree.Job.ID))
	if len(unprocessed) != 3 {
		t.Fatalf("unprocessed = %v, want 3 entries", unprocessed)
	}

	// Now process children
	barrier := newF1Barrier()
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return "done", nil
	})
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		barrier.arrive(ctx)
		return nil, nil
	})

	// Wait for parent to complete (meaning all children done)
	if !barrier.await(10 * time.Second) {
		t.Fatal("timed out")
	}

	// After all processing, all should be processed
	deps = must(parentQueue.GetDependenciesCount(ctx, tree.Job.ID))
	if deps.Unprocessed != 0 || deps.Processed != 3 {
		t.Fatalf("deps = %+v, want unprocessed=0 processed=3", deps)
	}
}

// Node.js: "moves children to delayed"
func TestShouldMoveChildrenToDelayed(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	childQueue := f1Queue(t, childQueueName, prefix)
	parentQueue := f1Queue(t, parentQueueName, prefix)

	// Child worker
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return map[string]any{"result": f1Idx(job)}, nil
	})

	// Parent worker verifies children values
	barrier := newF1Barrier()
	var parentGotValues atomic.Bool
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		childrenValues, err := job.GetChildrenValues(ctx)
		if err != nil {
			return nil, err
		}
		parentGotValues.Store(len(childrenValues) == 1)
		barrier.arrive(ctx)
		return nil, nil
	})

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "root-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{
				Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"},
				Opts: &bullmq.JobOptions{Delay: 2000 * time.Millisecond},
			},
		},
	}))

	if len(tree.Children) != 1 {
		t.Fatalf("children = %d, want 1", len(tree.Children))
	}

	// Check parent state
	if s := f1State(t, parentQueue, tree.Job.ID); s != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s, want waiting-children", s)
	}

	// Check child is in delayed state
	if s := f1State(t, childQueue, tree.Children[0].Job.ID); s != bullmq.StateDelayed {
		t.Fatalf("child state = %s, want delayed", s)
	}

	// Wait for parent to be processed (child will be processed after delay)
	if !barrier.await(15 * time.Second) {
		t.Fatal("timed out waiting for parent")
	}
	if !parentGotValues.Load() {
		t.Fatal("parent did not see 1 child value")
	}
}

// Node.js: "should start processing parent after a child fails" (continueParentOnFailure)
func TestShouldStartProcessingParentAfterChildFailsWithContinue(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	f1Queue(t, parentQueueName, prefix)

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{
				Name: "child-job", QueueName: childQueueName, Data: map[string]any{"foo": "bar"},
				Opts: &bullmq.JobOptions{ContinueParentOnFailure: true},
			},
		},
	}))

	childID := tree.Children[0].Job.ID
	childQualifiedName := fmt.Sprintf("%s:%s", prefix, childQueueName)

	// Parent worker checks getFailedChildrenValues
	barrier := newF1Barrier()
	var parentCheckOK atomic.Bool
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		failedChildren, err := job.GetFailedChildrenValues(ctx)
		if err != nil {
			return nil, err
		}
		childKey := fmt.Sprintf("%s:%s", childQualifiedName, childID)
		hasFailedChild := failedChildren[childKey] == "failed"

		deps, err := job.GetDependenciesCount(ctx)
		if err != nil {
			return nil, err
		}
		// continueParentOnFailure puts failure in "ignored" (same as ignoreDependencyOnFailure)
		countsOK := deps.Processed == 0 && deps.Unprocessed == 0 && deps.Ignored == 1

		if !hasFailedChild || !countsOK {
			t.Logf("failed children = %v, deps = %+v", failedChildren, deps)
		}
		parentCheckOK.Store(hasFailedChild && countsOK)
		barrier.arrive(ctx)
		return nil, nil
	})

	// Child worker always fails
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, errors.New("failed")
	})

	if !barrier.await(10 * time.Second) {
		t.Fatal("timed out waiting for parent to process")
	}
	if !parentCheckOK.Load() {
		t.Fatal("parent check failed")
	}
}

// Node.js: "uses default prefix to add jobs" (custom prefix)
func TestShouldUseCustomPrefixToAddJobs(t *testing.T) {
	ctx := context.Background()
	customPrefix := "{bull}"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, customPrefix)
	parentQueue := f1Queue(t, parentQueueName, customPrefix)

	// Child worker
	f1Worker(t, childQueueName, customPrefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return map[string]any{"value": f1Idx(job)}, nil
	})

	// Parent worker verifies children values
	barrier := newF1Barrier()
	var parentOK atomic.Bool
	f1Worker(t, parentQueueName, customPrefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		childrenValues, err := job.GetChildrenValues(ctx)
		if err != nil {
			return nil, err
		}
		parentOK.Store(len(childrenValues) == 1)
		barrier.arrive(ctx)
		return nil, nil
	})

	flow, err := bullmq.NewFlowProducer(ctx, &bullmq.FlowProducerOptions{
		Connection: testConnection(),
		Prefix:     customPrefix,
	})
	noErr(t, err)
	t.Cleanup(func() { _ = flow.Close() })
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-job", QueueName: childQueueName, Data: map[string]any{"idx": 0, "foo": "bar"}},
		},
	}))

	if len(tree.Children) != 1 {
		t.Fatalf("children = %d, want 1", len(tree.Children))
	}

	// Verify parent state
	if s := f1State(t, parentQueue, tree.Job.ID); s != bullmq.StateWaitingChildren {
		t.Fatalf("parent state = %s, want waiting-children", s)
	}

	// Verify child data
	if foo := f1Data(tree.Children[0].Job)["foo"]; foo != "bar" {
		t.Fatalf("child data foo = %v, want bar", foo)
	}

	// Wait for parent to process
	if !barrier.await(10 * time.Second) {
		t.Fatal("timed out waiting for parent")
	}
	if !parentOK.Load() {
		t.Fatal("parent did not see 1 child value")
	}
}

// Node.js: "should move parent to failed when child is moved to failed" (deep fpof chain)
func TestShouldMoveParentToFailedInDeepChainWithFpof(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	parentQueueName := testQueueName()
	childQueueName := testQueueName()
	grandchildQueueName := testQueueName()
	parentQueue := f1Queue(t, parentQueueName, prefix)
	childQueue := f1Queue(t, childQueueName, prefix)
	f1Queue(t, grandchildQueueName, prefix)

	// Grandchild worker: first one fails, second succeeds
	var gcCounter atomic.Int32
	f1Worker(t, grandchildQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		c := gcCounter.Add(1) - 1
		if c == 0 {
			// First grandchild fails
			time.Sleep(200 * time.Millisecond)
			return nil, errors.New("failed")
		}
		return "ok", nil
	})

	// Child worker (just processes normally)
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return "child-done", nil
	})

	// Parent worker (just processes normally)
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return "parent-done", nil
	})

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-ok", QueueName: childQueueName, Data: map[string]any{"foo": "bar"}},
			{
				Name:      "child-with-grandchildren",
				QueueName: childQueueName,
				Data:      map[string]any{"foo": "qux"},
				Opts:      &bullmq.JobOptions{FailParentOnFailure: true},
				Children: []bullmq.FlowJob{
					{
						Name: "grandchild-fail", QueueName: grandchildQueueName, Data: map[string]any{"foo": "bar"},
						Opts: &bullmq.JobOptions{FailParentOnFailure: true},
					},
					{Name: "grandchild-ok", QueueName: grandchildQueueName, Data: map[string]any{"foo": "baz"}},
				},
			},
		},
	}))

	// Wait for the cascade failure to propagate
	time.Sleep(3 * time.Second)

	// The grandchild with fpof fails, which should fail the child (also fpof), which should fail the parent
	if s := f1State(t, parentQueue, tree.Job.ID); s != bullmq.StateFailed {
		t.Fatalf("parent should be failed due to fpof cascade, got %s", s)
	}

	// The child that has fpof should also be failed
	childWithGC := tree.Children[1].Job.ID
	if s := f1State(t, childQueue, childWithGC); s != bullmq.StateFailed {
		t.Fatalf("child with fpof should be failed, got %s", s)
	}
}

// Node.js: "should start processing parent after child fails even with more unprocessed children"
func TestShouldProcessParentAfterChildFailsWithUnprocessedSiblings(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	f1Queue(t, parentQueueName, prefix)

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{Name: "child-1", QueueName: childQueueName, Data: map[string]any{"foo": "baz"}},
			{Name: "child-2", QueueName: childQueueName, Data: map[string]any{"foo": "baz"}},
			{
				Name: "child-fail", QueueName: childQueueName, Data: map[string]any{"foo": "bar"},
				Opts: &bullmq.JobOptions{ContinueParentOnFailure: true},
			},
			{Name: "child-3", QueueName: childQueueName, Data: map[string]any{"foo": "baz"}},
			{Name: "child-4", QueueName: childQueueName, Data: map[string]any{"foo": "baz"}},
		},
	}))

	childToFailID := tree.Children[2].Job.ID

	// Parent worker checks failed children
	barrier := newF1Barrier()
	var parentCheckOK atomic.Bool
	childQualifiedName := fmt.Sprintf("%s:%s", prefix, childQueueName)
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		failedChildren, err := job.GetFailedChildrenValues(ctx)
		if err != nil {
			return nil, err
		}
		childKey := fmt.Sprintf("%s:%s", childQualifiedName, childToFailID)
		parentCheckOK.Store(failedChildren[childKey] == "failed")
		barrier.arrive(ctx)
		return nil, nil
	})

	// Child worker: fails the specific child, processes others normally
	failID := tree.Children[2].Job.ID
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if job.ID == failID {
			return nil, errors.New("failed")
		}
		return "ok", nil
	})

	// Parent should process after all children complete/fail
	if !barrier.await(10 * time.Second) {
		t.Fatal("timed out waiting for parent to process")
	}
	if !parentCheckOK.Load() {
		t.Fatal("parent should see failed child in getFailedChildrenValues")
	}
}

// Node.js: "moves parent to wait without getting stuck" (removeOnFail:true in last pending child)
func TestShouldMoveParentToWaitWhenRemoveOnFailChildFails(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	queueName := testQueueName()
	f1Queue(t, queueName, prefix)

	barrier := newF1Barrier()
	var depsOK atomic.Bool
	f1Worker(t, queueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		if job.Name == "child0" {
			return nil, errors.New("fail")
		}
		if job.Name == "parent" {
			count, err := job.GetDependenciesCount(ctx)
			if err != nil {
				return nil, err
			}
			// child0 fails with removeOnFail:true → removed, child1 succeeds → processed=1
			depsOK.Store(count.Processed == 1)
			barrier.arrive(ctx)
		}
		return job.Name, nil
	})

	flow := f1FlowProducer(t, prefix)
	must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent",
		QueueName: queueName,
		Data:      map[string]any{},
		Children: []bullmq.FlowJob{
			{
				Name: "child0", QueueName: queueName, Data: map[string]any{},
				Opts: &bullmq.JobOptions{RemoveOnFail: bullmq.RemoveAll()},
			},
			{Name: "child1", QueueName: queueName, Data: map[string]any{}},
		},
	}))

	if !barrier.await(10 * time.Second) {
		t.Fatal("timed out")
	}
	if !depsOK.Load() {
		t.Fatal("processed dependencies != 1")
	}
}

// Node.js: "should move the parent to delayed after a child fails" (cpof + parent delay)
func TestShouldMoveParentToDelayedAfterChildFailsWithCpof(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	parentQueue := f1Queue(t, parentQueueName, prefix)

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Opts:      &bullmq.JobOptions{Delay: 1000 * time.Millisecond},
		Children: []bullmq.FlowJob{
			{
				Name: "child-job", QueueName: childQueueName, Data: map[string]any{"foo": "bar"},
				Opts: &bullmq.JobOptions{ContinueParentOnFailure: true},
			},
		},
	}))

	childID := tree.Children[0].Job.ID
	childQualifiedName := fmt.Sprintf("%s:%s", prefix, childQueueName)

	// Child worker always fails
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, errors.New("failed")
	})

	// Wait for child to fail
	time.Sleep(500 * time.Millisecond)

	// Parent should be delayed (not waiting, because it has delay opt)
	counts := must(parentQueue.GetJobCounts(ctx))
	if counts.Delayed != 1 {
		t.Fatalf("parent should be in delayed state, counts = %+v", counts)
	}
	if counts.Waiting != 0 {
		t.Fatalf("waiting = %d, want 0", counts.Waiting)
	}

	// Parent worker verifies failed children
	barrier := newF1Barrier()
	var parentOK atomic.Bool
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		failed, err := job.GetFailedChildrenValues(ctx)
		if err != nil {
			return nil, err
		}
		childKey := fmt.Sprintf("%s:%s", childQualifiedName, childID)
		parentOK.Store(failed[childKey] == "failed")
		barrier.arrive(ctx)
		return nil, nil
	})

	if !barrier.await(10 * time.Second) {
		t.Fatal("timed out waiting for parent")
	}
	if !parentOK.Load() {
		t.Fatal("parent did not see the failed child")
	}
}

// Node.js: "should move the parent to prioritized after a child fails" (cpof + parent priority)
func TestShouldMoveParentToPrioritizedAfterChildFailsWithCpof(t *testing.T) {
	ctx := context.Background()
	prefix := "bf-test"
	childQueueName := testQueueName()
	parentQueueName := testQueueName()
	f1Queue(t, childQueueName, prefix)
	parentQueue := f1Queue(t, parentQueueName, prefix)

	flow := f1FlowProducer(t, prefix)
	tree := must(flow.Add(ctx, bullmq.FlowJob{
		Name:      "parent-job",
		QueueName: parentQueueName,
		Data:      map[string]any{},
		Opts:      &bullmq.JobOptions{Priority: 42},
		Children: []bullmq.FlowJob{
			{
				Name: "child-job", QueueName: childQueueName, Data: map[string]any{"foo": "bar"},
				Opts: &bullmq.JobOptions{ContinueParentOnFailure: true},
			},
		},
	}))

	// Child worker always fails
	f1Worker(t, childQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		return nil, errors.New("failed")
	})

	// Wait for child to fail
	time.Sleep(500 * time.Millisecond)

	// Parent should be prioritized (not waiting or delayed)
	counts := must(parentQueue.GetJobCounts(ctx))
	if counts.Prioritized != 1 {
		t.Fatalf("parent should be in prioritized state, counts = %+v", counts)
	}
	if counts.Delayed != 0 || counts.Waiting != 0 {
		t.Fatalf("delayed = %d, waiting = %d, want 0, 0", counts.Delayed, counts.Waiting)
	}

	// Parent worker verifies failed children
	childID := tree.Children[0].Job.ID
	childQualifiedName := fmt.Sprintf("%s:%s", prefix, childQueueName)

	barrier := newF1Barrier()
	var parentOK atomic.Bool
	f1Worker(t, parentQueueName, prefix, func(ctx context.Context, job *bullmq.Job) (any, error) {
		failed, err := job.GetFailedChildrenValues(ctx)
		if err != nil {
			return nil, err
		}
		childKey := fmt.Sprintf("%s:%s", childQualifiedName, childID)
		parentOK.Store(failed[childKey] == "failed")
		barrier.arrive(ctx)
		return nil, nil
	})

	if !barrier.await(10 * time.Second) {
		t.Fatal("timed out waiting for parent")
	}
	if !parentOK.Load() {
		t.Fatal("parent did not see the failed child")
	}
}
