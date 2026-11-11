package bullmq_test

// Job-level method tests — clearLogs, discard, state checks
// (port of bullmq_rust/tests/job_test.rs).

import (
	"context"
	"reflect"
	"testing"
)

func mxAssertLogs(t *testing.T, logs []string, count, wantCount int64, want []string) {
	t.Helper()
	if count != wantCount {
		t.Fatalf("log count = %d, want %d", count, wantCount)
	}
	if want == nil {
		return
	}
	if len(want) == 0 && len(logs) == 0 {
		return
	}
	if !reflect.DeepEqual(logs, want) {
		t.Fatalf("logs = %q, want %q", logs, want)
	}
}

// Node.js: ".clearLogs > can clear the log"
func TestClearLogs(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))
	must(job.Log(ctx, "some log text 1"))
	must(job.Log(ctx, "some log text 2"))

	logs, count, err := queue.GetJobLogs(ctx, job.ID, 0, -1, true)
	noErr(t, err)
	mxAssertLogs(t, logs, count, 2, []string{"some log text 1", "some log text 2"})

	noErr(t, job.ClearLogs(ctx, 0))

	logs, count, err = queue.GetJobLogs(ctx, job.ID, 0, -1, true)
	noErr(t, err)
	mxAssertLogs(t, logs, count, 0, []string{})
}

// Node.js: ".clearLogs > can preserve up to keepLogs latest entries"
func TestClearLogsKeepLatest(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	job := must(queue.Add(ctx, "test", map[string]any{"foo": "bar"}, nil))
	must(job.Log(ctx, "some log text 1"))
	must(job.Log(ctx, "some log text 2"))
	must(job.Log(ctx, "some log text 3"))

	logs, count, err := queue.GetJobLogs(ctx, job.ID, 0, -1, true)
	noErr(t, err)
	mxAssertLogs(t, logs, count, 3, []string{"some log text 1", "some log text 2", "some log text 3"})

	// keepLogs larger than count is a no-op.
	noErr(t, job.ClearLogs(ctx, 4))
	logs, count, err = queue.GetJobLogs(ctx, job.ID, 0, -1, true)
	noErr(t, err)
	mxAssertLogs(t, logs, count, 3, nil)

	// keepLogs equal to count is a no-op.
	noErr(t, job.ClearLogs(ctx, 3))
	logs2, count, err := queue.GetJobLogs(ctx, job.ID, 0, -1, true)
	noErr(t, err)
	mxAssertLogs(t, logs2, count, 3, logs)

	// Keep only the latest 2.
	noErr(t, job.ClearLogs(ctx, 2))
	logs, count, err = queue.GetJobLogs(ctx, job.ID, 0, -1, true)
	noErr(t, err)
	mxAssertLogs(t, logs, count, 2, []string{"some log text 2", "some log text 3"})

	// keepLogs = 0 removes all.
	noErr(t, job.ClearLogs(ctx, 0))
	logs, count, err = queue.GetJobLogs(ctx, job.ID, 0, -1, true)
	noErr(t, err)
	mxAssertLogs(t, logs, count, 0, []string{})
}
