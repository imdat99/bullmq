package bullmq_test

// Prometheus metrics export tests — Queue.ExportPrometheusMetrics
// (port of bullmq_rust/tests/prometheus_test.rs).
//
// Mirrors Node.js `tests/getters.test.ts` `#exportPrometheusMetrics`.
// Requires a running Redis instance at redis://127.0.0.1:6379.

import (
	"bullmq"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// mxAssertContains fails when s does not contain sub.
func mxAssertContains(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Fatalf("output does not contain %q:\n%s", sub, s)
	}
}

func TestExportsJobStatesInGaugeFormat(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// 3 waiting, 1 delayed.
	for i := 0; i < 3; i++ {
		must(queue.Add(ctx, "w", map[string]any{}, nil))
	}
	must(queue.Add(ctx, "d", map[string]any{}, &bullmq.JobOptions{Delay: 60_000 * time.Millisecond}))

	metrics := must(queue.ExportPrometheusMetrics(ctx))

	mxAssertContains(t, metrics, "# HELP bullmq_job_count Number of jobs in the queue by state")
	mxAssertContains(t, metrics, "# TYPE bullmq_job_count gauge")
	mxAssertContains(t, metrics, fmt.Sprintf(`bullmq_job_count{queue="%s", state="waiting"} 3`, name))
	mxAssertContains(t, metrics, fmt.Sprintf(`bullmq_job_count{queue="%s", state="delayed"} 1`, name))
	// Counters from the time-series metrics are always present.
	mxAssertContains(t, metrics, "# TYPE bullmq_job_completed_total counter")
	mxAssertContains(t, metrics, "# TYPE bullmq_job_failed_total counter")
}

func TestExportsWithGlobalLabels(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	must(queue.Add(ctx, "w", map[string]any{}, nil))

	metrics := must(queue.ExportPrometheusMetrics(ctx,
		bullmq.PrometheusLabel{Name: "env", Value: "Production"},
		bullmq.PrometheusLabel{Name: "server", Value: "1"}))

	mxAssertContains(t, metrics, fmt.Sprintf(
		`bullmq_job_count{queue="%s", state="waiting", env="Production", server="1"} 1`, name,
	))
	mxAssertContains(t, metrics, fmt.Sprintf(
		`bullmq_job_completed_total{queue="%s", env="Production", server="1"} 0`, name,
	))
}

func TestExportsAllStatesPresent(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	metrics := must(queue.ExportPrometheusMetrics(ctx))
	for _, state := range []string{
		"active", "completed", "delayed", "failed", "paused", "prioritized", "waiting", "waiting-children",
	} {
		if !strings.Contains(metrics, fmt.Sprintf(`state="%s"`, state)) {
			t.Fatalf("missing state %s", state)
		}
	}
}

func TestCompletedAndFailedTotalsReflectMetrics(t *testing.T) {
	ctx := context.Background()
	name := testQueueName()
	queue := newTestQueue(t, name, nil)

	// 2 completed, 1 failed.
	for i := 0; i < 2; i++ {
		must(queue.Add(ctx, "ok", map[string]any{}, nil))
	}
	must(queue.Add(ctx, "bad", map[string]any{}, nil))

	var done atomic.Int32
	// Metrics option enables the time-series counters that back the totals.
	worker := newTestWorker(t, name, func(ctx context.Context, job *bullmq.Job) (any, error) {
		done.Add(1)
		if job.Name == "bad" {
			return nil, errors.New("fail")
		}
		return nil, nil
	}, &bullmq.WorkerOptions{Metrics: &bullmq.MetricsOptions{}})

	eventually(t, 15*time.Second, func() bool { return done.Load() >= 3 }, "timed out processing")
	time.Sleep(150 * time.Millisecond)

	metrics := must(queue.ExportPrometheusMetrics(ctx))
	mxAssertContains(t, metrics, fmt.Sprintf(`bullmq_job_completed_total{queue="%s"} 2`, name))
	mxAssertContains(t, metrics, fmt.Sprintf(`bullmq_job_failed_total{queue="%s"} 1`, name))

	noErr(t, worker.Close(5*time.Second))
}

func TestLabelValuesAreEscaped(t *testing.T) {
	ctx := context.Background()
	queue := newTestQueue(t, testQueueName(), nil)

	must(queue.Add(ctx, "w", map[string]any{}, nil))

	// A label value containing a quote, backslash and newline must be escaped.
	metrics := must(queue.ExportPrometheusMetrics(ctx, bullmq.PrometheusLabel{Name: "note", Value: "a\"b\\c\nd"}))

	if !strings.Contains(metrics, `note="a\"b\\c\nd"`) {
		t.Fatalf("label value was not escaped: %s", metrics)
	}
}
