package bullmq_test

// Throughput benchmarks, the Go counterpart of bullmq_rust/examples/benchmark.rs.
//
//	go test -run '^$' -bench . -benchtime 5000x -count 5 | tee new.txt
//	benchstat old.txt new.txt
//
// Every benchmark op is exactly one job, so the Redis data a run creates is
// bounded by b.N (see benchGuard), and every benchmark deletes all of its
// queue keys when it ends. Run them against a memory-capped Redis, e.g.
//
//	docker run -d --rm -p 127.0.0.1:6380:6379 --memory 1536m redis:7-alpine \
//	  redis-server --save '' --appendonly no --maxmemory 1gb --maxmemory-policy noeviction
//	REDIS_URL=redis://127.0.0.1:6380 go test -run '^$' -bench . -benchtime 5000x -count 6
//
// Each benchmark reports a jobs/s metric in addition to ns/op.

import (
	"bullmq"
	"context"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var benchPayload = map[string]any{"index": 0, "payload": strings.Repeat("x", 100)}

// benchMaxJobs caps the jobs a single benchmark may create (~600 bytes each
// in Redis), so a large -benchtime cannot exhaust the Redis host's memory.
const benchMaxJobs = 200_000

// benchQueue returns a fresh queue for a benchmark after checking b.N
// against benchMaxJobs, and deletes every key of the queue at cleanup.
func benchQueue(b *testing.B, kind string) *bullmq.Queue {
	b.Helper()

	if b.N > benchMaxJobs {
		b.Fatalf("b.N=%d would create more than %d jobs; lower -benchtime", b.N, benchMaxJobs)
	}

	q := newTestQueue(b, "bench-"+kind+"-"+testQueueName(), nil)
	b.Cleanup(func() { benchDeleteKeys(b, q) })

	return q
}

// benchDeleteKeys removes every key of q with SCAN + UNLINK, so nothing is
// left behind even when Obliterate could not finish.
func benchDeleteKeys(b *testing.B, q *bullmq.Queue) {
	b.Helper()

	ctx := context.Background()
	iter := q.Client().Scan(ctx, 0, q.Keys().KeyPrefix()+"*", 1000).Iterator()

	batch := make([]string, 0, 1000)
	for iter.Next(ctx) {
		if batch = append(batch, iter.Val()); len(batch) == cap(batch) {
			q.Client().Unlink(ctx, batch...)
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		q.Client().Unlink(ctx, batch...)
	}

	if err := iter.Err(); err != nil {
		b.Logf("cleanup scan: %v", err)
	}
}

func reportJobsPerSec(b *testing.B, jobs int) {
	b.Helper()
	b.ReportMetric(float64(jobs)/b.Elapsed().Seconds(), "jobs/s")
}

// BenchmarkAddJob adds jobs one by one (Rust: benchmark_add_jobs).
func BenchmarkAddJob(b *testing.B) {
	ctx := context.Background()
	q := benchQueue(b, "add")

	b.ReportAllocs()
	for b.Loop() {
		if _, err := q.Add(ctx, "bench", benchPayload, nil); err != nil {
			b.Fatal(err)
		}
	}
	reportJobsPerSec(b, b.N)
}

// BenchmarkAddJobParallel adds jobs from many goroutines sharing one queue
// (Rust: benchmark_add_jobs_parallel).
func BenchmarkAddJobParallel(b *testing.B) {
	ctx := context.Background()
	q := benchQueue(b, "addp")

	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := q.Add(ctx, "bench", benchPayload, nil); err != nil {
				b.Error(err)
				return
			}
		}
	})
	reportJobsPerSec(b, b.N)
}

// BenchmarkAddBulk adds b.N jobs in batches of 1000 (Rust:
// benchmark_bulk_add). One op is one job, like the other benchmarks.
func BenchmarkAddBulk(b *testing.B) {
	ctx := context.Background()
	q := benchQueue(b, "bulk")

	const batch = 1000

	jobs := make([]bullmq.BulkJob, batch)
	for i := range jobs {
		jobs[i] = bullmq.BulkJob{Name: "bench", Data: benchPayload}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for added := 0; added < b.N; added += batch {
		if _, err := q.AddBulk(ctx, jobs[:min(batch, b.N-added)]); err != nil {
			b.Fatal(err)
		}
	}

	reportJobsPerSec(b, b.N)
}

// BenchmarkProcess measures end-to-end processing throughput of pre-added
// jobs with an empty processor (Rust: benchmark_processing). It is bound by
// the Redis CPU spent in the Lua scripts.
func BenchmarkProcess(b *testing.B) {
	for _, concurrency := range []int{10, 100} {
		b.Run("concurrency="+strconv.Itoa(concurrency), func(b *testing.B) {
			benchmarkProcess(b, concurrency, 0)
		})
	}
}

// BenchmarkProcessIOBound processes jobs whose processor waits 10ms (an API
// or database call), the workload high concurrency is meant for. The ideal
// rate is concurrency / 10ms.
func BenchmarkProcessIOBound(b *testing.B) {
	for _, concurrency := range []int{100, 500} {
		b.Run("concurrency="+strconv.Itoa(concurrency), func(b *testing.B) {
			benchmarkProcess(b, concurrency, 10*time.Millisecond)
		})
	}
}

func benchmarkProcess(b *testing.B, concurrency int, work time.Duration) {
	ctx := context.Background()
	q := benchQueue(b, "proc")
	name := q.Name()

	jobs := make([]bullmq.BulkJob, b.N)
	for i := range jobs {
		jobs[i] = bullmq.BulkJob{Name: "bench", Data: benchPayload}
	}
	for start := 0; start < len(jobs); start += 1000 {
		if _, err := q.AddBulk(ctx, jobs[start:min(start+1000, len(jobs))]); err != nil {
			b.Fatal(err)
		}
	}

	var (
		mu   sync.Mutex
		done int
		all  = make(chan struct{})
	)
	b.ResetTimer()
	w := newTestWorker(b, name, func(context.Context, *bullmq.Job) (any, error) {
		if work > 0 {
			time.Sleep(work)
		}

		mu.Lock()
		done++
		if done == b.N {
			close(all)
		}
		mu.Unlock()
		return nil, nil
	}, &bullmq.WorkerOptions{Concurrency: concurrency, RemoveOnComplete: bullmq.RemoveAll()})

	select {
	case <-all:
	case <-time.After(5 * time.Minute):
		b.Fatal("timeout waiting for jobs to be processed")
	}
	b.StopTimer()
	reportJobsPerSec(b, b.N)
	_ = w.Close(10 * time.Second)
}
