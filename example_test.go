package bullmq_test

import (
	"bullmq"
	"context"
	"fmt"
	"log"
	"time"
)

// Examples compile with the tests but do not run (they need Redis).

func ExampleNewQueue() {
	ctx := context.Background()

	q, err := bullmq.NewQueue(ctx, "emails", &bullmq.QueueOptions{
		Connection: bullmq.RedisConnectionOptions{URL: "redis://127.0.0.1:6379"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	job, err := q.Add(ctx, "welcome", map[string]string{"to": "a@example.com"}, &bullmq.JobOptions{
		Attempts: 3,
		Backoff:  bullmq.ExponentialBackoff(time.Second),
		Delay:    5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("added", job.ID)
}

func ExampleNewWorker() {
	ctx := context.Background()

	w, err := bullmq.NewWorker(ctx, "emails", func(ctx context.Context, job *bullmq.Job) (any, error) {
		var in struct{ To string }
		if err := job.DecodeData(&in); err != nil {
			return nil, bullmq.NewUnrecoverableError(err.Error()) // fail without retries
		}

		return map[string]any{"sent": true}, nil // stored as the job return value
	}, &bullmq.WorkerOptions{Concurrency: 10})
	if err != nil {
		log.Fatal(err)
	}

	// Waits up to 30s for active jobs, then cancels their ctx.
	defer w.Close(30 * time.Second)
}

func ExampleFlowProducer_Add() {
	ctx := context.Background()

	fp, err := bullmq.NewFlowProducer(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()

	// The parent runs once all its children have completed.
	node, err := fp.Add(ctx, bullmq.FlowJob{
		Name: "render", QueueName: "videos",
		Children: []bullmq.FlowJob{
			{Name: "transcode", QueueName: "transcode", Data: map[string]int{"res": 1080}},
			{Name: "thumbnail", QueueName: "images"},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("parent", node.Job.ID)
}

func ExampleQueue_UpsertJobScheduler() {
	ctx := context.Background()

	q, err := bullmq.NewQueue(ctx, "reports", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	// Every day at 08:00 Asia/Ho_Chi_Minh; upserting again updates it.
	_, err = q.UpsertJobScheduler(ctx, "daily-report",
		bullmq.RepeatOptions{Pattern: "0 8 * * *", TZ: "Asia/Ho_Chi_Minh"},
		"report", nil, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleNewQueueEvents() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	events, err := bullmq.NewQueueEvents(ctx, "emails", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer events.Close()

	for {
		e, ok := events.NextEvent(ctx)
		if !ok {
			return
		}

		fmt.Println(e.Event.Name(), e.Event.JobID)
	}
}
