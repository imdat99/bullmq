// Command producer adds a few jobs to the "emails" queue.
//
//	REDIS_URL=redis://127.0.0.1:6379 go run ./examples/producer
package main

import (
	"bullmq"
	"context"
	"errors"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) (err error) {
	q, err := bullmq.NewQueue(ctx, "emails", &bullmq.QueueOptions{
		Connection: bullmq.RedisConnectionOptions{URL: os.Getenv("REDIS_URL")},
	})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, q.Close()) }()

	opts := &bullmq.JobOptions{
		Attempts:         3,
		Backoff:          bullmq.ExponentialBackoff(time.Second),
		RemoveOnComplete: bullmq.KeepLast(100),
	}

	for i := range 5 {
		job, err := q.Add(ctx, "welcome", map[string]string{"to": "user" + strconv.Itoa(i) + "@example.com"}, opts)
		if err != nil {
			return err
		}

		log.Printf("added job %s", job.ID)
	}

	counts, err := q.GetJobCounts(ctx)
	if err != nil {
		return err
	}

	log.Printf("waiting=%d active=%d completed=%d", counts.Waiting, counts.Active, counts.Completed)

	return nil
}
