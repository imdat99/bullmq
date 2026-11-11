// Command worker processes the "emails" queue until interrupted (Ctrl+C).
//
//	REDIS_URL=redis://127.0.0.1:6379 go run ./examples/worker
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/imdat99/bullmq"
)

type email struct {
	To string `json:"to"`
}

func process(ctx context.Context, job *bullmq.Job) (any, error) {
	var in email
	if err := job.DecodeData(&in); err != nil {
		return nil, bullmq.NewUnrecoverableError("bad payload: " + err.Error()) // no retries
	}

	select { // stand-in for real I/O; honor ctx so Close can cancel the job
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if err := job.UpdateProgress(ctx, 100); err != nil {
		return nil, err
	}

	return map[string]any{"sent": true, "to": in.To}, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx)

	stop()

	if err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	w, err := bullmq.NewWorker(ctx, "emails", process, &bullmq.WorkerOptions{
		Connection:  bullmq.RedisConnectionOptions{URL: os.Getenv("REDIS_URL")},
		Concurrency: 10,
	})
	if err != nil {
		return err
	}

	go func() {
		for {
			e, ok := w.NextEvent(ctx)
			if !ok {
				return
			}

			log.Printf("%s job=%s err=%v", e.Type, e.JobID, e.Err)
		}
	}()

	<-ctx.Done()
	log.Print("shutting down")

	// Waits up to 30s for active jobs, then cancels their ctx.
	return w.Close(30 * time.Second)
}
