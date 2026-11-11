# 1. Bắt đầu nhanh

## Yêu cầu

- Go 1.26 trở lên.
- Redis 6.2 trở lên (khuyên dùng 7.x), cấu hình `maxmemory-policy noeviction`.
  Xem [Kết nối Redis](02-ket-noi-redis.md#yêu-cầu-với-redis).

Chưa có Redis để thử? Chạy một Redis tạm, giới hạn bộ nhớ, không ghi xuống đĩa:

```bash
docker run -d --rm --name redis-dev -p 127.0.0.1:6379:6379 \
  redis:7-alpine redis-server --maxmemory 512mb --maxmemory-policy noeviction --save '' --appendonly no
```

## Cài đặt

Khi thư viện đã nằm trên Git (ví dụ `gitlab.company.vn/backend/bullmq-go`):

```bash
go get gitlab.company.vn/backend/bullmq-go@latest
```

Repo private thì đặt thêm `GOPRIVATE=gitlab.company.vn/*`.

Khi đang phát triển trên cùng máy, trỏ thẳng tới thư mục mã nguồn:

```bash
go mod edit -require=bullmq@v0.0.0 -replace=bullmq=/duong/dan/toi/bullMQ_go
go mod tidy
```

Trong code, tên package luôn là `bullmq`:

```go
import "bullmq" // hoặc "gitlab.company.vn/backend/bullmq-go"
```

## Ví dụ đầu tiên

Hệ thống gồm hai phần, thường chạy ở hai tiến trình (hai service) khác nhau:

- **Producer** dùng `Queue` để đẩy job vào Redis.
- **Worker** lấy job từ Redis và gọi hàm xử lý (processor) của bạn.

### Producer

```go
package main

import (
	"context"
	"log"
	"time"

	"bullmq"
)

type WelcomeEmail struct {
	To   string `json:"to"`
	Name string `json:"name"`
}

func main() {
	ctx := context.Background()

	q, err := bullmq.NewQueue(ctx, "emails", &bullmq.QueueOptions{
		Connection: bullmq.RedisConnectionOptions{URL: "redis://127.0.0.1:6379"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	job, err := q.Add(ctx, "welcome", WelcomeEmail{To: "an@example.com", Name: "An"}, &bullmq.JobOptions{
		Attempts:         3,                                      // tổng cộng tối đa 3 lần chạy
		Backoff:          bullmq.ExponentialBackoff(time.Second), // chờ 1s, 2s giữa các lần
		RemoveOnComplete: bullmq.KeepLast(1000),                  // chỉ giữ 1000 job xong gần nhất
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("đã thêm job", job.ID)
}
```

`data` có thể là bất kỳ giá trị nào `encoding/json` mã hóa được: struct, map,
slice, chuỗi… Nếu đã có sẵn JSON, truyền `json.RawMessage` để giữ nguyên.

### Worker

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"bullmq"
)

type WelcomeEmail struct {
	To   string `json:"to"`
	Name string `json:"name"`
}

func process(ctx context.Context, job *bullmq.Job) (any, error) {
	var in WelcomeEmail
	if err := job.DecodeData(&in); err != nil {
		// Dữ liệu hỏng thì thử lại cũng vô ích: đánh lỗi luôn, không retry.
		return nil, bullmq.NewUnrecoverableError("dữ liệu không hợp lệ: " + err.Error())
	}

	if err := sendEmail(ctx, in.To, in.Name); err != nil {
		return nil, err // lỗi thường: sẽ retry nếu còn lượt (Attempts)
	}

	return map[string]any{"sent": true}, nil // lưu thành returnvalue của job
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	w, err := bullmq.NewWorker(ctx, "emails", process, &bullmq.WorkerOptions{
		Connection:  bullmq.RedisConnectionOptions{URL: "redis://127.0.0.1:6379"},
		Concurrency: 10, // xử lý song song tối đa 10 job
	})
	if err != nil {
		log.Fatal(err)
	}

	<-ctx.Done() // chờ Ctrl+C hoặc SIGTERM

	// Ngừng lấy job mới, chờ tối đa 30s cho các job đang chạy.
	if err := w.Close(30 * time.Second); err != nil {
		log.Println(err)
	}
}

func sendEmail(ctx context.Context, to, name string) error { return nil }
```

Worker **bắt đầu chạy ngay khi được tạo** (trừ khi đặt `ManualStart: true`).
Không cần vòng lặp nào: `NewWorker` tự chạy nền và gọi `process` cho mỗi job.

Chạy thử với hai chương trình mẫu có sẵn trong repo:

```bash
go run ./examples/worker     # terminal 1
go run ./examples/producer   # terminal 2
```

## Vòng đời của một job

```
                 Add (Delay > 0)
   Add ──────────────┬──────────────► delayed ──(hết giờ)──┐
    │                │                                     │
    │ (Priority > 0) └──► prioritized ──┐                  │
    ▼                                   ▼                  ▼
  waiting ─────────────────────────► active ◄──────────── waiting
                                       │
              ┌────────────────────────┼─────────────────────────┐
              ▼                        ▼                         ▼
          completed           lỗi, còn lượt retry             failed
                              → delayed (có backoff)        (hết lượt hoặc
                              → waiting (không backoff)      lỗi unrecoverable)

  Job cha trong flow: waiting-children ──(mọi job con xong)──► waiting
```

| Trạng thái | Hằng số | Ý nghĩa |
| --- | --- | --- |
| `waiting` | `StateWaiting` | Đang xếp hàng, worker sẽ lấy theo thứ tự FIFO (hoặc LIFO) |
| `prioritized` | `StatePrioritized` | Đang xếp hàng theo độ ưu tiên |
| `delayed` | `StateDelayed` | Chờ đến giờ (Delay, hoặc chờ retry có backoff) |
| `active` | `StateActive` | Đang có worker xử lý (worker giữ khóa – lock) |
| `waiting-children` | `StateWaitingChildren` | Job cha đang chờ các job con |
| `completed` | `StateCompleted` | Xong, có `ReturnValue` |
| `failed` | `StateFailed` | Thất bại, có `FailedReason` |

## Những điều cần nhớ

- Mọi hàm gọi Redis nhận `ctx` ở tham số đầu. Hủy `ctx` là hủy lệnh đang chờ.
- `Queue`, `Worker`, `FlowProducer`, `QueueEvents` đều **an toàn khi dùng từ
  nhiều goroutine**. Hãy tạo **một** `Queue` cho mỗi tên queue rồi dùng lại
  suốt vòng đời ứng dụng, đừng tạo mới cho mỗi request.
- Luôn `Close()` khi tắt ứng dụng. Với worker, `Close` chờ job đang chạy.
- Một job có thể chạy **nhiều hơn một lần** (retry, hoặc worker chết giữa
  chừng rồi job được khôi phục). Hãy viết processor **idempotent**: chạy lại
  không gây hậu quả kép (ví dụ kiểm tra "đã gửi email chưa" trước khi gửi).

Tiếp theo: [Kết nối Redis](02-ket-noi-redis.md).
