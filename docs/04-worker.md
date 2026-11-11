# 4. Worker

## Tạo worker

```go
w, err := bullmq.NewWorker(ctx, "emails", process, &bullmq.WorkerOptions{
	Connection:  bullmq.RedisConnectionOptions{URL: os.Getenv("REDIS_URL")},
	Concurrency: 20,
})
if err != nil {
	return err
}
defer w.Close(30 * time.Second)
```

Worker tự chạy nền ngay sau `NewWorker`. Muốn tạo trước rồi chạy sau, đặt
`ManualStart: true` rồi gọi `w.Run()`.

`ctx` truyền vào `NewWorker` chỉ dùng cho bước khởi tạo (kết nối Redis), **không**
điều khiển vòng đời worker. Muốn dừng worker thì gọi `w.Close`.

## Viết processor

```go
type Processor func(ctx context.Context, job *bullmq.Job) (any, error)
```

| Processor trả về | Kết quả |
| --- | --- |
| `(v, nil)` | Job `completed`, `v` được mã hóa JSON thành `ReturnValue` (`nil` → `null`) |
| `(nil, err)` | Lần chạy thất bại: retry nếu còn lượt, hết lượt thì `failed` với `FailedReason = err.Error()` |
| `(nil, bullmq.NewUnrecoverableError(msg))` | `failed` ngay, bỏ qua các lượt retry còn lại |
| `(nil, bullmq.NewRateLimitError(d))` | Job quay lại hàng đợi, không tính là thất bại, xem [Rate limit](#rate-limit) |
| `(nil, bullmq.ErrDelayed)` | Bạn đã tự gọi `job.MoveToDelayed`; worker không đụng vào job nữa |
| `(nil, bullmq.ErrWaitingChildren)` | Bạn đã tự gọi `job.MoveToWaitingChildren`; xem [Flow](05-flow.md) |
| panic | Được bắt lại, job lỗi như trả `err` (có stack trace); worker không chết |

Lỗi được bọc (`fmt.Errorf("...: %w", err)`) vẫn nhận ra đúng loại.

### Định tuyến theo tên job

Một queue có thể chứa nhiều loại job:

```go
func process(ctx context.Context, job *bullmq.Job) (any, error) {
	switch job.Name {
	case "welcome":
		return sendWelcome(ctx, job)
	case "reset-password":
		return sendReset(ctx, job)
	default:
		return nil, bullmq.NewUnrecoverableError("không biết job " + job.Name)
	}
}
```

### Tôn trọng `ctx`

`ctx` của processor bị hủy khi:

- `w.CancelJob(id)` hoặc `w.CancelAllJobs()` được gọi;
- `w.Close(timeout)` hết thời gian chờ.

Truyền `ctx` xuống mọi lệnh I/O (HTTP, DB…) để job dừng được:

```go
req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
resp, err := http.DefaultClient.Do(req)
if err != nil {
	return nil, err // ctx bị hủy → err là context.Canceled → lần chạy này thất bại
}
```

### Trong lúc xử lý

```go
job.UpdateProgress(ctx, 40)                            // số, hoặc bất kỳ giá trị JSON nào
job.UpdateProgress(ctx, map[string]int{"done": 4, "total": 10})
job.Log(ctx, "bắt đầu gửi tới SMTP")
job.UpdateData(ctx, newData)                           // lưu trạng thái trung gian (ví dụ bước đã xong)
fmt.Println(job.AttemptsMade)                          // số lần đã chạy trước lần này
```

`job.UpdateData` hữu ích cho job nhiều bước: lưu "đã xong bước 2", lần retry
sau đọc lại và bỏ qua các bước đã làm.

## Retry, backoff và lỗi

Số lần chạy do `JobOptions.Attempts` của job quyết định (không phải worker), xem
[Queue và Job](03-queue-va-job.md#thử-lại-attempts-backoff). Với `Attempts: 3`:

```
lần 1 lỗi → chờ backoff → lần 2 lỗi → chờ backoff → lần 3 lỗi → failed
```

Mỗi lần thất bại đều phát sự kiện `EventFailed`, kể cả khi còn retry.

### Lỗi không retry

```go
if errors.Is(err, ErrCardDeclined) {
	return nil, bullmq.NewUnrecoverableError("thẻ bị từ chối")
}
```

Hoặc quyết định trong lúc chạy rằng job này không nên retry nữa:

```go
job.Discard()          // lỗi trả về sau đó sẽ không retry
return nil, err
```

### Backoff tùy biến

```go
w, _ := bullmq.NewWorker(ctx, "webhooks", process, &bullmq.WorkerOptions{
	BackoffStrategy: func(attemptsMade int, backoffType string, err error, job *bullmq.Job) time.Duration {
		switch backoffType {
		case "jitter":
			base := time.Second << min(attemptsMade, 10)
			return base/2 + time.Duration(rand.Int64N(int64(base/2)))
		case "respect-429":
			var rl *RetryAfterError
			if errors.As(err, &rl) {
				return rl.After // chờ đúng thời gian dịch vụ yêu cầu
			}
			return time.Minute
		}
		return -1 // âm = không retry nữa, job failed
	},
})

// Bên producer:
q.Add(ctx, "call", data, &bullmq.JobOptions{Attempts: 8, Backoff: bullmq.CustomBackoff("jitter")})
```

`attemptsMade` là số thứ tự lần vừa thất bại (1 cho lần đầu). Trả `0` để chạy
lại ngay, giá trị âm để dừng retry.

### Khôi phục job bị "treo" (stalled)

Khi worker lấy job, nó giữ một **khóa** (lock) trong Redis có thời hạn
`LockDuration` (mặc định 30s) và tự gia hạn mỗi `LockRenewTime` (mặc định
`LockDuration/2`). Nếu tiến trình chết đột ngột (OOM, `kill -9`, mất điện),
khóa hết hạn và một worker khác (kiểm tra mỗi `StalledInterval`, mặc định 30s)
đưa job về `waiting` để chạy lại. Mỗi lần như vậy phát `EventStalled`.

- Quá `MaxStalledCount` lần (mặc định 1) thì job `failed` với lý do
  "job stalled more than allowable limit". Đặt `MaxStalledCount: bullmq.Ptr(3)`
  nếu môi trường hay khởi động lại pod.
- Vòng lặp CPU nặng không làm khóa hết hạn: việc gia hạn chạy ở goroutine riêng.
  Nhưng nếu tiến trình bị treo hẳn (GC pause rất dài, máy quá tải), tăng
  `LockDuration`.
- `MaxStartedAttempts: bullmq.Ptr(5)` đánh lỗi job đã bắt đầu chạy quá 5 lần
  (phòng job làm sập worker liên tục).

Đây là lý do processor cần **idempotent**: job có thể chạy lại sau khi worker
chết giữa chừng.

## Concurrency

```go
&bullmq.WorkerOptions{Concurrency: 50} // mặc định 1
w.SetConcurrency(100)                  // đổi khi đang chạy
```

Mỗi job chạy trong một goroutine riêng. Chọn giá trị theo loại công việc:

| Loại job | Gợi ý |
| --- | --- |
| Chủ yếu chờ I/O (HTTP, email, DB) | 20–500. Goroutine rẻ; giới hạn thật là dịch vụ phía sau |
| Nặng CPU (xử lý ảnh, nén) | ≈ số CPU (`runtime.NumCPU()`) |
| Phải chạy tuần tự | 1, và chỉ chạy 1 worker (hoặc `q.SetGlobalConcurrency(ctx, 1)`) |

Tăng concurrency không tăng thông lượng khi Redis đã bão hòa CPU (khoảng vài
nghìn job/giây trên một Redis), xem [Vận hành](08-van-hanh.md#hiệu-năng-và-mở-rộng).

Chạy nhiều worker (nhiều pod/tiến trình) trên cùng queue là cách mở rộng bình
thường; mỗi job chỉ được một worker nhận.

## Rate limit

### Giới hạn cố định

```go
&bullmq.WorkerOptions{
	Limiter: &bullmq.RateLimiterOptions{Max: 100, Duration: time.Second}, // 100 job/giây
}
```

Giới hạn này lưu trong Redis nên **áp chung cho mọi worker** của queue: 3 worker
cùng `Max: 100` vẫn chỉ được 100 job/giây tổng cộng.

### Giới hạn động (ví dụ API trả 429)

```go
func process(ctx context.Context, job *bullmq.Job) (any, error) {
	resp, err := callAPI(ctx, job)
	if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
		wait := retryAfter(resp) // ví dụ 30s
		if err := q.RateLimit(ctx, wait); err != nil { // chặn mọi worker trong 30s
			return nil, err
		}
		return nil, bullmq.NewRateLimitError(wait) // trả job về hàng đợi, không tính lượt
	}
	return resp, err
}
```

`MaximumRateLimitDelay` (mặc định 30s) giới hạn thời gian worker ngủ mỗi lần
bị rate limit trước khi kiểm tra lại.

## Tắt worker an toàn

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

w, _ := bullmq.NewWorker(context.Background(), "emails", process, opts)

<-ctx.Done()
if err := w.Close(25 * time.Second); err != nil {
	log.Println("close:", err)
}
```

`Close(timeout)`:

1. Ngừng lấy job mới.
2. Chờ tối đa `timeout` cho các job đang chạy.
3. Hết giờ: hủy `ctx` của các job còn chạy. Job bị hủy sẽ được khôi phục bởi cơ
   chế stalled ở trên và chạy lại sau.
4. Đóng kết nối.

Trên Kubernetes, đặt `timeout` nhỏ hơn `terminationGracePeriodSeconds`
(mặc định 30s) vài giây.

`w.Pause()` / `w.Resume()` tạm dừng chỉ worker này (khác `q.Pause` dừng mọi
worker).

## Sự kiện của worker

```go
go func() {
	for {
		e, ok := w.NextEvent(context.Background())
		if !ok {
			return // worker đã đóng
		}
		switch e.Type {
		case bullmq.EventCompleted:
			log.Printf("job %s xong: %s", e.JobID, e.Result)
		case bullmq.EventFailed:
			log.Printf("job %s lỗi: %v", e.JobID, e.Err)
		case bullmq.EventError:
			log.Printf("lỗi worker (Redis...): %v", e.Err)
		}
	}
}()
```

Không bắt buộc phải đọc sự kiện: bộ đệm giới hạn 100 000 sự kiện, đầy thì bỏ sự
kiện cũ nhất, không tốn thêm bộ nhớ. Danh sách đầy đủ ở
[Theo dõi sự kiện](07-su-kien.md).

## Tất cả tùy chọn `WorkerOptions`

| Trường | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `Connection` | `127.0.0.1:6379` | Xem [Kết nối Redis](02-ket-noi-redis.md) |
| `Prefix` | `"bull"` | Phải trùng prefix của producer |
| `Name` | rỗng | Tên worker, lưu vào `job.ProcessedBy` và tên kết nối Redis |
| `Concurrency` | 1 | Số job chạy song song |
| `LockDuration` | 30s | Thời hạn khóa job |
| `LockRenewTime` | `LockDuration/2` | Chu kỳ gia hạn khóa |
| `StalledInterval` | 30s | Chu kỳ kiểm tra job treo |
| `MaxStalledCount` | 1 | Số lần job được khôi phục trước khi `failed` |
| `MaxStartedAttempts` | nil (tắt) | Đánh lỗi job đã bắt đầu quá N lần |
| `SkipStalledCheck` | false | Worker này không kiểm tra job treo |
| `SkipLockRenewal` | false | Không gia hạn khóa (chỉ cho job rất ngắn) |
| `DrainDelay` | 5s | Thời gian chặn chờ job khi queue rỗng |
| `RunRetryDelay` | 15s | Nghỉ sau lỗi Redis trước khi thử lại |
| `RemoveOnComplete`, `RemoveOnFail` | giữ tất cả | Chính sách giữ job mặc định |
| `BackoffStrategy` | nil | Hàm backoff tùy biến |
| `Limiter` | nil | Rate limit |
| `MaximumRateLimitDelay` | 30s | Thời gian ngủ tối đa khi bị rate limit |
| `Metrics` | nil | Bật thống kê theo phút: `&bullmq.MetricsOptions{}` |
| `ManualStart` | false | Không tự chạy, gọi `w.Run()` |

## Xử lý thủ công (không dùng processor)

Dành cho trường hợp đặc biệt cần tự điều khiển việc lấy job (ví dụ gom lô):

```go
// Processor không được nil, nhưng sẽ không bao giờ được gọi vì không gọi w.Run().
unused := func(context.Context, *bullmq.Job) (any, error) { return nil, nil }
w, _ := bullmq.NewWorker(ctx, "emails", unused, &bullmq.WorkerOptions{ManualStart: true})
```

```go
token := uuid.NewString() // mỗi worker một token
for {
	job, err := w.GetNextJob(ctx, token) // chặn tối đa DrainDelay; (nil, nil) nếu không có job
	if err != nil || job == nil {
		continue
	}
	if err := handle(ctx, job); err != nil {
		job.MoveToFailed(ctx, err.Error())
	} else {
		job.MoveToCompleted(ctx, map[string]bool{"ok": true})
	}
}
```

Ở chế độ này **không có** gia hạn khóa tự động: xử lý xong trong `LockDuration`,
hoặc tự gọi `job.ExtendLock(ctx, token, d)`.

Tiếp theo: [Flow](05-flow.md).
