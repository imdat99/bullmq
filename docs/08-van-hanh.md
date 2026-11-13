# 8. Vận hành production

## Cấu trúc service gợi ý

```go
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := bullmq.NewRedisConnection(ctx, bullmq.RedisConnectionOptions{URL: os.Getenv("REDIS_URL")})
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Producer: một Queue cho mỗi tên queue, dùng chung cả ứng dụng.
	emails, err := bullmq.NewQueueWithConnection(ctx, "emails", conn, &bullmq.QueueOptions{
		DefaultJobOptions: bullmq.JobOptions{
			Attempts:         5,
			Backoff:          bullmq.ExponentialBackoff(2 * time.Second),
			RemoveOnComplete: bullmq.KeepLast(1000),
			RemoveOnFail:     bullmq.KeepJobsPolicy(bullmq.KeepJobs{Age: bullmq.Ptr[int64](7 * 24 * 3600)}),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// Worker
	w, err := bullmq.NewWorkerWithConnection(ctx, "emails", processEmail, conn, &bullmq.WorkerOptions{
		Concurrency: 50,
		Name:        os.Getenv("HOSTNAME"), // biết job do pod nào xử lý
		Metrics:     &bullmq.MetricsOptions{},
	})
	if err != nil {
		log.Fatal(err)
	}
	go logWorkerEvents(w)

	// HTTP: /metrics, /healthz
	srv := startHTTP(emails)

	<-ctx.Done()
	_ = srv.Shutdown(context.Background())
	_ = w.Close(25 * time.Second) // nhỏ hơn terminationGracePeriodSeconds
	_ = emails.Close()
}
```

Có thể tách producer (API) và worker thành hai deployment để mở rộng độc lập.

## Checklist trước khi lên production

- [ ] Redis `maxmemory-policy noeviction`, có đặt `maxmemory`.
- [ ] Redis có persistence (AOF `appendfsync everysec` hoặc RDB) nếu không chấp
      nhận mất job khi Redis khởi động lại.
- [ ] Mọi queue có `RemoveOnComplete` / `RemoveOnFail`, nếu không Redis phình mãi.
- [ ] Đặt `Attempts` + `Backoff` cho job gọi dịch vụ bên ngoài.
- [ ] Processor idempotent, tôn trọng `ctx`.
- [ ] Phân biệt lỗi tạm thời (trả `err`) và lỗi vĩnh viễn (`NewUnrecoverableError`).
- [ ] Bắt SIGTERM và gọi `w.Close(timeout)` với timeout < grace period.
- [ ] Đặt `GOMEMLIMIT` khoảng 80–90% giới hạn bộ nhớ container.
- [ ] Có cảnh báo khi số job `failed` hoặc `waiting` tăng bất thường.
- [ ] Môi trường khác nhau (dev/staging/prod) dùng Redis hoặc prefix khác nhau.

## Giám sát

### Prometheus

```go
http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
	text, err := emails.ExportPrometheusMetrics(r.Context(),
		bullmq.PrometheusLabel{Name: "env", Value: "prod"})
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = io.WriteString(w, text)
})
```

Xuất số job theo trạng thái của queue. Đặt endpoint này ở **một** chỗ (ví dụ
service API), không cần ở mọi pod worker. Đã dùng client Prometheus của Go thì
có thể đẩy số liệu sang gauge riêng bằng `q.RecordJobCountsMetric`.

Cảnh báo gợi ý:

| Chỉ số | Gợi ý cảnh báo |
| --- | --- |
| `waiting` tăng liên tục | Worker không theo kịp hoặc đã chết |
| `failed` tăng nhanh | Lỗi hàng loạt (dịch vụ phía sau sập, bug) |
| `active` = 0 nhưng `waiting` > 0 | Không có worker, hoặc queue đang pause |
| `q.GetWorkersCount` = 0 | Không có worker kết nối |

### Thống kê theo phút

Bật `WorkerOptions.Metrics: &bullmq.MetricsOptions{}` (mặc định giữ 2 tuần), rồi:

```go
m, err := q.GetMetrics(ctx, "completed", 0, 59) // 60 phút gần nhất, mới nhất trước
fmt.Println(m.Data, m.Meta.Count)
```

### Health check

```go
http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
	if err := conn.Ping(r.Context()); err != nil || !worker.IsRunning() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
})
```

### Log

Thư viện ghi log lỗi nội bộ (mất kết nối, không gia hạn được khóa…) qua
`log/slog` mặc định. Đặt handler JSON cho cả ứng dụng:

```go
slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
```

Đồng thời nên đọc `EventError` của worker và ghi log hoặc đếm metric.

### Dashboard

Vì dùng chung cấu trúc dữ liệu với Node.js BullMQ, các công cụ như
**Bull Board** hoặc **Taskforce.sh** đọc trực tiếp được queue do thư viện này
tạo: xem job, retry, xóa… Chạy chúng như một service Node.js nhỏ trỏ tới cùng
Redis và cùng prefix.

## Hiệu năng và mở rộng

Đo trên máy 6 vCPU (Xeon E5-4669 v3), một Redis 7:

| Thao tác | Thông lượng |
| --- | --- |
| `Add` tuần tự, một goroutine | ~1 600 job/s |
| `Add` từ nhiều goroutine | ~8 500 job/s |
| `AddBulk` | ~10 700 job/s |
| Worker xử lý job rỗng, concurrency 10–100 | ~4 000–4 400 job/s |

Giới hạn nằm ở **CPU của Redis** (mỗi job chạy vài Lua script, ~150 µs), không
phải ở Go (dùng dưới 1 core). Vì vậy:

- Tăng `Concurrency` giúp khi job chờ I/O lâu; với job rất ngắn, vượt quá
  khoảng 10–50 không tăng thêm.
- Thêm worker/pod giúp khi job tốn thời gian xử lý, không giúp khi Redis đã
  100% CPU.
- Producer đẩy nhiều job: dùng `AddBulk` hoặc gọi `Add` từ nhiều goroutine.
- Vượt giới hạn một Redis: chia việc ra **nhiều queue** đặt trên các shard khác
  nhau của Redis Cluster (hash tag theo tên queue: `{emails}`, `{sms}`), hoặc
  dùng Redis trên CPU nhanh hơn.

Bộ nhớ phía Go: khoảng 30 lần cấp phát (~3–3.7 KB) mỗi job xử lý, GC nhẹ. Bộ nhớ phía Redis:
mỗi job khoảng 1–2 KB + kích thước dữ liệu; tính dung lượng theo số job **giữ
lại** (`RemoveOnComplete`), không theo tổng số job đã chạy.

## Chạy chung với Node.js / Python / Rust BullMQ

Hoạt động trên cùng queue, theo bất kỳ chiều nào, miễn là:

- Cùng Redis, cùng `Prefix` (mặc định `bull` ở mọi thư viện) và cùng tên queue.
- Dữ liệu là JSON. Đặt tag `json` trên struct Go cho khớp tên trường bên kia
  (Node.js thường dùng camelCase).
- Thời gian trong tùy chọn: thư viện Go nhận `time.Duration` và tự đổi sang
  mili giây như Node.js.
- `ReturnValue` của job do worker Node.js xử lý cũng là JSON: dùng
  `job.DecodeReturnValue`.

```ts
// Node.js producer
await new Queue('emails', { connection }).add('welcome', { to: 'a@example.com' }, { attempts: 3 })
```

```go
// Go worker: nhận job ở trên
type Welcome struct{ To string `json:"to"` }
```

Khác biệt nhỏ: trường `library` trong meta của queue ghi `bullmq-go:0.1.0`.

## Xử lý sự cố

| Hiện tượng | Nguyên nhân thường gặp | Cách xử lý |
| --- | --- | --- |
| Job nằm mãi ở `waiting` | Không có worker; worker khác **prefix**/tên queue; queue bị pause | `q.GetWorkersCount`, `q.IsPaused`, so prefix hai bên |
| Job nằm mãi ở `delayed` | Đang chờ backoff hoặc `Delay`; đồng hồ máy lệch | `job.Opts.Delay`, `job.AttemptsMade`; `job.Promote(ctx)` để chạy ngay |
| Job nằm mãi ở `waiting-children` | Một job con `failed` mà không đặt tùy chọn khi lỗi | Xem [Flow](05-flow.md#khi-job-con-thất-bại); retry job con |
| Job chạy hai lần | Retry, hoặc worker chết/treo quá `LockDuration` → stalled | Processor idempotent; tăng `LockDuration` nếu job chặn tiến trình lâu |
| `job stalled more than allowable limit` | Worker liên tục bị kill khi đang chạy job (OOM, deploy) | Tìm nguyên nhân pod chết; tăng `MaxStalledCount`; `Close` đúng cách khi deploy |
| `OOM command not allowed` | Redis đầy bộ nhớ | Đặt `RemoveOnComplete`/`RemoveOnFail`, `q.Clean`, tăng `maxmemory` |
| Job biến mất | `maxmemory-policy` không phải `noeviction`; hoặc `RemoveOnComplete` | Kiểm tra `config get maxmemory-policy` |
| `CROSSSLOT` trên Cluster | Key của queue khác slot | Thêm hash tag vào prefix hoặc tên queue |
| `Missing lock for job` | Job bị khôi phục (stalled) trong lúc vẫn đang chạy ở worker cũ | Như "job chạy hai lần" |
| Worker không thoát khi tắt | Processor bỏ qua `ctx` | Truyền `ctx` xuống mọi I/O |
| Kết quả `Add` thiếu job sau lỗi mạng | Lỗi trả về nhưng lệnh có thể đã chạy | Dùng `JobID` cố định rồi thử lại |

Xem trực tiếp trong Redis:

```bash
redis-cli --scan --pattern 'bull:emails:*' | head
redis-cli hgetall bull:emails:42                # một job
redis-cli zrange bull:emails:failed 0 9 rev     # 10 job lỗi mới nhất
redis-cli xrevrange bull:emails:events + - count 20
```

## Kiểm thử ứng dụng dùng thư viện

Không cần mock: chạy Redis thật trong Docker (testcontainers hoặc service của
CI), mỗi test một tên queue ngẫu nhiên, dọn bằng `q.Obliterate(ctx, true, 0)`.

```go
func newTestQueue(t *testing.T) *bullmq.Queue {
	t.Helper()
	q, err := bullmq.NewQueue(t.Context(), "test-"+uuid.NewString(), &bullmq.QueueOptions{
		Connection: bullmq.RedisConnectionOptions{URL: os.Getenv("REDIS_URL")},
	})
	if err != nil {
		t.Skipf("không có Redis: %v", err)
	}
	t.Cleanup(func() {
		_ = q.Obliterate(context.Background(), true, 0)
		_ = q.Close()
	})
	return q
}
```

Để kiểm thử logic nghiệp vụ, gọi thẳng hàm processor với một job dựng từ queue
test (`q.Add` rồi `q.GetJob`), không cần chạy worker.
