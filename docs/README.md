# Tài liệu bullmq (Go)

Hàng đợi job dùng Redis, tương thích với BullMQ của Node.js, Python và Rust:
cùng Lua script, cùng cấu trúc key trong Redis. Producer viết bằng Node.js có
thể đẩy job cho worker viết bằng Go, và ngược lại.

## Đọc theo thứ tự

| # | Tài liệu | Nội dung |
| --- | --- | --- |
| 1 | [Bắt đầu nhanh](01-bat-dau.md) | Cài đặt, chạy queue + worker đầu tiên, vòng đời của một job |
| 2 | [Kết nối Redis](02-ket-noi-redis.md) | URL, mật khẩu, DB, TLS, Cluster, dùng lại client có sẵn, yêu cầu với Redis |
| 3 | [Queue và Job](03-queue-va-job.md) | Thêm job, mọi tùy chọn của job, đọc trạng thái, quản lý queue |
| 4 | [Worker](04-worker.md) | Viết processor, retry/backoff, lỗi, concurrency, rate limit, tắt an toàn |
| 5 | [Flow (job cha – con)](05-flow.md) | Cây job phụ thuộc nhau, đọc kết quả của job con |
| 6 | [Job định kỳ](06-job-scheduler.md) | Cron, lặp theo chu kỳ, múi giờ |
| 7 | [Theo dõi sự kiện](07-su-kien.md) | Sự kiện của Worker và QueueEvents (xuyên tiến trình) |
| 8 | [Vận hành production](08-van-hanh.md) | Triển khai, giám sát, mở rộng, chạy chung với Node.js, xử lý sự cố |
| 9 | [Tham chiếu lỗi](09-loi.md) | Danh sách lỗi, mã lỗi Lua, thông báo kiểm tra tham số |

Tài liệu API chi tiết của từng hàm: `go doc -all bullmq` hoặc pkg.go.dev sau
khi publish. Chương trình mẫu chạy được nằm ở [`examples/`](../examples).

## Bảng tra nhanh

| Muốn làm gì | Dùng |
| --- | --- |
| Đẩy một job | `q.Add(ctx, "ten", data, opts)` |
| Đẩy nhiều job một lần | `q.AddBulk(ctx, []bullmq.BulkJob{...})` |
| Chạy sau 10 phút | `JobOptions{Delay: 10 * time.Minute}` |
| Thử lại 5 lần, chờ tăng dần | `JobOptions{Attempts: 5, Backoff: bullmq.ExponentialBackoff(time.Second)}` |
| Ưu tiên cao hơn | `JobOptions{Priority: 1}` (số nhỏ chạy trước) |
| Không cho trùng job | `JobOptions{JobID: "order-123"}` hoặc `Deduplication` |
| Không giữ job đã xong | `JobOptions{RemoveOnComplete: bullmq.RemoveAll()}` |
| Xử lý song song 50 job | `WorkerOptions{Concurrency: 50}` |
| Tối đa 100 job/giây | `WorkerOptions{Limiter: &bullmq.RateLimiterOptions{Max: 100, Duration: time.Second}}` |
| Lỗi không cần retry | `return nil, bullmq.NewUnrecoverableError("...")` |
| Chạy mỗi sáng 8h | `q.UpsertJobScheduler(ctx, "id", bullmq.RepeatOptions{Pattern: "0 8 * * *", TZ: "Asia/Ho_Chi_Minh"}, ...)` |
| Job cha chờ job con | `FlowProducer.Add` |
| Biết job xong ở tiến trình khác | `QueueEvents` |
| Tạm dừng toàn bộ queue | `q.Pause(ctx)` / `q.Resume(ctx)` |
| Chạy lại các job lỗi | `q.RetryJobs(ctx, "failed", 1000, time.Time{})` |
| Tắt worker an toàn | `w.Close(30 * time.Second)` |
