# 3. Queue và Job

## Tạo queue

```go
q, err := bullmq.NewQueue(ctx, "emails", &bullmq.QueueOptions{
	Connection: bullmq.RedisConnectionOptions{URL: os.Getenv("REDIS_URL")},
	Prefix:     "",                // mặc định "bull"
	DefaultJobOptions: bullmq.JobOptions{ // áp cho mọi job thêm vào queue này
		Attempts:         3,
		Backoff:          bullmq.ExponentialBackoff(2 * time.Second),
		RemoveOnComplete: bullmq.KeepLast(1000),
		RemoveOnFail:     bullmq.KeepLast(5000),
	},
})
if err != nil {
	return err
}
defer q.Close()
```

Tạo một `Queue` cho mỗi tên queue khi khởi động ứng dụng và dùng lại; `Queue`
an toàn khi gọi từ nhiều goroutine.

`DefaultJobOptions` chỉ áp các trường `Attempts`, `Backoff`, `RemoveOnComplete`,
`RemoveOnFail`, `Delay`, `Priority`, `LIFO`. Trường nào job tự đặt thì giá trị
của job được ưu tiên.

## Thêm job

```go
job, err := q.Add(ctx, "welcome", payload, opts) // opts có thể nil
```

- `name`: tên loại job. Một queue có thể chứa nhiều loại job; worker phân biệt
  bằng `job.Name`.
- `payload`: mã hóa JSON. Dùng `json.RawMessage` nếu đã có JSON sẵn.
- Kết quả là `*Job` đã gắn với queue, gọi tiếp được `job.GetState(ctx)`…

### Thêm nhiều job

```go
jobs, err := q.AddBulk(ctx, []bullmq.BulkJob{
	{Name: "welcome", Data: u1},
	{Name: "welcome", Data: u2, Opts: &bullmq.JobOptions{Priority: 1}},
})
```

Gửi tất cả trong **một** round trip, nhanh hơn nhiều lần so với gọi `Add` trong
vòng lặp. Mọi job được kiểm tra trước khi gửi: một job sai tham số thì không job
nào được thêm. Tuy nhiên, việc thêm **không nguyên tử**: nếu Redis lỗi giữa
chừng, một số job có thể đã vào queue. Nên chia lô khoảng 100–1000 job.

## Tùy chọn của job (`JobOptions`)

Mọi trường đều tùy chọn; giá trị 0 nghĩa là "không đặt".

### Hẹn giờ: `Delay`

```go
q.Add(ctx, "reminder", data, &bullmq.JobOptions{Delay: 24 * time.Hour})
```

Job nằm ở `delayed`, đến giờ tự chuyển sang `waiting`. Muốn chạy vào một thời
điểm cụ thể: `Delay: time.Until(t)`.

Thay đổi sau khi đã thêm:

```go
job, _ := q.GetJob(ctx, id)
job.ChangeDelay(ctx, 5*time.Minute) // hẹn lại từ bây giờ
job.Promote(ctx)                    // chạy ngay
q.PromoteJobs(ctx, 1000)            // đưa mọi job delayed sang waiting
```

### Thứ tự: `Priority`, `LIFO`

```go
&bullmq.JobOptions{Priority: 1}  // từ 1 đến 2_097_152, số NHỎ chạy TRƯỚC
&bullmq.JobOptions{LIFO: true}   // chen lên đầu hàng đợi
```

- Không đặt `Priority` (0) thì job vào hàng FIFO thường, và **được lấy trước**
  các job có priority.
- Các job cùng priority chạy theo thứ tự thêm vào.
- Đổi sau khi thêm: `job.ChangePriority(ctx, 5, false)`.

Priority tốn thêm chi phí (sorted set); chỉ dùng khi thật sự cần.

### Thử lại: `Attempts`, `Backoff`

```go
&bullmq.JobOptions{
	Attempts: 5,                                      // TỔNG số lần chạy (không phải số lần retry)
	Backoff:  bullmq.ExponentialBackoff(time.Second), // chờ 1s, 2s, 4s, 8s
}
```

| Backoff | Thời gian chờ trước lần thử thứ n+1 |
| --- | --- |
| không đặt | 0: quay lại `waiting` ngay |
| `bullmq.FixedBackoff(d)` | luôn `d` |
| `bullmq.ExponentialBackoff(d)` | `d × 2^(n-1)`: d, 2d, 4d… |
| `bullmq.CustomBackoff("ten")` | do `WorkerOptions.BackoffStrategy` quyết định, xem [Worker](04-worker.md#backoff-tùy-biến) |

`Attempts: 0` hoặc `1`: chạy một lần, lỗi là `failed`.

### Giữ lại job đã xong: `RemoveOnComplete`, `RemoveOnFail`

Mặc định **giữ tất cả** job đã xong trong Redis. Với queue lớn, Redis sẽ phình
mãi. Luôn đặt chính sách giữ lại:

| Giá trị | Ý nghĩa |
| --- | --- |
| `nil` / `bullmq.KeepAll()` | Giữ tất cả (mặc định) |
| `bullmq.RemoveAll()` | Xóa ngay khi xong |
| `bullmq.KeepLast(1000)` | Giữ 1000 job gần nhất |
| `bullmq.KeepJobsPolicy(bullmq.KeepJobs{Age: bullmq.Ptr[int64](3600)})` | Giữ job xong trong vòng 3600 **giây** |
| `bullmq.KeepJobsPolicy(bullmq.KeepJobs{Age: bullmq.Ptr[int64](86400), Count: bullmq.Ptr[int64](5000)})` | Giữ tối đa 5000 job, không quá 1 ngày |

Khuyến nghị: `RemoveOnComplete: KeepLast(1000)` và giữ job lỗi lâu hơn để còn
điều tra (`RemoveOnFail: KeepJobsPolicy(KeepJobs{Age: Ptr[int64](7 * 86400)})`).

Có thể đặt ở ba nơi, ưu tiên từ cao xuống: `JobOptions` của job →
`QueueOptions.DefaultJobOptions` → `WorkerOptions.RemoveOnComplete/RemoveOnFail`.

### ID tự đặt và chống trùng: `JobID`

```go
q.Add(ctx, "charge", data, &bullmq.JobOptions{JobID: "order-12345"})
```

- Thêm lần nữa với cùng `JobID` khi job cũ **còn trong Redis**: không tạo job
  mới, `Add` trả về job có ID cũ (phát sự kiện `duplicated`). Đây là cách đơn
  giản nhất để retry `Add` an toàn.
- Khi job cũ đã bị xóa (do `RemoveOnComplete`…), cùng `JobID` lại thêm được.
- Ràng buộc: không chứa `:`, không là số nguyên (`"123"` bị từ chối, vì ID tự
  sinh là số), không là `"0"`.

### Chống trùng có điều kiện: `Deduplication`

Dùng khi muốn chống trùng theo một khóa nghiệp vụ, kể cả sau khi job đã xong.

```go
// 1. Đơn giản: trong lúc job "user-42" chưa xong, mọi job trùng bị bỏ qua.
&bullmq.JobOptions{Deduplication: &bullmq.DeduplicationOptions{ID: "sync-user-42"}}

// 2. Throttle: tối đa 1 job mỗi 10 phút cho cùng khóa.
&bullmq.JobOptions{Deduplication: &bullmq.DeduplicationOptions{ID: "sync-user-42", TTL: 10 * time.Minute}}

// 3. Debounce: gom nhiều sự kiện liên tiếp, chỉ chạy job CUỐI sau 5s yên lặng.
&bullmq.JobOptions{
	Delay: 5 * time.Second,
	Deduplication: &bullmq.DeduplicationOptions{
		ID: "reindex-product-7", TTL: 5 * time.Second,
		Extend: true, Replace: true,
	},
}
```

| Trường | Tác dụng |
| --- | --- |
| `TTL` | Thời gian khóa chống trùng tồn tại (0 = đến khi job xong) |
| `Extend` | Mỗi lần trùng thì gia hạn TTL |
| `Replace` | Job trùng **thay thế** job đang chờ (delayed) bằng dữ liệu mới |
| `Reschedule` | Job trùng hẹn lại giờ chạy của job đang chờ |
| `KeepLastIfActive` | Nếu job đang chạy, giữ bản trùng cuối cùng và thêm nó sau khi job chạy xong |

Khi bị chống trùng, `Add` trả về job với **ID của job đã có** (dữ liệu trong
`*Job` trả về là dữ liệu bạn vừa truyền, không phải dữ liệu đang lưu). Kiểm tra
và gỡ khóa:

```go
id, ok, err := q.GetDeduplicationJobID(ctx, "sync-user-42")
removed, err := q.RemoveDeduplicationKey(ctx, "sync-user-42", id)
```

### Các tùy chọn khác

| Trường | Ý nghĩa |
| --- | --- |
| `KeepLogs` | Số dòng log giữ lại cho mỗi job (0 = tất cả) |
| `StackTraceLimit` | Số stack trace lưu lại khi lỗi |
| `SizeLimit` | Từ chối job có JSON dữ liệu lớn hơn N byte |
| `Timestamp` | Thời điểm tạo logic (mặc định bây giờ) |
| `Parent` | Gắn job vào một job cha có sẵn, xem [Flow](05-flow.md) |
| `FailParentOnFailure`, `IgnoreDependencyOnFailure`, `RemoveDependencyOnFailure`, `ContinueParentOnFailure` | Hành vi của job con khi lỗi, xem [Flow](05-flow.md) |

`Repeat`, `RepeatJobKey`, `PrevMillis` do job scheduler tự đặt, đừng tự gán.

Giữ dữ liệu job nhỏ (vài KB). Dữ liệu lớn (file, ảnh) nên lưu ở nơi khác
(S3, DB) và chỉ đưa ID vào job.

## Đọc job

```go
job, err := q.GetJob(ctx, "42")       // (nil, nil) nếu không tồn tại
if job == nil { ... }

state, err := job.GetState(ctx)       // bullmq.StateCompleted, ...
state, err := q.GetJobState(ctx, "42") // StateUnknown nếu không tồn tại

var out MyResult
err = job.DecodeReturnValue(&out)     // kết quả processor trả về
fmt.Println(job.FailedReason, job.AttemptsMade)

var in MyData
err = job.DecodeData(&in)
```

Các trường hay dùng của `Job`: `ID`, `Name`, `Data`, `Opts`, `Progress`,
`AttemptsMade`, `Timestamp`/`ProcessedOn`/`FinishedOn` (Unix ms),
`FailedReason`, `ReturnValue`, `ProcessedBy`, `ParentKey`.

### Liệt kê và đếm

```go
counts, err := q.GetJobCounts(ctx)
// counts.Waiting, Active, Delayed, Prioritized, Completed, Failed, WaitingChildren, Paused

n, err := q.GetFailedCount(ctx)
failed, err := q.GetFailed(ctx, 0, 49)      // 50 job lỗi mới nhất (chỉ số bao gồm 2 đầu)
waiting, err := q.GetWaiting(ctx, 0, -1)    // -1 = đến cuối; cẩn thận với queue lớn
jobs, err := q.GetJobs(ctx, []string{"failed", "delayed"}, 0, 99, false)
ids, err := q.GetRanges(ctx, []string{"completed"}, 0, 99, false) // chỉ ID, nhẹ hơn
total, err := q.Count(ctx) // số job đang chờ xử lý
```

Tên trạng thái dùng trong các hàm nhận `[]string`: `"waiting"` (hoặc `"wait"`),
`"active"`, `"delayed"`, `"prioritized"`, `"completed"`, `"failed"`,
`"waiting-children"`, `"paused"`.

### Log và tiến độ của job

```go
job.Log(ctx, "đã tải 30/100 file")         // ghi log (thường gọi trong processor)
logs, total, err := q.GetJobLogs(ctx, job.ID, 0, -1, true)
q.UpdateJobProgress(ctx, job.ID, 50)        // cập nhật tiến độ từ bên ngoài
```

## Sửa, thử lại, xóa job

```go
job.UpdateData(ctx, newData)                    // thay dữ liệu
job.Retry(ctx, bullmq.StateFailed, nil)         // đưa một job lỗi chạy lại
job.Retry(ctx, bullmq.StateFailed, &bullmq.RetryOptions{ResetAttemptsMade: true})

q.RetryJobs(ctx, "failed", 1000, time.Time{})   // chạy lại MỌI job lỗi, 1000 job mỗi lượt

ok, err := q.Remove(ctx, "42")                  // xóa job (và job con)
// ok == false: job hoặc job con đang bị khóa (đang chạy), chưa xóa được
ok, err = q.RemoveWithoutChildren(ctx, "42")
```

Job do job scheduler sinh ra không xóa trực tiếp được; xóa scheduler.

## Quản lý queue

```go
q.Pause(ctx)              // mọi worker ngừng lấy job mới (job đang chạy vẫn chạy xong)
q.Resume(ctx)
paused, _ := q.IsPaused(ctx)

q.Drain(ctx, true)        // xóa mọi job đang chờ (true: cả job delayed)

// Xóa job completed cũ hơn 1 giờ, tối đa 10_000 job; trả về ID đã xóa
ids, err := q.Clean(ctx, time.Hour, 10_000, "completed")

q.Obliterate(ctx, true, 0) // XÓA SẠCH queue: mọi key. force=true: kể cả khi có job đang chạy
```

`Obliterate` không thể hoàn tác; chỉ dùng cho môi trường test hoặc khi bỏ hẳn
một queue.

### Giới hạn toàn cục (mọi worker cộng lại)

```go
q.SetGlobalConcurrency(ctx, 20)              // cả cụm tối đa 20 job active cùng lúc
q.SetGlobalRateLimit(ctx, 100, time.Minute)  // cả cụm tối đa 100 job / phút
q.RemoveGlobalConcurrency(ctx)
q.RemoveGlobalRateLimit(ctx)
```

Khác với `WorkerOptions.Concurrency` (giới hạn từng worker), giới hạn toàn cục
lưu trong Redis và áp cho mọi worker, kể cả worker Node.js. Dùng khi một dịch vụ
bên ngoài chỉ chịu được N request song song.

### Thông tin queue

```go
meta, _ := q.GetMeta(ctx)              // trạng thái pause, giới hạn toàn cục...
workers, _ := q.GetWorkers(ctx)        // các worker đang kết nối (mọi ngôn ngữ)
n, _ := q.GetWorkersCount(ctx)
text, _ := q.ExportPrometheusMetrics(ctx) // xem Vận hành
```

Tiếp theo: [Worker](04-worker.md).
