# 9. Tham chiếu lỗi

Mọi lỗi được bọc thêm ngữ cảnh (`bullmq: ...: %w`). Luôn kiểm tra bằng
`errors.Is` / `errors.As`, không so sánh chuỗi.

## Lỗi sentinel

| Lỗi | Khi nào |
| --- | --- |
| `ErrInvalidConfig` | Tham số hoặc tùy chọn sai (xem bảng cuối trang). Không thử lại được: sửa code |
| `ErrJobNotFound` | `fp.GetFlow` với job gốc không tồn tại; `job.MoveToWaitingChildren` trên job đã bị xóa |
| `ErrJobLocked` | `job.ExtendLock` không gia hạn được khóa (token sai hoặc khóa đã mất) |
| `ErrJobLockNotExist` | `job.MoveToWaitingChildren`: khóa job đã mất |
| `ErrJobNotInState` | `job.MoveToWaitingChildren` trên job không `active` |
| `ErrWorkerClosed` | `w.Run()` hoặc `w.GetNextJob` trên worker đã đóng |
| `ErrNoScriptContext` | Gọi hàm Redis trên `Job` không gắn với queue (ví dụ `bullmq.Job{}` tự tạo) |
| `ErrDelayed` | Processor trả về sau khi đã tự gọi `job.MoveToDelayed` |
| `ErrWaitingChildren` | Processor trả về sau khi đã tự gọi `job.MoveToWaitingChildren` |

Các thao tác khác trên job (`Promote`, `ChangeDelay`, `Retry`, `Remove`…) khi
gặp trạng thái không hợp lệ trả về `*ScriptError` kèm mã (xem dưới), không phải
các sentinel ở trên.

```go
job, err := q.GetJob(ctx, id) // lưu ý: không tồn tại → (nil, nil), không phải lỗi
if err != nil { ... }
if job == nil { ... }

err = job.Promote(ctx)
if code, ok := bullmq.ScriptErrorCode(err); ok && code == bullmq.CodeJobNotInState {
	// job không còn ở delayed (đã chạy rồi), bỏ qua
}
```

## Kiểu lỗi

### `*UnrecoverableError`

Processor trả về để đánh lỗi job ngay, không retry. `Message` được lưu nguyên
văn vào `FailedReason`.

```go
return nil, bullmq.NewUnrecoverableError("email không hợp lệ")
bullmq.IsUnrecoverable(err) // kiểm tra, kể cả khi đã bọc
```

### `*RateLimitError`

Processor trả về (sau `q.RateLimit`) để đưa job về hàng đợi mà không tính là
thất bại. Xem [Worker](04-worker.md#rate-limit).

### `*ScriptError`: lỗi từ Lua script

```go
if code, ok := bullmq.ScriptErrorCode(err); ok && code == bullmq.CodeJobLockNotExist {
	// ...
}
```

| Mã | Hằng số | Ý nghĩa |
| --- | --- | --- |
| -1 | `CodeJobNotExist` | Job không tồn tại |
| -2 | `CodeJobLockNotExist` | Khóa job không còn |
| -3 | `CodeJobNotInState` | Job không ở trạng thái mong đợi |
| -4 | `CodeJobPendingDependencies` | Job còn job con chưa xong |
| -5 | `CodeParentJobNotExist` | Job cha không tồn tại (khi thêm job con với `Parent`) |
| -6 | `CodeJobLockMismatch` | Khóa đang thuộc worker khác (token khác) |
| -7 | `CodeParentJobCannotBeReplaced` | Không thể thay job cha đã có |
| -8 | `CodeJobBelongsToJobScheduler` | Job thuộc scheduler, không xóa trực tiếp được |
| -9 | `CodeJobHasFailedChildren` | Job có job con thất bại |
| -10 | `CodeSchedulerJobIDCollision` | Scheduler: ID job lần kế tiếp đã tồn tại |
| -11 | `CodeSchedulerJobSlotsBusy` | Scheduler: các mốc thời gian hiện tại và kế tiếp đều đã có job |

Khi worker hoàn tất job thất bại vì các mã trên, `FailedReason` có thông báo chi
tiết giống Node.js, ví dụ `Missing lock for job 42. moveToFinished`,
`Lock mismatch for job 42. Cmd moveToFinished from active`. Những thông báo này
gần như luôn nghĩa là job đã bị coi là treo (worker giữ nó quá `LockDuration`
mà không gia hạn được) và được chuyển cho worker khác.

## Lỗi kiểm tra tham số (`ErrInvalidConfig`)

| Thông báo | Nguyên nhân |
| --- | --- |
| `Queue name must be provided` | Tên queue rỗng |
| `Queue name cannot contain :` | Tên queue có dấu `:` |
| `Prefix must be non-empty and cannot contain :` | Prefix có dấu `:` |
| `Custom Id cannot be integers` | `JobID` là số nguyên, ví dụ `"123"` |
| `Custom Id cannot contain :` | `JobID` có dấu `:` |
| `JobId cannot be '0' or start with '0:'` | `JobID` bằng `"0"` |
| `Delay cannot be negative` | `Delay < 0` |
| `Priority should be between 0 and 2097152` | `Priority` ngoài khoảng |
| `The size of job X exceeds the limit N bytes` | Dữ liệu lớn hơn `SizeLimit` |
| `Parent id must be provided` | `Parent` thiếu `ID` |
| `concurrency must be a finite number greater than 0` | `Concurrency` âm |
| `lockDuration must be greater than 0` | `LockDuration` âm |
| `stalledInterval must be greater than 0` | `StalledInterval` âm |
| `drainDelay must be greater than 0` | `DrainDelay` âm |
| `maxStalledCount must be greater than or equal to 0` | `MaxStalledCount` âm |
| `limiter max and duration must be greater than 0` | `Limiter` thiếu `Max` hoặc `Duration` |
| `processor must not be nil` | `NewWorker` với processor `nil` |
| `Worker is already running` / `Worker can only be run once` | Gọi `w.Run()` hai lần, hoặc sau khi đóng |
| `Either .pattern or .every option must be defined` | `RepeatOptions` thiếu cả `Pattern` lẫn `Every` |
| `Both .pattern and .every options are defined; only one may be used` | Đặt cả hai |
| `Both .immediately and .startDate options are defined; only one may be used` | `Immediately` cùng `StartDate` |
| `End date must be greater than current timestamp` | `EndDate` ở quá khứ |
| `invalid cron pattern '...'` | Biểu thức cron sai |
| `invalid timezone: ...` | `TZ` không phải tên IANA hợp lệ |
| `invalid state: ...` | Tên trạng thái sai trong `Clean`, `GetJobs`… |
| `retry state must be failed or completed` | `job.Retry` với trạng thái khác |
| `invalid redis url ...` | URL Redis sai định dạng |
| `tls root certificate is not valid PEM` / `tls client certificate: ...` | File chứng chỉ hỏng |
| `dedicated connections need a *redis.Client or *redis.ClusterClient` | Worker/QueueEvents dùng client không hỗ trợ |
| `keepLastIfActive deduplication is not supported for flow nodes with a parent` | `KeepLastIfActive` trên job con của flow |

Giá trị 0 của các tùy chọn thời gian/số lượng nghĩa là "dùng mặc định", nên chỉ
giá trị **âm** bị từ chối.
