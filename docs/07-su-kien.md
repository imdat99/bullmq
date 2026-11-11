# 7. Theo dõi sự kiện

Có hai nguồn sự kiện, khác nhau ở phạm vi:

| | `Worker.NextEvent` | `QueueEvents` |
| --- | --- | --- |
| Phạm vi | Chỉ job **do worker này** xử lý | **Mọi** job của queue, từ mọi tiến trình và ngôn ngữ |
| Cơ chế | Trong bộ nhớ | Redis Stream `{prefix}:{queue}:events` |
| Dùng khi | Log, metric trong service worker | Service khác (API, dashboard) cần biết job xong/lỗi |

## Sự kiện của Worker

```go
for {
	e, ok := w.NextEvent(ctx)
	if !ok {
		break // ctx hết hạn, hoặc worker đã đóng và đã đọc hết sự kiện
	}
	log.Printf("%s job=%s err=%v", e.Type, e.JobID, e.Err)
}
```

| `e.Type` | Khi nào | Trường có giá trị |
| --- | --- | --- |
| `EventReady` | Worker bắt đầu chạy | |
| `EventActive` | Bắt đầu xử lý một job | `JobID` |
| `EventProgress` | Processor gọi `UpdateProgress` | `JobID`, `Progress` (JSON) |
| `EventCompleted` | Job xong | `JobID`, `Result` (JSON) |
| `EventFailed` | **Mỗi** lần chạy thất bại (kể cả khi còn retry) | `JobID`, `Err` |
| `EventStalled` | Một job bị treo được khôi phục | `JobID` |
| `EventDrained` | Queue hết job | |
| `EventError` | Lỗi của worker (mất kết nối Redis…), không gắn với job | `Err` |
| `EventPaused`, `EventResumed` | `w.Pause()`, `w.Resume()` | |
| `EventClosed` | Worker đã đóng | |

`w.TryNextEvent()` đọc không chờ. Bộ đệm giữ tối đa 100 000 sự kiện; không đọc
thì sự kiện cũ bị bỏ, không gây rò rỉ bộ nhớ.

Muốn biết job **thất bại hẳn** (hết retry) chứ không phải từng lần thử:

```go
case bullmq.EventFailed:
	if job, _ := q.GetJob(ctx, e.JobID); job != nil {
		if st, _ := job.GetState(ctx); st == bullmq.StateFailed {
			alert(job) // đã hết lượt retry
		}
	}
```

Hoặc dùng `QueueEventRetriesExhausted` của `QueueEvents` bên dưới.

## QueueEvents: sự kiện xuyên tiến trình

```go
events, err := bullmq.NewQueueEvents(ctx, "emails", &bullmq.QueueEventsOptions{
	Connection: bullmq.RedisConnectionOptions{URL: os.Getenv("REDIS_URL")},
})
if err != nil {
	return err
}
defer events.Close()

for {
	entry, ok := events.NextEvent(ctx)
	if !ok {
		break
	}
	ev := entry.Event
	switch ev.Type {
	case bullmq.QueueEventCompleted:
		var res MyResult
		_ = ev.DecodeReturnValue(&res)
		log.Printf("job %s xong: %+v", ev.JobID, res)
	case bullmq.QueueEventFailed:
		log.Printf("job %s lỗi: %s", ev.JobID, ev.FailedReason)
	case bullmq.QueueEventRetriesExhausted:
		log.Printf("job %s đã hết lượt retry", ev.JobID)
	case bullmq.QueueEventError:
		log.Printf("lỗi đọc stream: %s", ev.Message) // listener tự thử lại
	}
}
```

`NewQueueEvents` tự bắt đầu nghe ngay. Mặc định chỉ nhận sự kiện xảy ra **sau**
khi bắt đầu nghe.

### Các loại sự kiện

| `ev.Type` | Tên trong stream | Trường có giá trị |
| --- | --- | --- |
| `QueueEventAdded` | `added` | `JobID`, `JobName` |
| `QueueEventWaiting` | `waiting` | `JobID`, `Prev` |
| `QueueEventDelayed` | `delayed` | `JobID`, `Delay` (Unix ms; dùng `ev.DelayUntil()`) |
| `QueueEventActive` | `active` | `JobID`, `Prev` |
| `QueueEventProgress` | `progress` | `JobID`, `Progress` (`ev.DecodeProgress(&v)`) |
| `QueueEventCompleted` | `completed` | `JobID`, `ReturnValue` (`ev.DecodeReturnValue(&v)`), `Prev` |
| `QueueEventFailed` | `failed` | `JobID`, `FailedReason`, `Prev` |
| `QueueEventRetriesExhausted` | `retries-exhausted` | `JobID`, `AttemptsMade` |
| `QueueEventStalled` | `stalled` | `JobID` |
| `QueueEventWaitingChildren` | `waiting-children` | `JobID` |
| `QueueEventRemoved` | `removed` | `JobID`, `Prev` |
| `QueueEventDuplicated` | `duplicated` | `JobID` (thêm job trùng `JobID`) |
| `QueueEventDeduplicated` | `deduplicated` | `JobID`, `DeduplicationID`, `DeduplicatedJobID` |
| `QueueEventDebounced` | `debounced` | `JobID`, `DebounceID` (tên cũ của deduplicated) |
| `QueueEventCleaned` | `cleaned` | `Count` |
| `QueueEventDrained` | `drained` | |
| `QueueEventPaused`, `QueueEventResumed` | `paused`, `resumed` | |
| `QueueEventError` | (do listener tạo) | `Message` |
| `QueueEventOther` | tên lạ | `Event`, `Fields` (tương thích phiên bản BullMQ mới hơn) |

### Đọc lại lịch sử, tiếp tục sau khi khởi động lại

Mỗi `entry.ID` là ID của bản ghi trong Redis Stream. Lưu lại ID cuối cùng đã xử
lý để tiếp tục mà không bỏ sót:

```go
events, err := bullmq.NewQueueEvents(ctx, "emails", &bullmq.QueueEventsOptions{
	LastEventID: loadCheckpoint(), // "" hoặc "$": chỉ sự kiện mới; "0": từ đầu stream
})
// ...
saveCheckpoint(entry.ID)
```

Stream chỉ giữ khoảng 10 000 sự kiện gần nhất (cắt bớt tự động); có thể cắt
thêm bằng `q.TrimEvents(ctx, n)`. Không dùng stream này như một kho lưu trữ lâu
dài.

### Mẫu: API chờ job xong rồi trả kết quả

```go
// Tạo MỘT QueueEvents khi khởi động, phân phối kết quả theo job ID.
type waiter struct {
	mu      sync.Mutex
	pending map[string]chan bullmq.QueueEvent
}

func (w *waiter) run(ctx context.Context, events *bullmq.QueueEvents) {
	for {
		entry, ok := events.NextEvent(ctx)
		if !ok {
			return
		}
		ev := entry.Event
		if ev.Type != bullmq.QueueEventCompleted && ev.Type != bullmq.QueueEventFailed {
			continue
		}
		w.mu.Lock()
		if ch, ok := w.pending[ev.JobID]; ok {
			ch <- ev // ch có buffer 1
			delete(w.pending, ev.JobID)
		}
		w.mu.Unlock()
	}
}
```

Đăng ký `pending[jobID]` **trước** khi gọi `q.Add` không được (chưa có ID): dùng
`JobID` tự đặt, hoặc sau khi `Add` thì kiểm tra `job.GetState` một lần để không
lỡ sự kiện đến quá nhanh. Đặt timeout cho việc chờ.

### Các tùy chọn

| Trường | Mặc định | Ý nghĩa |
| --- | --- | --- |
| `Connection`, `Prefix` | | Như Queue |
| `LastEventID` | `"$"` | Điểm bắt đầu đọc |
| `BlockingTimeout` | 5s | Thời gian mỗi lần `XREAD BLOCK` |
| `ManualStart` | false | Không tự nghe; gọi `events.Run()` |

Có thể tạo từ cấu hình queue: `bullmq.NewQueueEventsFromQueueOptions(ctx, "emails", queueOpts)`.

Tiếp theo: [Vận hành production](08-van-hanh.md).
