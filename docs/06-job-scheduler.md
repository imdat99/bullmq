# 6. Job định kỳ (Job Scheduler)

Job scheduler sinh job theo lịch: theo biểu thức cron, hoặc mỗi N giây/phút.
Lịch lưu trong Redis, nên dù chạy bao nhiêu worker, **mỗi lần đến giờ chỉ sinh
một job**. Không cần (và không nên) dùng cron của hệ điều hành hay
`time.Ticker` ở từng pod.

## Tạo hoặc cập nhật

```go
// Mỗi ngày 08:00 giờ Việt Nam
_, err := q.UpsertJobScheduler(ctx,
	"daily-report",                                                   // ID của scheduler
	bullmq.RepeatOptions{Pattern: "0 8 * * *", TZ: "Asia/Ho_Chi_Minh"}, // lịch
	"report",                                                         // tên job sinh ra ("" = dùng ID)
	map[string]string{"type": "daily"},                               // dữ liệu của mỗi job
	&bullmq.JobOptions{Attempts: 3, RemoveOnComplete: bullmq.KeepLast(30)}, // tùy chọn của mỗi job
)
```

```go
// Mỗi 5 phút, lần đầu chạy ngay
_, err := q.UpsertJobScheduler(ctx, "sync-inventory",
	bullmq.RepeatOptions{Every: 5 * time.Minute}, "sync", nil, nil)
```

- **Upsert**: gọi lại với cùng ID thì cập nhật lịch/dữ liệu, không tạo trùng.
  Vì vậy gọi `UpsertJobScheduler` mỗi lần ứng dụng khởi động là an toàn và là
  cách được khuyên dùng (khai báo lịch trong code).
- Kết quả trả về là job **kế tiếp** (đang ở trạng thái `delayed`), hoặc `nil`
  nếu scheduler đã chạy đủ `Limit` lần.
- Job sinh ra được xử lý bởi worker bình thường của queue, có `job.RepeatJobKey`
  bằng ID scheduler. **Cần có worker chạy**: job kế tiếp được tạo khi job hiện
  tại chạy xong (thành công hoặc thất bại).

## `RepeatOptions`

| Trường | Ý nghĩa |
| --- | --- |
| `Pattern` | Biểu thức cron. Loại trừ với `Every` |
| `Every` | Lặp mỗi khoảng thời gian. Lần đầu chạy ngay (hoặc tại `StartDate`) |
| `TZ` | Múi giờ IANA cho `Pattern`, ví dụ `"Asia/Ho_Chi_Minh"`. Mặc định UTC |
| `Limit` | Số lần chạy tối đa (0 = vô hạn) |
| `StartDate` | Không chạy trước thời điểm này |
| `EndDate` | Không chạy sau thời điểm này (phải ở tương lai) |
| `Immediately` | Với `Pattern`: chạy một lần ngay rồi theo lịch. Không dùng cùng `StartDate` |
| `Offset` | Với `Every`: lệch mốc chạy. Ví dụ `Every: time.Hour, Offset: 15*time.Minute` → chạy phút 15 mỗi giờ |

**Luôn đặt `TZ`** khi dùng cron theo giờ: mặc định là UTC, `"0 8 * * *"` không
có `TZ` sẽ chạy lúc 15:00 giờ Việt Nam.

### Cú pháp cron

5 trường (phút giờ ngày tháng thứ), hoặc 6 trường với **giây ở đầu**.

| Biểu thức | Ý nghĩa |
| --- | --- |
| `*/10 * * * *` | Mỗi 10 phút |
| `0 * * * *` | Đầu mỗi giờ |
| `30 8 * * 1-5` | 08:30 các ngày thứ Hai – thứ Sáu |
| `0 0 1 * *` | 00:00 ngày 1 hằng tháng |
| `0 9,18 * * *` | 09:00 và 18:00 |
| `*/30 * * * * *` | Mỗi 30 giây (6 trường) |
| `@daily`, `@hourly`, `@weekly`, `@monthly` | Viết tắt |

Thử biểu thức trước khi dùng:

```go
next, err := bullmq.NextCronTime("30 8 * * 1-5", time.Now(), "Asia/Ho_Chi_Minh", time.Time{})
fmt.Println(next) // lần chạy kế tiếp
```

## Xem và xóa

```go
s, err := q.GetJobScheduler(ctx, "daily-report") // nil nếu không có
fmt.Println(s.Pattern, s.TZ, s.Next, s.IterationCount)

list, err := q.GetJobSchedulers(ctx, 0, -1, true)  // sắp theo lần chạy kế tiếp
n, err := q.GetJobSchedulersCount(ctx)

existed, err := q.RemoveJobScheduler(ctx, "daily-report") // xóa lịch và job kế tiếp đang chờ
```

Khi bỏ một lịch khỏi code, nhớ gọi `RemoveJobScheduler`: Upsert lúc khởi động
không tự xóa các lịch cũ.

## Lưu ý

- Mỗi scheduler chỉ có **một** job đang chờ tại một thời điểm. Nếu không có
  worker nào chạy lúc đến giờ, job đó nằm trong queue và chạy khi worker lên
  lại; các lần bị lỡ **không** được chạy bù.
- Các lần chạy của cùng một scheduler không chồng lên nhau: job kế tiếp chỉ được
  tạo khi job trước xong. Job chạy lâu hơn chu kỳ (10 phút với
  `Every: 5 * time.Minute`) thì lần sau chạy ngay sau khi nó xong.
- Job do scheduler sinh không xóa trực tiếp được bằng `q.Remove`; xóa scheduler.

Tiếp theo: [Theo dõi sự kiện](07-su-kien.md).
