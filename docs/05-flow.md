# 5. Flow: job cha – con

Flow là một **cây job**: job cha chỉ chạy khi mọi job con đã xong, và đọc được
kết quả của các job con. Job con có thể nằm ở queue khác (do worker khác xử lý).

Ví dụ: xử lý một video gồm chuyển mã 3 độ phân giải (song song, ở queue
`transcode`), xong cả 3 mới đóng gói và thông báo (queue `videos`).

```
render (videos)                 ← chạy cuối cùng
├── transcode-1080 (transcode)  ┐
├── transcode-720  (transcode)  ├ chạy song song
└── transcode-480  (transcode)  ┘
```

## Thêm một flow

```go
fp, err := bullmq.NewFlowProducer(ctx, &bullmq.FlowProducerOptions{
	Connection: bullmq.RedisConnectionOptions{URL: os.Getenv("REDIS_URL")},
})
if err != nil {
	return err
}
defer fp.Close()

node, err := fp.Add(ctx, bullmq.FlowJob{
	Name:      "render",
	QueueName: "videos",
	Data:      map[string]string{"videoId": "v42"},
	Children: []bullmq.FlowJob{
		{Name: "transcode", QueueName: "transcode", Data: map[string]int{"height": 1080}},
		{Name: "transcode", QueueName: "transcode", Data: map[string]int{"height": 720}},
		{Name: "transcode", QueueName: "transcode", Data: map[string]int{"height": 480}},
	},
})
if err != nil {
	return err
}

fmt.Println("job cha:", node.Job.ID)
for _, c := range node.Children {
	fmt.Println("  job con:", c.Job.ID)
}
```

- Cả cây được thêm **nguyên tử** (một transaction): hoặc thêm hết, hoặc không.
- Job cha nằm ở trạng thái `waiting-children`; job con vào hàng đợi bình thường.
- Cây có thể nhiều tầng: job con cũng có `Children`.
- Mỗi `FlowJob` nhận `Opts` (`*JobOptions`) như job thường: `Attempts`, `Delay`,
  `Priority`…
- Nhiều flow một lúc: `fp.AddBulk(ctx, []bullmq.FlowJob{...})`.

### Tùy chọn mặc định theo queue

```go
node, err := fp.AddWithOptions(ctx, flow, &bullmq.FlowOptions{
	QueuesOptions: map[string]bullmq.FlowQueueOptions{
		"transcode": {DefaultJobOptions: bullmq.JobOptions{
			Attempts: 3, Backoff: bullmq.ExponentialBackoff(5 * time.Second),
		}},
	},
})
```

## Worker của job cha đọc kết quả job con

Worker của các queue không cần biết gì về flow. Job con trả kết quả như bình
thường; job cha đọc lại:

```go
// worker queue "transcode"
func transcode(ctx context.Context, job *bullmq.Job) (any, error) {
	var in struct{ Height int `json:"height"` }
	_ = job.DecodeData(&in)
	url, err := doTranscode(ctx, in.Height)
	if err != nil {
		return nil, err
	}
	return map[string]any{"height": in.Height, "url": url}, nil
}

// worker queue "videos"
func render(ctx context.Context, job *bullmq.Job) (any, error) {
	values, err := job.GetChildrenValues(ctx) // map[khóa-job-con]giá-trị
	if err != nil {
		return nil, err
	}
	for childKey, v := range values {
		// childKey dạng "bull:transcode:17"; v là JSON đã giải mã (map[string]any...)
		fmt.Println(childKey, v)
	}
	return "ok", nil
}
```

Muốn giải mã sang struct, mã hóa lại `v` bằng `json.Marshal` rồi `json.Unmarshal`.

Các hàm khác trên job cha:

```go
cnt, _ := job.GetDependenciesCount(ctx)     // Processed, Unprocessed, Failed, Ignored
pending, _ := job.GetUnprocessedDependencies(ctx)
page, _ := job.GetDependencies(ctx, 0, 0, 100) // phân trang bằng cursor
```

## Khi job con thất bại

Mặc định, job con **hết lượt retry** thì job cha nằm mãi ở `waiting-children`
(phải xử lý tay). Chọn một hành vi khác qua `Opts` của **job con**:

| Tùy chọn của job con | Khi job con `failed` |
| --- | --- |
| (không đặt) | Job cha chờ mãi |
| `FailParentOnFailure: true` | Job cha `failed` luôn (lan dần lên gốc nếu job cha cũng đặt) |
| `IgnoreDependencyOnFailure: true` | Coi như xong; job cha chạy tiếp, đọc lỗi qua `job.GetIgnoredChildrenFailures(ctx)` |
| `RemoveDependencyOnFailure: true` | Bỏ phụ thuộc; job cha chạy tiếp như thể không có job con này |
| `ContinueParentOnFailure: true` | Job cha được chạy **ngay** khi job con lỗi, không chờ các job con khác |

```go
{Name: "thumbnail", QueueName: "images", Opts: &bullmq.JobOptions{
	Attempts:                  3,
	IgnoreDependencyOnFailure: true, // thiếu thumbnail vẫn render được
}},
```

## Thêm job con vào job cha có sẵn

```go
q.Add(ctx, "extra-step", data, &bullmq.JobOptions{
	Parent: &bullmq.ParentOptions{Queue: "videos", ID: parentID},
})
```

`Queue` là tên queue của job cha (hoặc `"prefix:tên"` nếu khác prefix).

## Job cha tự sinh job con khi đang chạy

Mẫu "chia việc động": job cha chạy, sinh job con, rồi chờ chúng xong và chạy
lại để tổng hợp.

```go
func parent(ctx context.Context, job *bullmq.Job) (any, error) {
	var st struct{ Step string `json:"step"` }
	_ = job.DecodeData(&st)

	switch st.Step {
	case "": // lần chạy đầu: sinh job con
		for _, part := range []int{1, 2, 3} {
			_, err := childQueue.Add(ctx, "part", part, &bullmq.JobOptions{
				Parent: &bullmq.ParentOptions{Queue: job.QueueName, ID: job.ID},
			})
			if err != nil {
				return nil, err
			}
		}
		if err := job.UpdateData(ctx, map[string]string{"step": "aggregate"}); err != nil {
			return nil, err
		}
		waiting, err := job.MoveToWaitingChildren(ctx, "")
		if err != nil {
			return nil, err
		}
		if waiting {
			return nil, bullmq.ErrWaitingChildren // worker không đụng vào job nữa
		}
		fallthrough // các job con đã xong hết (rất nhanh): tổng hợp luôn

	case "aggregate": // chạy lại sau khi mọi job con xong
		values, err := job.GetChildrenValues(ctx)
		if err != nil {
			return nil, err
		}
		return len(values), nil
	}
	return nil, nil
}
```

## Đọc lại một flow

```go
tree, err := fp.GetFlow(ctx, bullmq.GetFlowOptions{
	QueueName: "videos", ID: parentID,
	Depth: 3, MaxChildren: 50, // mặc định 10 và 20
})
```

Lỗi bọc `ErrJobNotFound` nếu job gốc không tồn tại.

## Lưu ý

- Mọi queue trong một flow phải nằm cùng Redis. Với Redis Cluster, các queue
  của một flow phải cùng hash tag (ví dụ `Prefix: "{bull}"`).
- `RemoveOnComplete` của job con xóa job con nhưng kết quả của nó **vẫn được
  giữ** trong job cha, `GetChildrenValues` vẫn đọc được.
- Xóa job cha bằng `q.Remove` xóa luôn các job con (trả `false` nếu có job con
  đang chạy). `q.RemoveWithoutChildren` chỉ xóa job cha;
  `q.RemoveUnprocessedChildren` chỉ xóa các job con chưa chạy.

Tiếp theo: [Job định kỳ](06-job-scheduler.md).
