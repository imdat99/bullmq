# 2. Kết nối Redis

Thư viện chỉ là client: trỏ tới Redis nào cũng được, kể cả Redis đang phục vụ
ứng dụng khác. Mọi constructor (`NewQueue`, `NewWorker`, `NewFlowProducer`,
`NewQueueEvents`) đều nhận `RedisConnectionOptions` qua trường `Connection`.

## Các cách khai báo

### Bằng URL

```go
conn := bullmq.RedisConnectionOptions{
	URL: "redis://:matkhau@10.0.0.5:6379/2", // DB số 2
}
```

Định dạng: `redis://[user][:password]@host:port[/db]`. Dùng `rediss://` cho TLS.
Để trống tất cả thì mặc định là `redis://127.0.0.1:6379`.

Nên đọc URL từ biến môi trường thay vì ghi cứng:

```go
conn := bullmq.RedisConnectionOptions{URL: os.Getenv("REDIS_URL")}
```

### Bằng từng trường

```go
conn := bullmq.RedisConnectionOptions{
	Host:     "10.0.0.5",
	Port:     6379,          // mặc định 6379
	Username: "app",         // ACL user (Redis 6+), có thể bỏ trống
	Password: os.Getenv("REDIS_PASSWORD"),
	DB:       bullmq.Ptr(2), // nil = DB 0 (hoặc DB trong URL)
}
```

Khi `Host` khác rỗng, `URL` bị bỏ qua.

### TLS và mTLS

```go
ca, _ := os.ReadFile("/etc/redis/ca.pem")
crt, _ := os.ReadFile("/etc/redis/client.crt")
key, _ := os.ReadFile("/etc/redis/client.key")

conn := bullmq.RedisConnectionOptions{
	Host: "redis.internal", Port: 6380, Password: "...",
	TLS: true,
	TLSCerts: &bullmq.TLSCerts{
		RootCert:   ca,  // bỏ trống nếu CA đã có trong hệ thống
		ClientCert: crt, // ClientCert + ClientKey = mTLS
		ClientKey:  key,
	},
}
```

Chứng chỉ PEM được kiểm tra ngay khi tạo kết nối: file hỏng trả về
`ErrInvalidConfig` thay vì lỗi khó hiểu lúc bắt tay TLS.

### Redis Cluster

```go
conn := bullmq.RedisConnectionOptions{
	ClusterNodes: []string{"redis://10.0.0.1:7000", "redis://10.0.0.2:7000", "redis://10.0.0.3:7000"},
	Password:     os.Getenv("REDIS_PASSWORD"),
}

q, err := bullmq.NewQueue(ctx, "emails", &bullmq.QueueOptions{
	Connection: conn,
	Prefix:     "{bull}", // BẮT BUỘC có hash tag
})
```

Lua script của BullMQ đụng tới nhiều key của cùng một queue trong một lệnh, nên
mọi key của queue phải nằm trên cùng một slot. Có hai cách:

- Hash tag trong prefix: `Prefix: "{bull}"`. Đơn giản, nhưng **mọi queue** đều
  về một node.
- Hash tag trong tên queue: `"{emails}"`, `"{reports}"`. Mỗi queue một slot,
  tải được chia đều giữa các node. Nên dùng khi có nhiều queue tải cao.

Producer, worker và QueueEvents của cùng một queue phải dùng **cùng** prefix và
tên queue.

### Dùng lại client go-redis có sẵn

Nếu ứng dụng đã có `*redis.Client` (go-redis v9), bọc nó lại để không phải mở
thêm một pool kết nối nữa:

```go
rdb := redis.NewClient(&redis.Options{Addr: "10.0.0.5:6379", Password: "..."})

conn := bullmq.NewRedisConnectionFromClient(rdb)

q, err := bullmq.NewQueueWithConnection(ctx, "emails", conn, nil)
w, err := bullmq.NewWorkerWithConnection(ctx, "emails", process, conn, &bullmq.WorkerOptions{Concurrency: 10})
fp, err := bullmq.NewFlowProducerWithConnection(conn, "")
ev, err := bullmq.NewQueueEventsWithConnection(ctx, "emails", conn, nil)
```

- `Close()` của queue/worker **không** đóng `rdb`; ứng dụng tự đóng.
- Worker và QueueEvents vẫn mở **một kết nối riêng** (cùng cấu hình) cho lệnh
  chờ dạng chặn (`BZPOPMIN`, `XREAD BLOCK`), vì lệnh chặn sẽ giữ cả kết nối.
  Do đó client phải là `*redis.Client` hoặc `*redis.ClusterClient`, không được
  là `redis.Ring` hay kiểu tự viết.
- Cũng có thể dùng chung một `*bullmq.RedisConnection` cho nhiều queue:
  `conn, err := bullmq.NewRedisConnection(ctx, opts)`.

## Prefix: tách dữ liệu

Mọi key có dạng `{prefix}:{tên-queue}:{...}`, prefix mặc định `bull`
(giống Node.js). Ví dụ: `bull:emails:wait`, `bull:emails:42`.

```go
&bullmq.QueueOptions{Connection: conn, Prefix: "shop"} // shop:emails:...
```

Dùng prefix khác nhau khi nhiều môi trường (dev/staging) hoặc nhiều ứng dụng
dùng chung một Redis. Prefix và tên queue **không được chứa dấu `:`**.

## Số kết nối

| Thành phần | Kết nối dùng |
| --- | --- |
| `Queue` | Pool `MaxConnections` (mặc định ≥ 4) |
| `Worker` | Pool riêng + 1 kết nối chặn, **không phụ thuộc** `Concurrency` |
| `QueueEvents` | Pool riêng + 1 kết nối chặn |
| `FlowProducer` | Pool riêng |

Một worker với `Concurrency: 500` vẫn chỉ cần khoảng 5 kết nối. Chia sẻ
`RedisConnection` giữa nhiều thành phần nếu cần tiết kiệm hơn nữa.

## Mất kết nối và Redis khởi động lại

go-redis tự kết nối lại. Worker gặp lỗi Redis sẽ phát sự kiện `EventError`,
nghỉ `RunRetryDelay` (mặc định 15s) rồi thử lại; không cần tự viết vòng lặp
kết nối lại. Lệnh chờ dạng chặn có deadline (thời gian chặn + 10s) để phát hiện
kết nối "chết lửng" (half-open) sau sự cố mạng.

Lua script được nạp lại tự động khi Redis khởi động lại (mất cache script).

Thư viện **không tự retry** lệnh ghi khi lỗi mạng (có thể đã chạy trên Redis),
để không thêm trùng job. `q.Add` trả lỗi thì ứng dụng tự quyết định thử lại;
dùng `JobID` cố định để thử lại an toàn.

## Yêu cầu với Redis

| Yêu cầu | Lý do |
| --- | --- |
| Redis ≥ 6.2 | Lua script dùng lệnh của 6.2 |
| `maxmemory-policy noeviction` | Policy khác (`allkeys-lru`…) cho phép Redis tự xóa key khi đầy, làm mất job hoặc hỏng queue |
| Cho phép `EVAL`/`EVALSHA` | Mọi thao tác chạy bằng Lua |
| Cho phép `CLIENT SETNAME` | Đặt tên kết nối để `q.GetWorkers` thấy worker (tùy chọn) |

Kiểm tra:

```bash
redis-cli -u "$REDIS_URL" config get maxmemory-policy   # phải là noeviction
redis-cli -u "$REDIS_URL" info server | grep redis_version
```

Nếu Redis đang làm cache với `allkeys-lru` và không đổi được, hãy dùng một
Redis riêng cho queue.

**Khi đầy bộ nhớ** (với `noeviction`): Redis từ chối lệnh ghi, `q.Add` trả lỗi
`OOM command not allowed`. Dữ liệu cũ không mất. Đặt `maxmemory` và giới hạn
số job giữ lại (`RemoveOnComplete`/`RemoveOnFail`) để tránh tình huống này.

## Dùng chung Redis với ứng dụng khác

Được, với điều kiện:

- Policy là `noeviction` (xem trên).
- Dùng prefix riêng hoặc DB riêng để không đụng key của ứng dụng khác.
- Không chạy bộ test của thư viện trên Redis production: có 2 test gọi
  `SCRIPT FLUSH` (xóa cache Lua của cả server).

Tiếp theo: [Queue và Job](03-queue-va-job.md).
