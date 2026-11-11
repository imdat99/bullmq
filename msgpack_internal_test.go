package bullmq

import (
	"bullmq/internal/msgpack"
	"bullmq/internal/scripts"
	"os"
	"reflect"
	"strconv"
	"testing"

	vmsgpack "github.com/vmihailenco/msgpack/v5"
)

// The encoder feeds cmsgpack in the Lua scripts; check its output with an
// independent decoder.
func decodeMsgpack(t *testing.T, b []byte) any {
	t.Helper()

	var v any
	if err := vmsgpack.Unmarshal(b, &v); err != nil {
		t.Fatalf("decode %x: %v", b, err)
	}

	return v
}

func TestMsgpackMapRoundTrip(t *testing.T) {
	var m msgpack.Map
	m.PutStr("token", "w:1")
	m.PutInt("neg", -1)
	m.PutInt("big", 1<<40)
	m.PutBool("ok", true)
	putKeepJobs(&m, "keepJobs", KeepJobsPolicy(KeepJobs{Age: Ptr[int64](60), Count: Ptr[int64](0)}))
	putLimiter(&m, &RateLimiterOptions{Max: 5, Duration: 1500 * 1e6})

	var inner msgpack.Map
	inner.PutInt("delay", 3)
	m.PutRaw("raw", inner.Bytes())

	got := decodeMsgpack(t, m.Bytes())
	want := map[string]any{
		"token": "w:1", "neg": int8(-1), "big": uint64(1 << 40), "ok": true,
		"keepJobs": map[string]any{"age": int8(60), "count": int8(0)},
		"limiter":  map[string]any{"max": int8(5), "duration": uint16(1500)},
		"raw":      map[string]any{"delay": int8(3)},
	}
	if !reflect.DeepEqual(normalize(got), normalize(want)) {
		t.Fatalf("got %#v\nwant %#v", got, want)
	}
}

func TestPutKeepJobsForms(t *testing.T) {
	for _, tc := range []struct {
		p    *RemoveOnFinish
		want map[string]any
	}{
		{nil, map[string]any{"count": int64(-1)}},
		{RemoveAll(), map[string]any{"count": int64(0)}},
		{KeepAll(), map[string]any{"count": int64(-1)}},
		{KeepLast(7), map[string]any{"count": int64(7)}},
		{KeepJobsPolicy(KeepJobs{Limit: Ptr[int64](10)}), map[string]any{"limit": int64(10)}},
	} {
		var m msgpack.Map
		putKeepJobs(&m, "keepJobs", tc.p)

		got := normalize(decodeMsgpack(t, m.Bytes()))
		if !reflect.DeepEqual(got, map[string]any{"keepJobs": tc.want}) {
			t.Errorf("%+v: got %#v", tc.p, got)
		}
	}
}

// normalize converts every integer to int64 so comparisons ignore the
// compact encoding width.
func normalize(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			out[k] = normalize(e)
		}

		return out
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		return int64(x)
	}

	return v
}

func TestRunArgsFallsBackToEvalAfterScriptFlush(t *testing.T) {
	ctx := t.Context()

	conn, err := NewRedisConnection(ctx, RedisConnectionOptions{URL: os.Getenv("REDIS_URL")})
	if err != nil {
		t.Skipf("redis unavailable: %v", err)
	}
	defer conn.Close()

	if err := conn.client.ScriptFlush(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	keys := NewQueueKeys("runargs-"+strconv.FormatInt(nowMillis(), 36), "")
	res, err := moveToActive(ctx, conn, keys, "tok", &WorkerOptions{LockDuration: DefaultLockDuration})
	if err != nil {
		t.Fatalf("moveToActive after SCRIPT FLUSH: %v", err)
	}

	if res.job != nil {
		t.Fatalf("empty queue returned a job: %+v", res.job)
	}

	loaded, err := conn.client.ScriptExists(ctx, scripts.Get("moveToActive").Hash()).Result()
	if err != nil || !loaded[0] {
		t.Fatalf("EVAL fallback should have cached the script: %v %v", loaded, err)
	}
}
