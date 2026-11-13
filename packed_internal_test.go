package bullmq

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// packHash mirrors packJobHash in the fetch Lua scripts.
func packHash(kv ...string) string {
	var b strings.Builder
	for _, s := range kv {
		b.WriteString(strconv.Itoa(len(s)))
		b.WriteByte(':')
		b.WriteString(s)
	}

	return b.String()
}

func TestJobFromPacked(t *testing.T) {
	t.Parallel()

	packed := packHash(
		"name", "send:mail", // ':' inside a value
		"data", `{"to":"a@b.c","n":12}`,
		"opts", `{"attempts":3}`,
		"timestamp", "1700000000000",
		"delay", "1500",
		"atm", "2",
		"empty", "",
		"pb", "worker-1",
	)

	j := jobFromReply("42", packed)
	if j == nil {
		t.Fatal("nil job")
	}

	switch {
	case j.ID != "42", j.Name != "send:mail", string(j.Data) != `{"to":"a@b.c","n":12}`:
		t.Fatalf("text fields: %+v", j)
	case j.Opts.Attempts != 3, j.Timestamp != 1700000000000, j.AttemptsMade != 2:
		t.Fatalf("numeric fields: %+v", j)
	case j.Delay != 1500*time.Millisecond, j.ProcessedBy != "worker-1":
		t.Fatalf("delay/pb: %+v", j)
	}

	// Same result as the plain HGETALL array form.
	arr := []any{"name", "send:mail", "data", `{"to":"a@b.c","n":12}`, "opts", `{"attempts":3}`}
	if a := jobFromReply("42", arr); a.Name != j.Name || string(a.Data) != string(j.Data) || a.Opts.Attempts != 3 {
		t.Fatalf("array form differs: %+v", a)
	}
}

func TestJobFromPackedMalformed(t *testing.T) {
	t.Parallel()

	if j := jobFromReply("1", ""); j != nil {
		t.Fatalf("empty reply: %+v", j)
	}

	for _, in := range []string{
		"4:name",                   // value missing
		"4:name99:x",               // length past the end
		"x:name",                   // not a number
		":name",                    // no length
		"4name",                    // no separator
		"99999999999999999999999:", // overflow
	} {
		j := jobFromPacked("1", in)
		if j == nil || j.Name != "" {
			t.Fatalf("%q: %+v", in, j)
		}
	}

	// A valid prefix is kept when the tail is broken.
	if j := jobFromPacked("1", packHash("name", "ok")+"7:bad"); j.Name != "ok" {
		t.Fatalf("prefix lost: %+v", j)
	}
}

func FuzzNextPacked(f *testing.F) {
	f.Add(packHash("name", "x", "data", "{}"))
	f.Add("3:ab")
	f.Fuzz(func(t *testing.T, s string) {
		for rest := s; rest != ""; {
			elem, r, ok := nextPacked(rest)
			if !ok {
				return
			}

			if len(r) >= len(rest) || len(elem) > len(rest) {
				t.Fatalf("no progress on %q", rest)
			}

			rest = r
		}
	})
}
