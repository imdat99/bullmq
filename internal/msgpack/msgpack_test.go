package msgpack

import (
	"reflect"
	"strconv"
	"testing"

	vmsgpack "github.com/vmihailenco/msgpack/v5"
)

// decode checks the encoder output with an independent decoder.
func decode(t *testing.T, b []byte) any {
	t.Helper()

	var v any
	if err := vmsgpack.Unmarshal(b, &v); err != nil {
		t.Fatalf("decode %x: %v", b, err)
	}

	return v
}

func TestBufScalars(t *testing.T) {
	var b Buf
	b.ArrayLen(7)
	b.Nil()
	b.Bool(true)
	b.Int(-200)
	b.Uint(70000)
	b.Str("héllo")
	b.Int(-1 << 40)
	b.Str(string(make([]byte, 300)))

	got, ok := decode(t, b.Bytes()).([]any)
	if !ok || len(got) != 7 {
		t.Fatalf("decoded %#v", got)
	}

	if got[0] != nil || got[1] != true || got[4] != "héllo" || len(got[6].(string)) != 300 {
		t.Fatalf("decoded %#v", got)
	}

	if reflect.ValueOf(got[2]).Int() != -200 || reflect.ValueOf(got[3]).Uint() != 70000 || reflect.ValueOf(got[5]).Int() != -1<<40 {
		t.Fatalf("decoded integers %#v", got)
	}
}

func TestMapEmptyAndLarge(t *testing.T) {
	var empty Map
	if got := decode(t, empty.Bytes()); !reflect.DeepEqual(got, map[string]any{}) {
		t.Fatalf("empty map decoded as %#v", got)
	}

	var big Map // > 15 entries needs a map16 header
	for i := range 40 {
		big.PutInt("k"+strconv.Itoa(i), int64(i))
	}

	got, ok := decode(t, big.Bytes()).(map[string]any)
	if !ok || len(got) != 40 {
		t.Fatalf("large map decoded as %#v", got)
	}

	var outer Buf
	big.Encode(&outer)

	if got := decode(t, outer.Bytes()).(map[string]any); len(got) != 40 {
		t.Fatalf("encode() produced %d entries", len(got))
	}
}
