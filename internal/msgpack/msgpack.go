// Package msgpack is a minimal, allocation-lean MessagePack encoder producing
// the compact encodings the BullMQ Lua scripts (cmsgpack) expect.
package msgpack

import (
	"encoding/binary"
	"math"
)

// Buf is a minimal MessagePack encoder producing the compact encodings
// the BullMQ Lua scripts (cmsgpack) expect. It writes exactly what is asked,
// so map lengths always match their entries.
type Buf struct {
	b []byte
}

// Bytes returns the encoded bytes.
func (m *Buf) Bytes() []byte { return m.b }

// Grow ensures room for n more bytes, so a caller that knows the encoded
// size up front avoids repeated regrowth.
func (m *Buf) Grow(n int) {
	if cap(m.b)-len(m.b) < n {
		m.b = append(make([]byte, 0, len(m.b)+n), m.b...)
	}
}

// Nil writes nil.
func (m *Buf) Nil() { m.b = append(m.b, 0xc0) }

// Bool writes a boolean.
func (m *Buf) Bool(v bool) {
	if v {
		m.b = append(m.b, 0xc3)
	} else {
		m.b = append(m.b, 0xc2)
	}
}

// Uint writes an unsigned integer in its smallest encoding.
func (m *Buf) Uint(v uint64) {
	switch {
	case v < 1<<7:
		m.b = append(m.b, byte(v))
	case v <= math.MaxUint8:
		m.b = append(m.b, 0xcc, byte(v))
	case v <= math.MaxUint16:
		m.b = binary.BigEndian.AppendUint16(append(m.b, 0xcd), uint16(v))
	case v <= math.MaxUint32:
		m.b = binary.BigEndian.AppendUint32(append(m.b, 0xce), uint32(v))
	default:
		m.b = binary.BigEndian.AppendUint64(append(m.b, 0xcf), v)
	}
}

// Int writes a signed integer in its smallest encoding.
func (m *Buf) Int(v int64) {
	if v >= 0 {
		m.Uint(uint64(v))
		return
	}
	// Two's complement encodings: the narrowing conversions are intended.
	//nolint:gosec // G115: v is range-checked by each case.
	switch {
	case v >= -32:
		m.b = append(m.b, byte(int8(v)))
	case v >= math.MinInt8:
		m.b = append(m.b, 0xd0, byte(int8(v)))
	case v >= math.MinInt16:
		m.b = binary.BigEndian.AppendUint16(append(m.b, 0xd1), uint16(int16(v)))
	case v >= math.MinInt32:
		m.b = binary.BigEndian.AppendUint32(append(m.b, 0xd2), uint32(int32(v)))
	default:
		m.b = binary.BigEndian.AppendUint64(append(m.b, 0xd3), uint64(v))
	}
}

// Str writes a string.
func (m *Buf) Str(s string) {
	n := len(s)
	switch {
	case n < 32:
		m.b = append(m.b, 0xa0|byte(n))
	case n <= math.MaxUint8:
		m.b = append(m.b, 0xd9, byte(n))
	case n <= math.MaxUint16:
		m.b = binary.BigEndian.AppendUint16(append(m.b, 0xda), uint16(n))
	default:
		m.b = binary.BigEndian.AppendUint32(append(m.b, 0xdb), uint32(n)) //nolint:gosec // G115: n is range-checked by the enclosing case
	}

	m.b = append(m.b, s...)
}

// ArrayLen writes an array header for n elements, which must follow.
func (m *Buf) ArrayLen(n int) {
	switch {
	case n < 16:
		m.b = append(m.b, 0x90|byte(n)) //nolint:gosec // G115: n is range-checked by the enclosing case
	case n <= math.MaxUint16:
		m.b = binary.BigEndian.AppendUint16(append(m.b, 0xdc), uint16(n))
	default:
		m.b = binary.BigEndian.AppendUint32(append(m.b, 0xdd), uint32(n)) //nolint:gosec // G115: n is range-checked by the enclosing case
	}
}

// MapLen writes a map header for n key/value pairs, which must follow.
func (m *Buf) MapLen(n int) {
	switch {
	case n < 16:
		m.b = append(m.b, 0x80|byte(n)) //nolint:gosec // G115: n is range-checked by the enclosing case
	case n <= math.MaxUint16:
		m.b = binary.BigEndian.AppendUint16(append(m.b, 0xde), uint16(n))
	default:
		m.b = binary.BigEndian.AppendUint32(append(m.b, 0xdf), uint32(n)) //nolint:gosec // G115: n is range-checked by the enclosing case
	}
}

// Raw appends already-encoded bytes.
func (m *Buf) Raw(b []byte) { m.b = append(m.b, b...) }

// Map encodes a map entry by entry into a single buffer and emits it
// with a header whose length always matches the number of entries.
//
// The header length is only known at the end, so headerRoom bytes are
// reserved up front and the header is written right before the body: the
// whole map costs one allocation (it is built for every script call).
type Map struct {
	body Buf
	n    int
}

// headerRoom is the size of the largest map header (map32).
const headerRoom = 5

func (mm *Map) key(k string) {
	if mm.body.b == nil {
		// 256 bytes fit the per-job option maps (token, keepJobs, limiter…)
		// without regrowing.
		mm.body.b = make([]byte, headerRoom, 256)
	}

	mm.n++
	mm.body.Str(k)
}

// PutInt adds an integer entry.
func (mm *Map) PutInt(key string, v int64) {
	mm.key(key)
	mm.body.Int(v)
}

// PutStr adds a string entry.
func (mm *Map) PutStr(key, v string) {
	mm.key(key)
	mm.body.Str(v)
}

// PutBool adds a boolean entry.
func (mm *Map) PutBool(key string, v bool) {
	mm.key(key)
	mm.body.Bool(v)
}

// PutRaw adds an entry whose value is already encoded.
func (mm *Map) PutRaw(key string, v []byte) {
	mm.key(key)
	mm.body.Raw(v)
}

// BeginMap starts a nested map of exactly n entries under key; write them
// with the returned KV before adding the next entry. Unlike a callback API,
// this keeps a local Map off the heap.
func (mm *Map) BeginMap(key string, n int) KV {
	mm.key(key)
	mm.body.MapLen(n)

	return KV{&mm.body}
}

// KV writes the key/value pairs of a nested map.
type KV struct{ b *Buf }

// Int writes an integer entry.
func (kv KV) Int(key string, v int64) {
	kv.b.Str(key)
	kv.b.Int(v)
}

// Encode appends the map to m.
func (mm *Map) Encode(m *Buf) {
	m.MapLen(mm.n)

	if mm.n > 0 {
		m.Raw(mm.body.b[headerRoom:])
	}
}

// Bytes returns the encoded map, reusing the reserved header room.
func (mm *Map) Bytes() []byte {
	if mm.n == 0 {
		return []byte{0x80}
	}

	// Write the header right before the body, into the reserved room.
	b := mm.body.b

	var start int

	switch n := mm.n; {
	case n < 16:
		start = headerRoom - 1
		b[start] = 0x80 | byte(n) //nolint:gosec // G115: n < 16
	case n <= math.MaxUint16:
		start = headerRoom - 3
		b[start] = 0xde
		binary.BigEndian.PutUint16(b[start+1:], uint16(n))
	default:
		start = 0
		b[0] = 0xdf
		binary.BigEndian.PutUint32(b[1:], uint32(n)) //nolint:gosec // G115: a map never holds 2^32 entries
	}

	return mm.body.b[start:]
}
