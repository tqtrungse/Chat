/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package slice

import (
	"math"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -------
// index()
// -------

func TestIndex(t *testing.T) {
	cases := []struct {
		n    uint32
		want uint32
	}{
		// Powers of two map to themselves (ceil-log2 of a power-of-two is the exponent).
		{1, 0}, // 2^0 = 1
		{2, 1}, // 2^1 = 2
		{4, 2},
		{8, 3},
		{16, 4},
		{1024, 10},
		{1 << 31, 31},

		// Values that are not powers of two round up to the next bucket.
		{3, 2}, // fits in 2^2 = 4
		{5, 3}, // fits in 2^3 = 8
		{9, 4}, // fits in 2^4 = 16
		{1023, 10},
		{1025, 11},
	}

	for _, tc := range cases {
		got := index(tc.n)
		assert.Equal(t, tc.want, got)
	}
}

// index() keeps mapping sizes above maxPooledSize to bucket 31, which pools
// has no slot for. This pins the relationship the Get guard relies on: cross
// maxPooledSize and the next bucket is already out of range.
func TestIndex_MaxPooledSizeMatchesBucketCount(t *testing.T) {
	var p pool
	last := uint32(len(p.pools) - 1)
	assert.Equal(t, last, index(maxPooledSize), "maxPooledSize must land on the last bucket")
	assert.Equal(t, uint32(len(p.pools)), index(maxPooledSize+1), "one byte more must fall off the end")
}

// ---------------
// SlicePool.Get()
// ---------------

// A zero-length request is a legitimate outcome of size arithmetic — a frame
// carrying an empty payload, say — so it yields nil rather than panicking.
// Panicking here would turn such a frame into a remote crash.
func TestGet_Zero_ReturnsNil(t *testing.T) {
	var p pool
	assert.Nil(t, p.Get(0))
}

// A negative size cannot come from valid arithmetic, so it is a bug worth
// surfacing loudly instead of papering over with nil.
func TestGet_Negative_Panics(t *testing.T) {
	var p pool
	assert.Panics(t, func() { p.Get(-1) })
}

// Anything past the last bucket must be allocated directly: indexing pools
// with the bucket index() returns would run off the end of the array.
func TestGet_AboveMaxPooledSize_AllocatesDirect(t *testing.T) {
	var (
		p    pool
		size = maxPooledSize + 1
		s    = p.Get(size)
	)
	assert.Len(t, s, size)
	assert.Equal(t, size, cap(s))
}

func TestGet_MaxPooledSize_UsesLastBucket(t *testing.T) {
	var (
		p pool
		s = p.Get(maxPooledSize)
	)
	assert.Len(t, s, maxPooledSize)
	assert.Equal(t, maxPooledSize, cap(s))
}

func TestGet_LengthAndCapacity(t *testing.T) {
	cases := []struct {
		size    int
		wantCap int // next power-of-two >= size
	}{
		{1, 1},
		{2, 2},
		{3, 4},
		{7, 8},
		{8, 8},
		{9, 16},
		{100, 128},
		{1024, 1024},
		{1025, 2048},
	}

	var p pool
	for _, tc := range cases {
		s := p.Get(tc.size)
		assert.Len(t, s, tc.size)
		assert.Equal(t, tc.wantCap, cap(s))
	}
}

// ---------------
// SlicePool.Put()
// ---------------

func TestPut_ZeroCapIsIgnored(t *testing.T) {
	var p pool
	// Should not panic.
	p.Put([]byte{})
	p.Put(nil)
}

func TestPut_LargerThanMaxInt32_IsIgnored(t *testing.T) {
	var p pool

	// Build a slice header with a very large cap without actually allocating.
	// We only test that Put does not panic; the GC-safety of the trick is not
	// our concern here — the implementation itself does the same thing.
	s := make([]byte, 1)
	// Craft a slice whose cap exceeds MaxInt32 via unsafe so we exercise the
	// early-return branch without needing 2 GB of real memory.
	type sliceHeader struct {
		Data unsafe.Pointer
		Len  int
		Cap  int
	}
	hdr := (*sliceHeader)(unsafe.Pointer(&s))
	origCap := hdr.Cap
	hdr.Cap = math.MaxInt32 + 1

	p.Put(s) // must not panic

	// Restore so the GC can clean up properly.
	hdr.Cap = origCap
}

func TestPut_NonPowerOfTwoCap_DoesNotPanic(t *testing.T) {
	var (
		p   pool
		buf = make([]byte, 12) // cap 12, not a power of two
	)
	p.Put(buf)

	got := p.Get(8)
	assert.Len(t, got, 8)
	assert.Equal(t, 8, cap(got))
}

// Put derives the bucket from cap alone, so a buffer whose cap was truncated
// — by a []byte -> string -> []byte round trip, say — is filed by the smaller
// cap and can never climb back. The demotion is documented here because it is
// silent at the call site: nothing fails, the pool just stops serving the
// size class the buffer came from.
func TestPut_TruncatedCapIsDemoted(t *testing.T) {
	var p pool

	// Under -race, sync.Pool drops one Put in four on purpose, so a single
	// round trip cannot be asserted on. Retry until one survives.
	for range 100 {
		buf := p.Get(28)
		assert.Equal(t, 32, cap(buf), "Get(28) should come from the 32-byte bucket")
		array := unsafe.SliceData(buf)

		// Same storage, cap collapsed to the length a string header carries.
		p.Put(unsafe.Slice(array, 9))

		// index(9) = 4, and 1<<4 != 9, so Put stepped down to bucket 3.
		demoted := p.Get(8)
		if unsafe.SliceData(demoted) != array {
			continue // that Put was dropped; try again
		}

		assert.Equal(t, 8, cap(demoted), "24 bytes of the original array are unreachable")
		// The bucket it came from no longer has it.
		assert.NotSame(t, array, unsafe.SliceData(p.Get(32)))
		return
	}
	require.Fail(t, "Put never filed the truncated buffer into bucket 3")
}

// --------
// PutZeroed
// --------

func TestPutZeroed_ClearsToCapacity(t *testing.T) {
	buf := Get(24)
	assert.Equal(t, 32, cap(buf))

	full := buf[:cap(buf)]
	for i := range full {
		full[i] = 0xAA
	}

	array := unsafe.SliceData(buf)
	PutZeroed(buf)

	// Whatever comes back out of that bucket must not expose the old bytes,
	// including the len..cap tail a plain clear(buf) would have missed.
	got := Get(24)
	if unsafe.SliceData(got) != array {
		t.Skip("pool handed back a different array; nothing to assert")
	}
	assert.Equal(t, make([]byte, 32), got[:cap(got)])
}

// --------------------------------------
// Round-trip: Put then Get remains usable
// --------------------------------------

func TestRoundTrip_MultipleSizes(t *testing.T) {
	var (
		p     pool
		sizes = []int{1, 7, 8, 63, 64, 255, 256, 1023, 1024}
	)

	for _, size := range sizes {
		s := p.Get(size)
		assert.Len(t, s, size)

		// Write a sentinel pattern before returning the storage to the pool.
		for i := range s {
			s[i] = byte((i % 255) + 1)
		}

		p.Put(s)

		s2 := p.Get(size)
		assert.Len(t, s2, size)
		assert.Equal(t, 1<<index(uint32(size)), cap(s2))
		for i := range s2 {
			s2[i] = 0x55
		}
		p.Put(s2)
	}
}

// The whole point of the package is handing the same storage back out. Without
// this, replacing Get's body with a plain make() would still pass every other
// test in this file.
func TestGetPut_ReusesUnderlyingArray(t *testing.T) {
	var (
		p      pool
		reused = false
	)

	// A GC between Put and Get can drain the pool, so one round trip is not
	// guaranteed to reuse. Retrying makes a pool that NEVER reuses — the
	// regression this guards against — impossible to pass by luck.
	for range 100 {
		buf := p.Get(100)
		array := unsafe.SliceData(buf)
		p.Put(buf)

		again := p.Get(100)
		reused = unsafe.SliceData(again) == array
		p.Put(again)
		if reused {
			break
		}
	}
	assert.True(t, reused, "Get should hand back storage that Put returned")
}

// ------------------------------
// Built-in package-level helpers
// ------------------------------

func TestBuiltinGet_Put(t *testing.T) {
	s := Get(32)
	assert.Len(t, s, 32)

	for i := range s {
		s[i] = 0xFF
	}
	Put(s)

	s2 := Get(32)
	assert.Len(t, s2, 32)
}

// -----------
// Concurrency
// -----------

func TestConcurrentGetPut(t *testing.T) {
	const goroutines = 64
	const iterations = 1000

	var (
		p    pool
		done = make(chan struct{})
	)

	for range goroutines {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := range iterations {
				size := (i % 32) + 1 // 1..32
				s := p.Get(size)
				assert.Len(t, s, size)

				// Initialize the slice before use; Get may return dirty storage.
				for j := range s {
					s[j] = 0x55
				}
				p.Put(s)
			}
		}()
	}

	for range goroutines {
		<-done
	}
}

// ----------
// Benchmarks
// ----------

func BenchmarkGet_PowerOfTwo(b *testing.B) {
	var p pool
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := p.Get(64)
		p.Put(s)
	}
}

func BenchmarkGet_NonPowerOfTwo(b *testing.B) {
	var p pool
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := p.Get(100)
		p.Put(s)
	}
}

func BenchmarkGetPut_Parallel(b *testing.B) {
	var p pool
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s := p.Get(256)
			p.Put(s)
		}
	})
}
