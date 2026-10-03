/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package slice

import (
	"math"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
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

// ---------------
// SlicePool.Get()
// ---------------

func TestGet_ZeroOrNegative(t *testing.T) {
	var p Pool

	s := p.Get(0)
	assert.Nil(t, s)

	s = p.Get(-1)
	assert.Nil(t, s)
}

func TestGet_LargerThanMaxInt32_AllocatesDirect(t *testing.T) {
	var p Pool

	size := math.MaxInt32 + 1
	s := p.Get(size)
	assert.Len(t, s, size)
}

func TestGet_LengthAndCapacity(t *testing.T) {
	var p Pool

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

	for _, tc := range cases {
		s := p.Get(tc.size)
		assert.Len(t, s, tc.size)
		assert.Equal(t, tc.wantCap, cap(s))
	}
}

func TestGet_ReturnsZeroedSlice(t *testing.T) {
	var p Pool

	// Populate a slice with non-zero data, return it, then Get again —
	// the slice coming back must be zeroed.
	s := p.Get(64)
	for i := range s {
		s[i] = 0xFF
	}
	p.Put(s)

	s2 := p.Get(64)
	for _, b := range s2 {
		assert.Zero(t, b)
	}
}

// ---------------
// SlicePool.Put()
// ---------------

func TestPut_ZeroCapIsIgnored(t *testing.T) {
	var p Pool
	// Should not panic.
	p.Put([]byte{})
	p.Put(nil)
}

func TestPut_LargerThanMaxInt32_IsIgnored(t *testing.T) {
	var p Pool

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

func TestPut_NonPowerOfTwo_BucketDowngrade(t *testing.T) {
	var p Pool

	// Allocate from pool (cap = power-of-two), dirty it, return it, then
	// manually create a slice with a non-power-of-two cap and Put that.
	// After the Put it should be retrievable at the lower bucket size.
	//
	// We verify indirectly: Put(non-pow2 cap slice of cap 12) stores into
	// bucket 3 (cap 8). A subsequent Get(8) should reuse that memory.

	buf := make([]byte, 12, 12) // cap 12, not a power of two
	for i := range buf {
		buf[i] = 0xAB
	}

	p.Put(buf) // should go into bucket for 8 (index 3)

	got := p.Get(8)
	// The slice must be zeroed regardless of which pool path was taken.
	for _, b := range got {
		assert.Zero(t, b)
	}
	_ = got
}

// --------------------------------------
// Round-trip: Put then Get reuses memory
// --------------------------------------

func TestRoundTrip_SameUnderlyingArray(t *testing.T) {
	var p Pool

	s := p.Get(64)
	ptr := unsafe.SliceData(s)

	p.Put(s)

	s2 := p.Get(64)
	ptr2 := unsafe.SliceData(s2)

	assert.Equal(t, ptr, ptr2)
}

func TestRoundTrip_MultipleSizes(t *testing.T) {
	var p Pool

	sizes := []int{1, 7, 8, 63, 64, 255, 256, 1023, 1024}
	for _, size := range sizes {
		s := p.Get(size)
		assert.Len(t, s, size)

		// Write a sentinel pattern.
		for i := range s {
			s[i] = byte(i & 0xFF)
		}

		p.Put(s)

		s2 := p.Get(size)
		assert.Len(t, s2, size)
		// Must come back zeroed.
		for _, b := range s2 {
			assert.Zero(t, b)
		}
	}
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
	for _, b := range s2 {
		assert.Zero(t, b)
	}
}

// -----------
// Concurrency
// -----------

func TestConcurrentGetPut(t *testing.T) {
	var p Pool
	const goroutines = 64
	const iterations = 1000

	done := make(chan struct{})
	for range goroutines {
		go func() {
			defer func() { done <- struct{}{} }()
			for i := range iterations {
				size := (i % 32) + 1 // 1..32
				s := p.Get(size)
				assert.Len(t, s, size)

				for _, b := range s {
					if !assert.Zerof(t, b, "Get(%d): returned non-zero byte", size) {
						break
					}
				}
				// Write and return.
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
	var p Pool
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := p.Get(64)
		p.Put(s)
	}
}

func BenchmarkGet_NonPowerOfTwo(b *testing.B) {
	var p Pool
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := p.Get(100)
		p.Put(s)
	}
}

func BenchmarkGetPut_Parallel(b *testing.B) {
	var p Pool
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s := p.Get(256)
			p.Put(s)
		}
	})
}
