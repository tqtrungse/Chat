/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package bs

import (
	"math"
	"math/rand/v2"
	"strconv"
	"testing"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// powersOf10[i] is 10^i. The list stops at 10^19 because 10^20 overflows uint64.
var powersOf10 = [20]uint64{
	1,
	10,
	100,
	1000,
	10000,
	100000,
	1000000,
	10000000,
	100000000,
	1000000000,
	10000000000,
	100000000000,
	1000000000000,
	10000000000000,
	100000000000000,
	1000000000000000,
	10000000000000000,
	100000000000000000,
	1000000000000000000,
	10000000000000000000,
}

// ------------
// digitCount()
// ------------

// Zero has no base-10 exponent, so it is the one value a log-based count cannot
// answer and a boundary-comparison count must special-case.
func TestDigitCount_Zero(t *testing.T) {
	assert.Equal(t, 1, digitCount(0))
}

// Every power of ten is a bucket edge: 10^i gains a digit over 10^i - 1. These
// are exactly where a float64 log is allowed to be off by an ulp and round the
// wrong way, so each edge is pinned from both sides.
func TestDigitCount_PowersOfTenAndBoundaries(t *testing.T) {
	for i, p := range powersOf10 {
		digits := i + 1

		assert.Equal(t, digits, digitCount(p), "10^%d = %d", i, p)
		assert.Equal(t, digits, digitCount(p+1), "10^%d + 1 = %d", i, p+1)

		// 10^0 - 1 is zero, which carries one digit rather than none.
		wantBelow := max(digits-1, 1)
		assert.Equal(t, wantBelow, digitCount(p-1), "10^%d - 1 = %d", i, p-1)
	}
}

func TestDigitCount_MaxUint64(t *testing.T) {
	assert.Equal(t, 20, digitCount(math.MaxUint64))
}

// Above 2^53 a uint64 cannot round-trip through float64, so the old
// math.Log10(float64(v)) implementation saw these values rounded up to the next
// power of ten and returned one digit too many.
func TestDigitCount_ValuesUnrepresentableAsFloat64(t *testing.T) {
	cases := []struct {
		value uint64
		want  int
	}{
		{999_999_999_999_999_999, 18},
		{9_999_999_999_999_999, 16},
		{99_999_999_999_999_999, 17},
		{9_999_999_999_999_999_999, 19},
		{math.MaxUint64 - 1, 20},
		{1 << 53, 16},
		{(1 << 53) + 1, 16},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.want, digitCount(tc.value), "value %d", tc.value)
	}
}

// digitCount sizes the buffer that strconv.AppendUint then fills, so any
// disagreement with the formatted length either wastes pool capacity or forces
// append to reallocate and drop the pooled array. strconv is the ground truth.
func TestDigitCount_MatchesFormattedLength(t *testing.T) {
	assertMatches := func(v uint64) {
		assert.Equal(t, len(strconv.FormatUint(v, 10)), digitCount(v), "value %d", v)
	}

	for v := uint64(0); v <= 100_000; v++ {
		assertMatches(v)
	}

	// Dense coverage of the high range the exhaustive loop cannot reach.
	for range 200_000 {
		assertMatches(rand.Uint64())
	}

	// Each decade, sampled around and across its edge.
	for i, p := range powersOf10 {
		assertMatches(p)
		assertMatches(p - 1)
		if i+1 < len(powersOf10) {
			assertMatches(p + rand.Uint64N(powersOf10[i+1]-p))
		}
	}
}

func FuzzDigitCount(f *testing.F) {
	for _, p := range powersOf10 {
		f.Add(p)
		f.Add(p - 1)
	}
	f.Add(uint64(0))
	f.Add(uint64(math.MaxUint64))

	f.Fuzz(func(t *testing.T, v uint64) {
		assert.Equal(t, len(strconv.FormatUint(v, 10)), digitCount(v))
	})
}

// ------------
// U64ToBytes()
// ------------

func TestU64ToBytes(t *testing.T) {
	values := []uint64{
		0, 1, 9, 10, 99, 100, 999, 1000, 12345,
		999_999_999, 1_000_000_000,
		999_999_999_999_999_999,
		math.MaxUint64,
	}

	for _, v := range values {
		buf := U64ToBytes(v)
		assert.Equal(t, strconv.FormatUint(v, 10), string(buf), "value %d", v)
		slicepool.Put(buf)
	}
}

// The buffer is sized up front precisely so the append below it never grows:
// a reallocation would strand the pooled array and hand Put a slice the pool
// did not issue.
func TestU64ToBytes_FitsWithoutReallocating(t *testing.T) {
	for _, p := range powersOf10 {
		for _, v := range []uint64{p - 1, p, p + 1} {
			buf := U64ToBytes(v)
			require.LessOrEqual(t, len(buf), cap(buf), "value %d", v)
			assert.Equal(t, digitCount(v), len(buf), "value %d", v)
			slicepool.Put(buf)
		}
	}
}

// --------
// Concat()
// --------

func TestConcat(t *testing.T) {
	cases := []struct {
		prefix string
		value  uint64
	}{
		{"", 0},
		{"", math.MaxUint64},
		{"key-", 1},
		{"prefix_", 12345},
		{"x", 999_999_999_999_999_999},
		{"a-rather-long-prefix:", math.MaxUint64},
	}

	for _, tc := range cases {
		buf := Concat(tc.prefix, tc.value)
		assert.Equal(
			t,
			tc.prefix+strconv.FormatUint(tc.value, 10),
			string(buf),
			"prefix %q value %d", tc.prefix, tc.value,
		)
		slicepool.Put(buf)
	}
}

func TestConcat_FitsWithoutReallocating(t *testing.T) {
	const prefix = "metric."

	for _, p := range powersOf10 {
		for _, v := range []uint64{p - 1, p, p + 1} {
			buf := Concat(prefix, v)
			require.LessOrEqual(t, len(buf), cap(buf), "value %d", v)
			assert.Equal(t, len(prefix)+digitCount(v), len(buf), "value %d", v)
			slicepool.Put(buf)
		}
	}
}

// ----------
// Benchmarks
// ----------

func BenchmarkDigitCount(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		digitCount(1234567890)
	}
}

func BenchmarkU64ToBytes(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		slicepool.Put(U64ToBytes(1234567890))
	}
}

func BenchmarkConcat(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		slicepool.Put(Concat("prefix_", 1234567890))
	}
}
