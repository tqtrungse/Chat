/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pkg

import (
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// refNextPow2 serves as a reference: it is slow but demonstrably correct;
// it avoids using math/bits or unsafe, so it is not subject to issues
// related to word-size dependencies.
func refNextPow2(v int) uint {
	if v <= 1 {
		return 1
	}
	p := uint(1)
	for p < uint(v) {
		p <<= 1
	}
	return p
}

// safeNextPow2 invokes the function and converts panics into errors to provide clear error reporting
// (an old 32-bit bug would otherwise trigger a "negative shift amount" panic).
func safeNextPow2(v int) (got uint, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return NextPowerOfTwo(v), nil
}

func checkOne(t *testing.T, v int, want uint) {
	t.Helper()
	got, err := safeNextPow2(v)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// Table of fixed values; use only constants that are compatible with both 32-bit and 64-bit architectures.
func TestNextPowerOfTwo_Table(t *testing.T) {
	cases := []struct {
		in   int
		want uint
	}{
		{-5, 1}, {0, 1}, {1, 1},
		{2, 2}, {3, 4}, {4, 4}, {5, 8}, {7, 8}, {8, 8}, {9, 16},
		{1023, 1024}, {1024, 1024}, {1025, 2048},
		{1 << 20, 1 << 20}, {1<<20 + 1, 1 << 21},
		{1 << 30, 1 << 30}, {1<<30 + 1, 1 << 31},
		{math.MaxInt32, 1 << 31},
	}
	for _, c := range cases {
		t.Run(fmt.Sprint(c.in), func(t *testing.T) { checkOne(t, c.in, c.want) })
	}
}

// Scan every bit position up to bits.UintSize: catch "32-bit-only" errors and edge-case errors
// (at 2^k-1, 2^k, and 2^k+1) up to the maximum valid value.
func TestNextPowerOfTwo_EveryBit(t *testing.T) {
	// Maximum k = UintSize-2, ensuring v+1 does not overflow an int; the maximum result is 2^(UintSize-1).
	for k := 0; k <= bits.UintSize-2; k++ {
		v := 1 << k
		t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
			checkOne(t, v, uint(1)<<k)       // correct power of 2
			checkOne(t, v+1, uint(1)<<(k+1)) // just passed
			if k >= 2 {
				checkOne(t, v-1, uint(1)<<k) // immediately below (k=1 → v-1=1 → 1, ignored)
			}
		})
	}
}

// Maximum value of int: the result must be 2^(UintSize-1), with no overflow.
func TestNextPowerOfTwo_MaxInt(t *testing.T) {
	checkOne(t, math.MaxInt, uint(1)<<(bits.UintSize-1))
	checkOne(t, math.MaxInt-1, uint(1)<<(bits.UintSize-1))
}

// Verify the property using random values (with a fixed seed for reproducibility).
func TestNextPowerOfTwo_Properties(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for range 20000 {
		// Choose a random bit length to ensure an even distribution across numerical magnitudes,
		// avoiding a concentration toward very large numbers.
		n := rng.Intn(bits.UintSize-1) + 1
		v := int(rng.Uint64() & (uint64(1)<<n - 1))
		checkProperties(t, v)
	}
}

func checkProperties(t *testing.T, v int) {
	t.Helper()
	got, err := safeNextPow2(v)
	require.NoError(t, err)
	require.Falsef(t, got == 0 || got&(got-1) != 0, "NextPowerOfTwo(%d) = %d: not power of 2", v, got)

	if v > 1 {
		require.GreaterOrEqual(t, got, uint(v))
		require.Less(t, got/2, uint(v))
	} else if got != 1 {
		require.Failf(t, "", "NextPowerOfTwo(%d) = %d, want 1", v, got)
	}

	want := refNextPow2(v)
	require.Equal(t, want, got)
}

// Fuzz (Go >= 1.18): go test -fuzz=FuzzNextPowerOfTwo -fuzztime=30s
func FuzzNextPowerOfTwo(f *testing.F) {
	for _, s := range []int{-1, 0, 1, 2, 3, 4, 5, 1 << 20, 1<<20 + 1, math.MaxInt32, math.MaxInt} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, v int) {
		checkProperties(t, v)
	})
}

// For any given entrySize and capacity, CacheRemap(0..capacity-1) must be a bijection
// onto [0, capacity). Otherwise, two elements could overwrite each other (and some slots would never be used).
func TestCacheRemap_IsPermutation(t *testing.T) {
	capacities := []uint32{1, 2, 3, 4, 5, 7, 8, 16, 64, 100, 1000, 1024, 4096, 1 << 16}
	maxSize := uint32(halfCacheLineSize) + 1 // including the branch where entrySize >= halfCacheLineSize

	for entrySize := uint32(1); entrySize <= maxSize; entrySize++ {
		for _, capacity := range capacities {
			entSize, capa := entrySize, capacity
			t.Run(fmt.Sprintf("size=%d/cap=%d", entSize, capa), func(t *testing.T) {
				owner := make([]uint32, capa) // store the index+1 of the element that has occupied the slot
				for i := range capa {
					out := CacheRemap(i, capa, entSize)
					require.Less(t, out, capa)
					require.Zero(t, owner[out])
					owner[out] = i + 1
				}
			})
		}
	}
}

// index >= capacity must yield the same result as index % capacity.
func TestCacheRemap_WrapsIndex(t *testing.T) {
	for _, size := range []uint32{1, 3, 12, 20, 64} {
		for _, capacity := range []uint32{8, 1024} {
			for i := range capacity {
				want := CacheRemap(i, capacity, size)
				got := CacheRemap(i+3*capacity, capacity, size)
				require.Equalf(
					t,
					want, got,
					"size=%d cap=%d: CacheRemap(%d)=%d but CacheRemap(%d)=%d",
					size, capacity, i, want, i+3*capacity, got,
				)
			}
		}
	}
}

// Prevent "fixing" by always returning the rawIndex: for small entries (>= 2 entries per half-line)
// and a capacity larger than a single group, a remap must actually shift the index.
func TestCacheRemap_StillSpreads(t *testing.T) {
	for size := uint32(1); size <= uint32(halfCacheLineSize)/2; size++ {
		if CacheRemap(1, 1024, size) == 1 {
			assert.Failf(t, "", "entrySize=%d: remapping to identity eliminates the cache-line-spreading effect.", size)
		}
	}
}

// entrySize == 0 must not result in division by zero.
func TestCacheRemap_ZeroEntrySize(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			require.Failf(t, "", "CacheRemap(5, 16, 0) panic: %v", r)
		}
	}()
	_ = CacheRemap(5, 16, 0)
}
