/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package cpu

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
