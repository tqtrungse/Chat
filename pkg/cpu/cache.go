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

package cpu

import (
	"math/bits"
	"unsafe"

	"golang.org/x/sys/cpu"
)

const (
	CacheLineSize = unsafe.Sizeof(cpu.CacheLinePad{})

	halfCacheLineSize = uint32(CacheLineSize / 2)
)

// CacheRemap remaps an index to spread small entries across cache-line halves.
//
// When an entry occupies at least half a cache line, the original modulo index
// is returned because cache-line-level spreading provides little benefit.
//
// For smaller entries, the capacity is logically divided into groups sized by
// the number of entries that fit in half a cache line. The index is then
// transposed between groups so consecutive logical indices are distributed
// across different cache-line halves rather than clustered together.
//
// Condition: capacity > 0. When capacity is a power of 2, the result is a
// permutation of [0, capacity). For other values of capacity, the function
// does not perform a remapping and instead returns index % capacity
// (which is still a permutation, though cache optimization is lost).
//
// Forked from https://github.com/bytedance/gopkg
func CacheRemap(index, capacity, entrySize uint32) uint32 {
	if capacity&(capacity-1) == 0 && capacity != 0 {
		return cacheRemapPow2(index, capacity, entrySize)
	}
	return index % capacity
}

func cacheRemapPow2(index, capacity, entrySize uint32) uint32 {
	rawIndex := index & (capacity - 1)
	if entrySize == 0 || entrySize >= halfCacheLineSize {
		return rawIndex
	}

	// Number of entries in half a cache line, rounded down to a power of 2.
	e := uint32(1) << (bits.Len32(halfCacheLineSize/entrySize) - 1)
	if capacity <= e {
		return rawIndex
	}

	eBits := uint(bits.TrailingZeros32(e))
	groupBits := uint(bits.TrailingZeros32(capacity)) - eBits // groupCount = 1 << groupBits

	groupNum := rawIndex & (1<<groupBits - 1)
	groupIdx := rawIndex >> groupBits // always < e
	return groupNum<<eBits | groupIdx
}
