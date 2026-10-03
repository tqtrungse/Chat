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

package pkg

import (
	"math/bits"
	"unsafe"

	"golang.org/x/sys/cpu"
)

const (
	PtrSize       = unsafe.Sizeof(uintptr(0))
	CacheLineSize = unsafe.Sizeof(cpu.CacheLinePad{})

	halfCacheLineSize = uint32(CacheLineSize / 2)
)

func ZeroValue[T any]() T {
	var zero T
	return zero
}

// NextPowerOfTwo
//
// Calculates the power of 2 that is greater than or equal to v.
func NextPowerOfTwo(v int) uint {
	if v <= 1 {
		return 1
	}
	nBits := int(unsafe.Sizeof(uint(0))) << 3
	return 1 << (nBits - bits.LeadingZeros64(uint64(v)-1))
}

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
// The returned value is always an index in the range [0, capacity).
//
// Forked from https://github.com/bytedance/gopkg
func CacheRemap(index, capacity, entrySize uint32) uint32 {
	rawIndex := index % capacity
	if entrySize >= halfCacheLineSize {
		return rawIndex
	}

	entriesPerHalfLine := halfCacheLineSize / entrySize
	if capacity <= entriesPerHalfLine {
		return rawIndex
	}

	groupCount := capacity / entriesPerHalfLine
	groupNum := rawIndex % groupCount
	groupIdx := rawIndex / groupCount
	return groupNum*entriesPerHalfLine + groupIdx
}

//// GetRAM
////
//// Returns current stat of RAM with byte.
//func GetRAM() (total uint64, used uint64, available uint64, err error) {
//	stat, err := mem.VirtualMemory()
//	if err != nil {
//		return 0, 0, 0, err
//	}
//	return stat.Total, stat.Used, stat.Available, nil
//}

//type iFace struct {
//	tab  unsafe.Pointer
//	data unsafe.Pointer
//}
//
//func GetInterfaceAddr(i interface{}) uintptr {
//	p := uintptr((*iFace)(unsafe.Pointer(&i)).data)
//	return p
//}
