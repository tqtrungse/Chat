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
	"fmt"
	"math/bits"
	"os"
	"strconv"
	"strings"
	"unsafe"

	"golang.org/x/sys/cpu"
)

const (
	PtrSize       = unsafe.Sizeof(uintptr(0))
	CacheLineSize = unsafe.Sizeof(cpu.CacheLinePad{})

	ordinalBits       = uint64(8) // max 256 replicas per service
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
	return 1 << bits.Len(uint(v-1))
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
	return index % capacity // capacity == 0 vẫn panic chia cho 0, như hành vi cũ
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

// MachineIDFromHostname get machine ID from K8S StatefullSet host name.
func MachineIDFromHostname(serviceID uint64) (uint64, error) {
	h, err := os.Hostname()
	if err != nil {
		return 0, err
	}

	i := strings.LastIndex(h, "-")
	if i < 0 {
		return 0, fmt.Errorf("no ordinal in hostname %q", h)
	}

	n, err := strconv.Atoi(h[i+1:])
	if err != nil || n < 0 || n >= 1<<ordinalBits {
		return 0, fmt.Errorf("invalid ordinal in hostname %q", h)
	}
	return serviceID<<ordinalBits | uint64(n), nil
}

func Mix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
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
