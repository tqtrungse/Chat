//go:build 386 || arm || mips || mipsle

/*
 * Copyright 2014 The Go Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 *
 * This file contains modifications made by tqtrungse@gmail.com
 * to the Go runtime.
 */

package hash

import (
	"unsafe"

	"xxx/pkg"
)

// Modified from runtime.memhashfallback.

// Int
//
// Hashes for [~int | ~uint].
func Int[T ~int | ~uint](v T, seed uint64) uint64 {
	if pkg.PtrSize == 8 {
		return hash64(uint64(v), seed)
	}
	return hash32(uint32(v), seed)
}

// Int8
//
// Hashes for [~int8 | ~uint8].
func Int8[T ~int8 | ~uint8](v T, seed uint64) uint64 {
	a, b := mix32(uint32(seed), uint32(1^hashkey[0]))
	b ^= uint32(v)
	a, b = mix32(a, b)
	a, b = mix32(a, b)
	return uint64(a ^ b)
}

// Int16
//
// Hashes for [~int16 | ~uint16].
func Int16[T ~int16 | ~uint16](v T, seed uint64) uint64 {
	a, b := mix32(uint32(seed), uint32(2^hashkey[0]))
	b ^= uint32(uint8(v)) | uint32(v>>8)<<8
	a, b = mix32(a, b)
	a, b = mix32(a, b)
	return uint64(a ^ b)
}

// Int32
//
// Hashes for [~int32 | ~uint32].
func Int32[T ~int32 | ~uint32](v T, seed uint64) uint64 {
	return hash32(uint32(v), seed)
}

// Int64
//
// Hashes for [~int64 | ~uint64].
func Int64[T ~int64 | ~uint64](v T, seed uint64) uint64 {
	return hash64(uint64(v), seed)
}

// Uintptr
//
// Hashes for [~uintptr].
func Uintptr[T ~uintptr](v T, seed uint64) uint64 {
	if pkg.PtrSize == 8 {
		return hash64(uint64(v), seed)
	}
	return hash32(uint32(v), seed)
}

// Float32
//
// Hashes for [~float32].
func Float32[T ~float32](v T, seed uint64) uint64 {
	switch {
	case v == 0:
		return c1 * (c0 ^ seed) // +0, -0
	case v != v:
		return c1 * (c0 ^ seed ^ MakeSeed())
	default:
		return hash32(uint32(v), seed)
	}
}

// Float64
//
// Hashes for [~float64].
func Float64[T ~float64](v T, seed uint64) uint64 {
	switch {
	case v == 0:
		return c1 * (c0 ^ seed) // +0, -0
	case v != v:
		return c1 * (c0 ^ seed ^ MakeSeed())
	default:
		return hash64(uint64(v), seed)
	}
}

func hash32(v uint32, seed uint64) uint64 {
	a, b := mix32(uint32(seed), uint32(4^hashkey[0]))
	a ^= v
	b ^= v
	a, b = mix32(a, b)
	a, b = mix32(a, b)
	return uint64(a ^ b)
}

func hash64(v uint64, seed uint64) uint64 {
	a, b := mix32(uint32(seed), uint32(8^hashkey[0]))
	a ^= uint32(v)
	b ^= uint32(v >> 32)
	a, b = mix32(a, b)
	a, b = mix32(a, b)
	return uint64(a ^ b)
}

func hashContiguousBytes(ptr unsafe.Pointer, seed uint64, length uintptr) uint64 {
	a, b := mix32(uint32(seed), uint32(uint64(length)^hashkey[0]))
	if length == 0 {
		return uint64(a ^ b)
	}
	for ; length > 8; length -= 8 {
		a ^= uint32(r4(ptr))
		b ^= uint32(r4(seek(ptr, 4)))
		a, b = mix32(a, b)
		ptr = seek(ptr, 8)
	}
	if length >= 4 {
		a ^= uint32(r4(ptr))
		b ^= uint32(r4(seek(ptr, length-4)))
	} else {
		t := uint32(*(*byte)(ptr))
		t |= uint32(*(*byte)(seek(ptr, length>>1))) << 8
		t |= uint32(*(*byte)(seek(ptr, length-1))) << 16
		b ^= t
	}
	a, b = mix32(a, b)
	a, b = mix32(a, b)
	return uint64(a ^ b)
}

func mix32(a, b uint32) (uint32, uint32) {
	c := uint64(a^uint32(hashkey[1])) * uint64(b^uint32(hashkey[2]))
	return uint32(c), uint32(c >> 32)
}
