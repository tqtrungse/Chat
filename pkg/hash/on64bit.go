//go:build amd64 || arm64 || loong64 || mips64 || mips64le || ppc64 || ppc64le || riscv64 || s390x || wasm

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
	"math/bits"
	"unsafe"

	"xxx/pkg"
)

// Int
//
// Hashes for [~int | ~uint].
func Int[T ~int | ~uint](v T, seed uint64) uint64 {
	a := uint64(v)
	return mix64(m^uint64(pkg.PtrSize), mix64(a^hashkey[1], a^seed^hashkey[0]))
}

// Int8
//
// Hashes for [~int8 | ~uint8].
func Int8[T ~int8 | ~uint8](v T, seed uint64) uint64 {
	a := uint64(v)
	return mix64(m^1, mix64(a^hashkey[1], a^seed^hashkey[0]))
}

// Int16
//
// Hashes for [~int16 | ~uint16].
func Int16[T ~int16 | ~uint16](v T, seed uint64) uint64 {
	a := uint64(v)
	return mix64(m^2, mix64(a^hashkey[1], a^seed^hashkey[0]))
}

// Int32
//
// Hashes for [~int32 | ~uint32].
func Int32[T ~int32 | ~uint32](v T, seed uint64) uint64 {
	a := uint64(v)
	return mix64(m^4, mix64(a^hashkey[1], a^seed^hashkey[0]))
}

// Int64
//
// Hashes for [~int64 | ~uint64].
func Int64[T ~int64 | ~uint64](v T, seed uint64) uint64 {
	a := uint64(v)
	return mix64(m^8, mix64(a^hashkey[1], a^seed^hashkey[0]))
}

// Uintptr
//
// Hashes for [~uintptr].
func Uintptr[T ~uintptr](v T, seed uint64) uint64 {
	a := uint64(v)
	return mix64(m^uint64(pkg.PtrSize), mix64(a^hashkey[1], a^seed^hashkey[0]))
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
		a := uint64(v)
		return mix64(m^4, mix64(a^hashkey[1], a^seed^hashkey[0]))
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
		a := uint64(v)
		return mix64(m^8, mix64(a^hashkey[1], a^seed^hashkey[0]))
	}
}

func hashContiguousBytes(ptr unsafe.Pointer, seed uint64, length uintptr) uint64 {
	h := seed ^ hashkey[0]
	switch {
	case length == 0:
		return h
	case length < 4:
		a := uint64(*(*byte)(ptr))
		a |= uint64(*(*byte)(seek(ptr, length>>1))) << 8
		a |= uint64(*(*byte)(seek(ptr, length-1))) << 16
		return mix64(m^uint64(length), mix64(a^hashkey[1], h))
	case length == 4:
		a := r4(ptr)
		b := a
		return mix64(m^uint64(length), mix64(a^hashkey[1], b^h))
	case length < 8:
		a := r4(ptr)
		b := r4(seek(ptr, length-4))
		return mix64(m^uint64(length), mix64(a^hashkey[1], b^h))
	case length == 8:
		a := r8(ptr)
		b := a
		return mix64(m^uint64(length), mix64(a^hashkey[1], b^h))
	case length <= 16:
		a := r8(ptr)
		b := r8(seek(ptr, length-8))
		return mix64(m^uint64(length), mix64(a^hashkey[1], b^h))
	case length <= 32:
		h = mix64(r8(ptr)^hashkey[1], r8(seek(ptr, 8))^h)
		ptr = seek(ptr, 16)
		a := r8(seek(ptr, length-32))
		b := r8(seek(ptr, length-24))
		return mix64(m^uint64(length), mix64(a^hashkey[1], b^h))
	default:
		return hashLenGt32(ptr, seed, length)
	}
}

func hashLenGt32(ptr unsafe.Pointer, seed uint64, length uintptr) uint64 {
	seed ^= hashkey[0]
	if length > 48 {
		seed1 := seed
		seed2 := seed
		for ; length > 48; length -= 48 {
			seed = mix64(r8(ptr)^hashkey[1], r8(seek(ptr, 8))^seed)
			seed1 = mix64(r8(seek(ptr, 16))^hashkey[2], r8(seek(ptr, 24))^seed1)
			seed2 = mix64(r8(seek(ptr, 32))^hashkey[3], r8(seek(ptr, 40))^seed2)
			ptr = seek(ptr, 48)
		}
		seed ^= seed1 ^ seed2
	}
	for ; length > 16; length -= 16 {
		seed = mix64(r8(ptr)^hashkey[1], r8(seek(ptr, 8))^seed)
		ptr = seek(ptr, 16)
	}
	a := r8(seek(ptr, length-16))
	b := r8(seek(ptr, length-8))
	return mix64(m^uint64(length), mix64(a^hashkey[1], b^seed))
}

func mix64(a, b uint64) uint64 {
	hi, lo := bits.Mul64(a, b)
	return hi ^ lo
}
