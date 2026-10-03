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

// Hash function for swiss.Table internal use.
// Security relies on per-process random hashkey.

const (
	c0 = uint64(8-pkg.PtrSize)/4*2860486313 + uint64(pkg.PtrSize-4)/4*33054211828000289
	c1 = uint64(8-pkg.PtrSize)/4*3267000013 + uint64(pkg.PtrSize-4)/4*23344194077549503
	m  = 0x79d5f9e0de1e8cf5
)

var hashkey [4]uint64

func init() {
	for i := range hashkey {
		hashkey[i] = MakeSeed()
	}
}

// Empty
//
// Hashes for [~struct{}].
func Empty[T ~struct{}](_ T, seed uint64) uint64 {
	return seed
}

// Complex64
//
// Hashes for [~complex64].
func Complex64[T ~complex64](v T, seed uint64) uint64 {
	x := (*[2]float32)(unsafe.Pointer(&v))
	return Float32[float32](x[1], Float32[float32](x[0], seed))
}

// Complex128
//
// Hashes for [~complex128].
func Complex128[T ~complex128](v T, seed uint64) uint64 {
	x := (*[2]float64)(unsafe.Pointer(&v))
	return Float64[float64](x[1], Float64[float64](x[0], seed))
}

func seek(p unsafe.Pointer, offset uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(p) + offset)
}
