//go:build !(386 || amd64)

/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package hash

import "unsafe"

// Bytes
//
// Hashes for [~[]byte].
func Bytes[T ~[]byte](v T, seed uint64) uint64 {
	ptr := unsafe.Pointer(unsafe.SliceData(v))
	length := uintptr(len(v))
	return hashContiguousBytes(ptr, seed, length)
}

// String
//
// Hashes for [~string].
func String[T ~string](v T, seed uint64) uint64 {
	ptr := unsafe.Pointer(unsafe.StringData(string(v)))
	length := uintptr(len(v))
	return hashContiguousBytes(ptr, seed, length)
}
