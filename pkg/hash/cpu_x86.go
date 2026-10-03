//go:build 386 || amd64

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

package hash

import (
	"unsafe"

	"golang.org/x/sys/cpu"
)

var useRuntimeMemHash = cpu.X86.HasAES && cpu.X86.HasSSSE3 && cpu.X86.HasSSE41

// Bytes
//
// Hashes for [~[]byte].
func Bytes[T ~[]byte](v T, seed uint64) uint64 {
	ptr := unsafe.Pointer(unsafe.SliceData(v))
	length := uintptr(len(v))
	if useRuntimeMemHash {
		return uint64(memHash(ptr, uintptr(seed), length))
	}
	return hashContiguousBytes(ptr, seed, length)
}

// String
//
// Hashes for [~string].
func String[T ~string](v T, seed uint64) uint64 {
	ptr := unsafe.Pointer(unsafe.StringData(string(v)))
	length := uintptr(len(v))
	if useRuntimeMemHash {
		return uint64(memHash(ptr, uintptr(seed), length))
	}
	return hashContiguousBytes(ptr, seed, length)
}

//go:noescape
//go:linkname memHash runtime.memhash
func memHash(p unsafe.Pointer, h, s uintptr) uintptr
