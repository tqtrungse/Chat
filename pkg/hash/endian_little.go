//go:build 386 || amd64 || amd64p32 || alpha || arm || arm64 || loong64 || mipsle || mips64le || mips64p32le || nios2 || ppc64le || riscv || riscv64 || sh || wasm

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
	"crypto/rand"
	"encoding/binary"
	"unsafe"
)

// MakeSeed randoms uint64 number.
func MakeSeed() uint64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return binary.LittleEndian.Uint64(b[:])
}

func r4(p unsafe.Pointer) uint64 {
	b := (*[4]byte)(p)
	return uint64(binary.LittleEndian.Uint32(b[:]))
}

func r8(p unsafe.Pointer) uint64 {
	b := (*[8]byte)(p)
	return binary.LittleEndian.Uint64(b[:])
}
