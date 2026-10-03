//go:build armbe || arm64be || m68k || mips || mips64 || mips64p32 || ppc || ppc64 || s390 || s390x || shbe || sparc || sparc64

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
	return binary.BigEndian.Uint64(b[:])
}

func r4(p unsafe.Pointer) uint64 {
	b := (*[4]byte)(p)
	return uint64(binary.BigEndian.Uint32(b[:]))
}

func r8(p unsafe.Pointer) uint64 {
	b := (*[8]byte)(p)
	// The Go Compiler will replace this function with the most optimal machine instruction.
	// Depending on the CPU architecture during compilation (Intrinsic)
	return binary.BigEndian.Uint64(b[:])
}
