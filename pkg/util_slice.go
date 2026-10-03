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
	"strconv"
	"unsafe"

	slicepool "xxx/pkg/pool/slice"
)

// BytesToString converts byte slice to a string without any memory allocation.
func BytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

// StringToBytes converts string to a byte slice without any memory allocation.
func StringToBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// Concat appends the base-10 representation of value to prefix and returns
// the resulting byte slice.
//
// The returned slice is allocated from slicePool and must be returned to the
// pool by the caller when it is no longer needed.
func Concat(prefix string, value uint64) []byte {
	buf := slicepool.Get(len(prefix) + 20)
	buf = buf[:0]
	buf = append(buf, prefix...)
	buf = strconv.AppendUint(buf, value, 10)
	return buf
}

// U64ToBytes converts value to its base-10 representation and returns it as a
// byte slice.
//
// The returned slice is allocated from slicePool and must be returned to the
// pool by the caller when it is no longer needed.
func U64ToBytes(value uint64) []byte {
	buf := slicepool.Get(20)
	buf = buf[:0]
	buf = strconv.AppendUint(buf, value, 10)
	return buf
}
