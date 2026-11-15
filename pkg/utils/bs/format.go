/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package bs

import (
	"strconv"

	slicepool "xxx/pkg/pool/slice"
)

// Concat appends the base-10 representation of value to prefix and returns
// the resulting byte slice.
//
// The returned slice is allocated from slicePool and must be returned to the
// pool by the caller when it is no longer needed.
func Concat(prefix string, value uint64) []byte {
	n := digitCount(value)
	buf := slicepool.Get(len(prefix) + n)
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
	n := digitCount(value)
	buf := slicepool.Get(n)
	buf = buf[:0]
	buf = strconv.AppendUint(buf, value, 10)
	return buf
}

func digitCount(v uint64) int {
	switch {
	case v < 10:
		return 1
	case v < 100:
		return 2
	case v < 1000:
		return 3
	case v < 10000:
		return 4
	case v < 100000:
		return 5
	case v < 1000000:
		return 6
	case v < 10000000:
		return 7
	case v < 100000000:
		return 8
	case v < 1000000000:
		return 9
	case v < 10000000000:
		return 10
	case v < 100000000000:
		return 11
	case v < 1000000000000:
		return 12
	case v < 10000000000000:
		return 13
	case v < 100000000000000:
		return 14
	case v < 1000000000000000:
		return 15
	case v < 10000000000000000:
		return 16
	case v < 100000000000000000:
		return 17
	case v < 1000000000000000000:
		return 18
	case v < 10000000000000000000:
		return 19
	default:
		return 20
	}
}
