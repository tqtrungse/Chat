/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
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
