/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package bit

import "math/bits"

// NextPowerOfTwo
//
// Calculates the power of 2 that is greater than or equal to v.
func NextPowerOfTwo(v int) uint {
	if v <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(v-1))
}
