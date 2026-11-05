/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package user

type ID uint64

// Uint64 returns the underlying numeric user identifier.
func (id ID) Uint64() uint64 {
	return uint64(id)
}
