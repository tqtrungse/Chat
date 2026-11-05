/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package jitter

import "errors"

var (
	ErrBaseNegative = errors.New("jitter base negative")
	ErrBaseOverCap  = errors.New("jitter base over cap")
)
