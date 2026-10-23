/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package jitter

import "errors"

var (
	ErrBaseNegative = errors.New("jitter base negative")
	ErrBaseOverCap  = errors.New("jitter base over cap")
)
