/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package redis

import "errors"

var (
	ErrBatchTooLarge       = errors.New("device batch exceeds maximum size")
	ErrCacheClosed         = errors.New("cache is closed")
	ErrCacheStarted        = errors.New("cache start already called")
	ErrCallbacksAlreadySet = errors.New("cache callbacks already set")
	ErrCallbacksAfterStart = errors.New("cache callbacks must be set before start")
)
