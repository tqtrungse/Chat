/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package connection

import "errors"

var (
	ErrManagerClosed   = errors.New("manager closed")
	ErrConnReachMax    = errors.New("connection over")
	ErrSessionNotFound = errors.New("session not found")
	ErrSessionClosed   = errors.New("session is closed")
)
