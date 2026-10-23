/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connection

import "errors"

var (
	ErrManagerClosed   = errors.New("manager closed")
	ErrConnReachMax    = errors.New("connection over")
	ErrSessionNotFound = errors.New("session not found")
	ErrSessionClosed   = errors.New("session is closed")
)
