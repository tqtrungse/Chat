/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package protocol

import "errors"

var (
	ErrPkgSizeInvalid = errors.New("invalid packet size")
	ErrPkgTooLarge    = errors.New("packet too large")
	ErrMsgKindInvalid = errors.New("invalid message type")
)
