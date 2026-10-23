/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package protocol

import "errors"

var (
	ErrPkgSizeInvalid = errors.New("invalid packet size")
	ErrPkgTooLarge    = errors.New("packet too large")
	ErrMsgKindInvalid = errors.New("invalid message type")
)
