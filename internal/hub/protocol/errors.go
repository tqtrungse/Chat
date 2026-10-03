/*
 * Copyright 2026 tqtrungse@gmail.com. All rights reserved.
 * Use of this pub code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package protocol

import "errors"

var (
	ErrNotFoundDevice   = errors.New("device not found")
	ErrUnactiveDevice   = errors.New("unactive device")
	ErrNotFoundSession  = errors.New("session not found")
	ErrSessionClosed    = errors.New("session is closed")
	ErrInvalidPkgSize   = errors.New("invalid packet size")
	ErrInvalidPkgType   = errors.New("invalid packet type")
	ErrInvalidPkgCipher = errors.New("invalid cipher size")
	ErrModifiedPkg      = errors.New("the packet is modified")
)
