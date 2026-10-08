/*
 * Copyright 2026 tqtrungse@gmail.com. All rights reserved.
 * Use of this pub code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package protocol

import "errors"

var (
	ErrDeviceNotFound   = errors.New("device not found")
	ErrDeviceUnactive   = errors.New("unactive device")
	ErrSessionNotFound  = errors.New("session not found")
	ErrSessionClosed    = errors.New("session is closed")
	ErrSessionDuplicate = errors.New("duplicate session")
	ErrPkgSizeInvalid   = errors.New("invalid packet size")
	ErrPkgTypeInvalid   = errors.New("invalid packet type")
	ErrPkgCipherInvalid = errors.New("invalid cipher size")
	ErrPkgModified      = errors.New("the packet is modified")
)
