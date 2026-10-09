/*
 * Copyright 2026 tqtrungse@gmail.com. All rights reserved.
 * Use of this pub code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package protocol

import "errors"

var (
	ErrPkgSizeInvalid   = errors.New("invalid packet size")
	ErrPkgTypeInvalid   = errors.New("invalid packet type")
	ErrPkgCipherInvalid = errors.New("invalid cipher size")
	ErrPkgModified      = errors.New("the packet is modified")
)
