/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package connticket

import "errors"

var (
	ErrRequestInvalid     = errors.New("invalid client request")
	ErrDeviceNotFound     = errors.New("device not found")
	ErrDeviceUnactive     = errors.New("unactive device")
	ErrIdentityPubInvalid = errors.New("identity public key is invalid")
)
