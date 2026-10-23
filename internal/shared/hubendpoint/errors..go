/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package hubendpoint

import "errors"

var (
	ErrUnknownField   = errors.New("hub descriptor has unexpected field (private key material is not allowed)")
	ErrDuplicateField = errors.New("hub descriptor has duplicate field")
)
