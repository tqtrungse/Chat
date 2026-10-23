/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package hubendpoint

import "errors"

var (
	ErrUnknownField   = errors.New("hub descriptor has unexpected field (private key material is not allowed)")
	ErrDuplicateField = errors.New("hub descriptor has duplicate field")
)
