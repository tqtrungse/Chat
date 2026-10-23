/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connticket

import "errors"

var (
	ErrRequestInvalid = errors.New("invalid client request")
	ErrDeviceNotFound = errors.New("device not found")
	ErrDeviceUnactive = errors.New("unactive device")
)
