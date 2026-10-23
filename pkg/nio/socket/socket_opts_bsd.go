//go:build dragonfly || freebsd || netbsd || openbsd

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package socket

import errorx "xxx/pkg/nio/error"

// SetBindToDevice is not implemented on *BSD because there is
// no equivalent of Linux's SO_BINDTODEVICE.
func SetBindToDevice(_ int, _ string) error {
	return errorx.ErrUnsupportedOp
}
