/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package socket

import errorx "xxx/pkg/nio/error"

// SetKeepAlivePeriod is not implemented on OpenBSD because there are
// no equivalents of Linux's TCP_KEEPIDLE, TCP_KEEPINTVL, and TCP_KEEPCNT.
func SetKeepAlivePeriod(_, _ int) error {
	// OpenBSD has no user-settable per-socket TCP keepalive options.
	return errorx.ErrUnsupportedOp
}

// SetKeepAlive is not implemented on OpenBSD.
func SetKeepAlive(_ int, _ bool, _, _, _ int) error {
	// OpenBSD has no user-settable per-socket TCP keepalive options.
	return errorx.ErrUnsupportedOp
}
