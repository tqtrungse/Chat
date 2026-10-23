/*
 * Copyright (c) 2024 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
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
