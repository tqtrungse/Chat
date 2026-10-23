/*
 * Copyright (c) 2024 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package socket

import (
	"os"

	"golang.org/x/sys/unix"
)

// SetReuseport enables SO_REUSEPORT_LB option on socket.
func SetReuseport(fd, reusePort int) error {
	return os.NewSyscallError("setsockopt", unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT_LB, reusePort))
}
