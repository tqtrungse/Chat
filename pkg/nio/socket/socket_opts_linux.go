/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package socket

import (
	"os"

	"golang.org/x/sys/unix"
)

// SetBindToDevice binds the socket to a specific network interface.
//
// SO_BINDTODEVICE on Linux works in both directions: only process packets
// received from the particular interface along with sending them through
// that interface, instead of following the default route.
func SetBindToDevice(fd int, ifname string) error {
	return os.NewSyscallError("setsockopt", unix.BindToDevice(fd, ifname))
}
