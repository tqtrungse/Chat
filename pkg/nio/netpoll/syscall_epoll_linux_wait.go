//go:build linux && (amd64 || 386)

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package netpoll

import (
	"unsafe"

	"golang.org/x/sys/unix"
)

func epollWait(epfd int, events []epollevent, msec int) (int, error) {
	var ep unsafe.Pointer
	if len(events) > 0 {
		ep = unsafe.Pointer(&events[0])
	} else {
		ep = unsafe.Pointer(&zero)
	}
	var (
		np    uintptr
		errno unix.Errno
	)
	if msec == 0 { // non-block system call, use RawSyscall6 to avoid getting preempted by runtime
		np, _, errno = unix.RawSyscall6(
			unix.SYS_EPOLL_WAIT,
			uintptr(epfd),
			uintptr(ep),
			uintptr(len(events)),
			0,
			0,
			0,
		)
	} else {
		np, _, errno = unix.Syscall6(
			unix.SYS_EPOLL_WAIT,
			uintptr(epfd),
			uintptr(ep),
			uintptr(len(events)),
			uintptr(msec),
			0,
			0,
		)
	}
	if errno != 0 {
		return int(np), errnoErr(errno)
	}
	return int(np), nil
}
