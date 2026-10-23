//go:build darwin || netbsd || openbsd

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package socket

import (
	"syscall"

	"golang.org/x/sys/unix"
)

func sysSocket(family, sotype, proto int) (fd int, err error) {
	syscall.ForkLock.RLock()
	if fd, err = unix.Socket(family, sotype, proto); err == nil {
		unix.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return
	}
	if err = unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
	}
	return
}

func sysAccept(fd int) (nfd int, sa unix.Sockaddr, err error) {
	syscall.ForkLock.RLock()
	if nfd, sa, err = unix.Accept(fd); err == nil {
		unix.CloseOnExec(nfd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return
	}
	if err = unix.SetNonblock(nfd, true); err != nil {
		_ = unix.Close(nfd)
	}
	return
}
