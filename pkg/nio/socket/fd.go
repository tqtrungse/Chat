//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package socket

import (
	"errors"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"
)

// Dup duplicates the given fd and marks it close-on-exec.
func Dup(fd int) (int, error) {
	return dupCloseOnExec(fd)
}

// tryDupCloexec indicates whether F_DUPFD_CLOEXEC should be used.
// If the kernel doesn't support it, this is set to false.
var tryDupCloexec atomic.Bool

func init() {
	tryDupCloexec.Store(true)
}

// dupCloseOnExec duplicates the given fd and marks it close-on-exec.
func dupCloseOnExec(fd int) (int, error) {
	if tryDupCloexec.Load() {
		r, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
		if err == nil {
			return r, nil
		}
		switch {
		case errors.Is(err.(syscall.Errno), unix.EINVAL), errors.Is(err.(syscall.Errno), unix.ENOSYS):
			// Old kernel, or js/wasm (which returns
			// ENOSYS). Fall back to the portable way from
			// now on.
			tryDupCloexec.Store(false)
		default:
			return -1, err
		}
	}
	return dupCloseOnExecOld(fd)
}

// dupCloseOnExecOld is the traditional way to dup an fd and
// set its O_CLOEXEC bit, using two system calls.
func dupCloseOnExecOld(fd int) (int, error) {
	syscall.ForkLock.RLock()
	defer syscall.ForkLock.RUnlock()
	newFD, err := syscall.Dup(fd)
	if err != nil {
		return -1, err
	}
	syscall.CloseOnExec(newFD)
	return newFD, nil
}
