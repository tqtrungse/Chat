//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package socket

import (
	"errors"
	"net"
	"os"

	errorx "xxx/pkg/nio/error"

	"golang.org/x/sys/unix"
)

// GetUnixSockAddr the structured addresses based on the protocol and raw address.
func GetUnixSockAddr(proto, addr string) (
	sa unix.Sockaddr,
	family int,
	unixAddr *net.UnixAddr,
	err error,
) {
	unixAddr, err = net.ResolveUnixAddr(proto, addr)
	if err != nil {
		return
	}

	switch unixAddr.Network() {
	case "unix":
		sa, family = &unix.SockaddrUnix{Name: unixAddr.Name}, unix.AF_UNIX
	default:
		err = errorx.ErrUnsupportedUDSProtocol
	}

	return
}

// udsSocket creates an endpoint for communication and returns a file descriptor that refers to that endpoint.
func udsSocket(
	proto, addr string,
	passive bool,
	sockOptInts []Option[int],
	sockOptStrs []Option[string],
) (
	fd int,
	netAddr net.Addr,
	err error,
) {
	var (
		family int
		sa     unix.Sockaddr
	)

	if sa, family, netAddr, err = GetUnixSockAddr(proto, addr); err != nil {
		return
	}

	if fd, err = sysSocket(family, unix.SOCK_STREAM, 0); err != nil {
		err = os.NewSyscallError("socket", err)
		return
	}
	defer func() {
		if err != nil {
			// Ignore EINPROGRESS for non-blocking socket connect, should be processed by caller
			// though there is less situation for EINPROGRESS when using unix socket
			if errors.Is(err, unix.EINPROGRESS) {
				return
			}
			_ = unix.Close(fd)
		}
	}()

	if err = execSockOpts(fd, sockOptInts); err != nil {
		return
	}
	if err = execSockOpts(fd, sockOptStrs); err != nil {
		return
	}

	if passive {
		if err = os.NewSyscallError("bind", unix.Bind(fd, sa)); err != nil {
			return
		}

		// Set backlog size to the maximum.
		err = os.NewSyscallError("listen", unix.Listen(fd, listenerBacklogMaxSize))
	} else {
		err = os.NewSyscallError("connect", unix.Connect(fd, sa))
	}

	return
}
