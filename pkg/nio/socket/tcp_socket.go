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

var listenerBacklogMaxSize = maxListenerBacklog()

// GetTCPSockAddr the structured addresses based on the protocol and raw address.
func GetTCPSockAddr(proto, addr string) (
	sa unix.Sockaddr,
	family int,
	tcpAddr *net.TCPAddr,
	ipv6only bool,
	err error,
) {
	var tcpVersion string

	tcpAddr, err = net.ResolveTCPAddr(proto, addr)
	if err != nil {
		return
	}

	tcpVersion, err = determineTCPProto(proto, tcpAddr)
	if err != nil {
		return
	}

	switch tcpVersion {
	case "tcp4":
		family = unix.AF_INET
		sa, err = ipToSockaddr(family, tcpAddr.IP, tcpAddr.Port, "")
	case "tcp6":
		ipv6only = true
		fallthrough
	case "tcp":
		family = unix.AF_INET6
		sa, err = ipToSockaddr(family, tcpAddr.IP, tcpAddr.Port, tcpAddr.Zone)
	default:
		err = errorx.ErrUnsupportedProtocol
	}

	return
}

func determineTCPProto(proto string, addr *net.TCPAddr) (string, error) {
	// If the protocol is set to "tcp", we try to determine the actual protocol
	// version from the size of the resolved IP address. Otherwise, we simple use
	// the protocol given to us by the caller.

	if addr.IP.To4() != nil {
		return "tcp4", nil
	}

	if addr.IP.To16() != nil {
		return "tcp6", nil
	}

	switch proto {
	case "tcp", "tcp4", "tcp6":
		return proto, nil
	}

	return "", errorx.ErrUnsupportedTCPProtocol
}

// tcpSocket creates an endpoint for communication and returns a file descriptor that refers to that endpoint.
func tcpSocket(
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
		family   int
		ipv6only bool
		sa       unix.Sockaddr
	)

	if sa, family, netAddr, ipv6only, err = GetTCPSockAddr(proto, addr); err != nil {
		return
	}

	if fd, err = sysSocket(family, unix.SOCK_STREAM, unix.IPPROTO_TCP); err != nil {
		err = os.NewSyscallError("socket", err)
		return
	}
	defer func() {
		if err != nil {
			// Ignore EINPROGRESS for non-blocking socket connect, should be processed by caller
			if errors.Is(err, unix.EINPROGRESS) {
				return
			}
			_ = unix.Close(fd)
		}
	}()

	if family == unix.AF_INET6 && ipv6only {
		if err = SetIPv6Only(fd, 1); err != nil {
			return
		}
	}

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
		if err != nil {
			return
		}
		// Resolve the bound endpoint so a requested port 0 is reported as the
		// actual OS-assigned port rather than the configured placeholder.
		sa, err = unix.Getsockname(fd)
		if err != nil {
			err = os.NewSyscallError("getsockname", err)
			return
		}
		netAddr = SockaddrToTCPOrUnixAddr(sa)
	} else {
		err = os.NewSyscallError("connect", unix.Connect(fd, sa))
	}

	return
}
