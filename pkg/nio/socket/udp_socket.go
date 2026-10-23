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

// GetUDPSockAddr the structured addresses based on the protocol and raw address.
func GetUDPSockAddr(proto, addr string) (
	sa unix.Sockaddr,
	family int,
	udpAddr *net.UDPAddr,
	ipv6only bool,
	err error,
) {
	var udpVersion string

	udpAddr, err = net.ResolveUDPAddr(proto, addr)
	if err != nil {
		return
	}

	udpVersion, err = determineUDPProto(proto, udpAddr)
	if err != nil {
		return
	}

	switch udpVersion {
	case "udp4":
		family = unix.AF_INET
		sa, err = ipToSockaddr(family, udpAddr.IP, udpAddr.Port, "")
	case "udp6":
		ipv6only = true
		fallthrough
	case "udp":
		family = unix.AF_INET6
		sa, err = ipToSockaddr(family, udpAddr.IP, udpAddr.Port, udpAddr.Zone)
	default:
		err = errorx.ErrUnsupportedProtocol
	}

	return
}

func determineUDPProto(proto string, addr *net.UDPAddr) (string, error) {
	// If the protocol is set to "udp", we try to determine the actual protocol
	// version from the size of the resolved IP address. Otherwise, we simple use
	// the protocol given to us by the caller.

	if addr.IP.To4() != nil {
		return "udp4", nil
	}

	if addr.IP.To16() != nil {
		return "udp6", nil
	}

	switch proto {
	case "udp", "udp4", "udp6":
		return proto, nil
	}

	return "", errorx.ErrUnsupportedUDPProtocol
}

// udpSocket creates an endpoint for communication and returns a file descriptor that refers to that endpoint.
func udpSocket(
	proto, addr string,
	connect bool,
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

	if sa, family, netAddr, ipv6only, err = GetUDPSockAddr(proto, addr); err != nil {
		return
	}

	if fd, err = sysSocket(family, unix.SOCK_DGRAM, unix.IPPROTO_UDP); err != nil {
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

	// Allow broadcast.
	if err = os.NewSyscallError("setsockopt", unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_BROADCAST, 1)); err != nil {
		return
	}

	if err = execSockOpts(fd, sockOptInts); err != nil {
		return
	}
	if err = execSockOpts(fd, sockOptStrs); err != nil {
		return
	}

	if connect {
		err = os.NewSyscallError("connect", unix.Connect(fd, sa))
	} else {
		err = os.NewSyscallError("bind", unix.Bind(fd, sa))
		if err != nil {
			return
		}
		// Report the actual bound endpoint when the caller requested port zero.
		sa, err = unix.Getsockname(fd)
		if err != nil {
			err = os.NewSyscallError("getsockname", err)
			return
		}
		netAddr = SockaddrToUDPAddr(sa)
	}

	return
}
