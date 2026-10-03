//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package socket

import (
	"net"
	"syscall"

	"golang.org/x/sys/unix"
)

// Option is used for setting an option on socket.
type Option[T int | string] struct {
	SetSockOpt func(int, T) error
	Opt        T
}

func execSockOpts[T int | string](fd int, opts []Option[T]) error {
	for _, opt := range opts {
		if err := opt.SetSockOpt(fd, opt.Opt); err != nil {
			return err
		}
	}
	return nil
}

// TCPSocket creates a TCP socket and returns a file descriptor that refers to it.
// The given socket options will be set on the returned file descriptor.
func TCPSocket(proto, addr string, passive bool, sockOptInts []Option[int], sockOptStrs []Option[string]) (int, net.Addr, error) {
	return tcpSocket(proto, addr, passive, sockOptInts, sockOptStrs)
}

// UDPSocket creates a UDP socket and returns a file descriptor that refers to it.
// The given socket options will be set on the returned file descriptor.
func UDPSocket(proto, addr string, connect bool, sockOptInts []Option[int], sockOptStrs []Option[string]) (int, net.Addr, error) {
	return udpSocket(proto, addr, connect, sockOptInts, sockOptStrs)
}

// UnixSocket creates a Unix socket and returns a file descriptor that refers to it.
// The given socket options will be set on the returned file descriptor.
func UnixSocket(proto, addr string, passive bool, sockOptInts []Option[int], sockOptStrs []Option[string]) (int, net.Addr, error) {
	return udsSocket(proto, addr, passive, sockOptInts, sockOptStrs)
}

// Accept accepts the next incoming socket along with setting
// O_NONBLOCK and O_CLOEXEC flags on it.
func Accept(fd int) (int, unix.Sockaddr, error) {
	return sysAccept(fd)
}

func ipToSockaddrInet4(ip net.IP, port int) (unix.SockaddrInet4, error) {
	if len(ip) == 0 {
		ip = net.IPv4zero
	}
	ip4 := ip.To4()
	if ip4 == nil {
		return unix.SockaddrInet4{}, &net.AddrError{Err: "non-IPv4 address", Addr: ip.String()}
	}
	sa := unix.SockaddrInet4{Port: port}
	copy(sa.Addr[:], ip4)
	return sa, nil
}

func ipToSockaddrInet6(ip net.IP, port int, zone string) (unix.SockaddrInet6, error) {
	// In general, an IP wildcard address, which is either
	// "0.0.0.0" or "::", means the entire IP addressing
	// space. For some historical reason, it is used to
	// specify "any available address" on some operations
	// of IP node.
	//
	// When the IP node supports IPv4-mapped IPv6 address,
	// we allow a listener to listen to the wildcard
	// address of both IP addressing spaces by specifying
	// IPv6 wildcard address.
	if len(ip) == 0 || ip.Equal(net.IPv4zero) {
		ip = net.IPv6zero
	}
	// We accept any IPv6 address including IPv4-mapped
	// IPv6 address.
	ip6 := ip.To16()
	if ip6 == nil {
		return unix.SockaddrInet6{}, &net.AddrError{Err: "non-IPv6 address", Addr: ip.String()}
	}

	sa := unix.SockaddrInet6{Port: port}
	copy(sa.Addr[:], ip6)
	iface, err := net.InterfaceByName(zone)
	if err != nil {
		return sa, nil
	}
	sa.ZoneId = uint32(iface.Index)

	return sa, nil
}

func ipToSockaddr(family int, ip net.IP, port int, zone string) (unix.Sockaddr, error) {
	switch family {
	case syscall.AF_INET:
		sa, err := ipToSockaddrInet4(ip, port)
		if err != nil {
			return nil, err
		}
		return &sa, nil
	case syscall.AF_INET6:
		sa, err := ipToSockaddrInet6(ip, port, zone)
		if err != nil {
			return nil, err
		}
		return &sa, nil
	}
	return nil, &net.AddrError{Err: "invalid address family", Addr: ip.String()}
}
