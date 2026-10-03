//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Modified from the original gnet source by tqtrungse@gmail.com in 2026.
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

package nio

import (
	"net"
	"os"
	"runtime"
	"strings"
	"sync"

	"xxx/pkg/log"
	errorx "xxx/pkg/nio/error"
	"xxx/pkg/nio/netpoll"
	"xxx/pkg/nio/socket"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

type listener struct {
	openOnce, closeOnce sync.Once
	fd                  int
	addr                net.Addr
	address, network    string
	sockOptInts         []socket.Option[int]
	sockOptStrs         []socket.Option[string]
	pollAttachment      *netpoll.PollAttachment // listener attachment for poller
	logger              *log.Logger
}

func (ln *listener) packPollAttachment(handler netpoll.PollEventHandler) *netpoll.PollAttachment {
	ln.pollAttachment = &netpoll.PollAttachment{FD: ln.fd, Callback: handler}
	return ln.pollAttachment
}

func (ln *listener) dup() (int, error) {
	return socket.Dup(ln.fd)
}

func (ln *listener) open() (err error) {
	ln.openOnce.Do(func() {
		switch ln.network {
		case "tcp", "tcp4", "tcp6":
			ln.fd, ln.addr, err = socket.TCPSocket(ln.network, ln.address, true, ln.sockOptInts, ln.sockOptStrs)
			ln.network = "tcp"
		case "udp", "udp4", "udp6":
			ln.fd, ln.addr, err = socket.UDPSocket(ln.network, ln.address, false, ln.sockOptInts, ln.sockOptStrs)
			ln.network = "udp"
		case "unix":
			_ = os.RemoveAll(ln.address)
			ln.fd, ln.addr, err = socket.UnixSocket(ln.network, ln.address, true, ln.sockOptInts, ln.sockOptStrs)
		default:
			err = errorx.ErrUnsupportedProtocol
		}
	})
	return
}

func (ln *listener) close() {
	ln.closeOnce.Do(func() {
		if ln.fd > 0 {
			err := os.NewSyscallError("close", unix.Close(ln.fd))
			if err != nil {
				ln.logger.Error("close listener", zap.Error(err))
			}
		}
		ln.fd = -1
		if ln.network == "unix" {
			err := os.RemoveAll(ln.address)
			if err != nil {
				ln.logger.Error("remove listener", zap.Error(err))
			}
		}
	})
}

func initListener(network, addr string, options *Options) (ln *listener, err error) {
	var (
		sockOptInts []socket.Option[int]
		sockOptStrs []socket.Option[string]
	)

	if options.ReusePort && network != "unix" {
		sockOpt := socket.Option[int]{SetSockOpt: socket.SetReuseport, Opt: 1}
		sockOptInts = append(sockOptInts, sockOpt)
	}
	if options.ReuseAddr {
		sockOpt := socket.Option[int]{SetSockOpt: socket.SetReuseAddr, Opt: 1}
		sockOptInts = append(sockOptInts, sockOpt)
	}
	if options.TCPNoDelay == TCPNoDelay && strings.HasPrefix(network, "tcp") {
		sockOpt := socket.Option[int]{SetSockOpt: socket.SetNoDelay, Opt: 1}
		sockOptInts = append(sockOptInts, sockOpt)
	}
	if options.SocketRecvBuffer > 0 {
		sockOpt := socket.Option[int]{SetSockOpt: socket.SetRecvBuffer, Opt: options.SocketRecvBuffer}
		sockOptInts = append(sockOptInts, sockOpt)
	}
	if options.SocketSendBuffer > 0 {
		sockOpt := socket.Option[int]{SetSockOpt: socket.SetSendBuffer, Opt: options.SocketSendBuffer}
		sockOptInts = append(sockOptInts, sockOpt)
	}
	if strings.HasPrefix(network, "udp") {
		udpAddr, err := net.ResolveUDPAddr(network, addr)
		if err == nil && udpAddr.IP.IsMulticast() {
			if sockoptFn := socket.SetMulticastMembership(network, udpAddr); sockoptFn != nil {
				sockOpt := socket.Option[int]{SetSockOpt: sockoptFn, Opt: options.MulticastInterfaceIndex}
				sockOptInts = append(sockOptInts, sockOpt)
			}
		}
	}

	ln = &listener{
		network:     network,
		address:     addr,
		sockOptInts: sockOptInts,
		sockOptStrs: sockOptStrs,
		logger:      options.Logger,
	}
	err = ln.open()

	if options.TCPKeepAlive > 0 && ln.network == "tcp" &&
		(runtime.GOOS == "linux" || runtime.GOOS == "freebsd" || runtime.GOOS == "dragonfly") {
		// TCP keepalive options will be inherited from the listening socket
		// only when running on Linux, FreeBSD or DragonFlyBSD.
		//
		// Check out https://github.com/nginx/nginx/pull/337 for details.
		err = setKeepAlive(
			ln.fd,
			true,
			options.TCPKeepAlive,
			options.TCPKeepInterval,
			options.TCPKeepCount,
		)
	}

	return
}
