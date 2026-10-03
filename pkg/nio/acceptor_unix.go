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
	"runtime"

	errorx "xxx/pkg/nio/error"
	"xxx/pkg/nio/netpoll"
	"xxx/pkg/nio/queue"
	"xxx/pkg/nio/socket"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

func (el *eventloop) accept0(fd int, _ netpoll.IOEvent, _ netpoll.IOFlags) error {
	for {
		nfd, sa, err := socket.Accept(fd)
		switch err {
		case nil:
		case unix.EAGAIN: // the Accept queue has been drained out, we can return now
			return nil
		case unix.EINTR, unix.ECONNRESET, unix.ECONNABORTED:
			// ECONNRESET or ECONNABORTED could indicate that a socket
			// in the Accept queue was closed before we Accept()ed it.
			// It's a silly error, let's retry it.
			continue
		default:
			el.getLogger().Error("Accept() failed due to", zap.Error(err))
			return errorx.ErrAcceptSocket
		}

		remoteAddr := socket.SockaddrToTCPOrUnixAddr(sa)
		l, _ := el.listeners.Lookup(fd)
		network := l.network
		if opts := el.engine.opts; opts.TCPKeepAlive > 0 && network == "tcp" &&
			(runtime.GOOS != "linux" && runtime.GOOS != "freebsd" && runtime.GOOS != "dragonfly") {
			// TCP keepalive options are not inherited from the listening socket
			// on platforms other than Linux, FreeBSD, or DragonFlyBSD.
			// We therefore need to set them on the accepted socket explicitly.
			//
			// Check out https://github.com/nginx/nginx/pull/337 for details.
			err = setKeepAlive(
				nfd,
				true,
				opts.TCPKeepAlive,
				opts.TCPKeepInterval,
				opts.TCPKeepCount,
			)
			if err != nil {
				el.getLogger().Error("failed to set TCP keepalive", zap.Int("fd", fd), zap.Error(err))
			}
		}

		nel := el.engine.eventLoops.next(remoteAddr)
		l, _ = nel.listeners.Lookup(fd)
		c := newStreamConn(network, nfd, nel, sa, l.addr, remoteAddr)
		err = nel.poller.Trigger(queue.HighPriority, nel.register, c)
		if err != nil {
			nel.getLogger().Error("failed to enqueue the accepted socket to poller", zap.Int("fd", fd), zap.Error(err))
			_ = unix.Close(nfd)
			c.release()
		}
	}
}

func (el *eventloop) accept(fd int, ev netpoll.IOEvent, flags netpoll.IOFlags) error {
	l, _ := el.listeners.Lookup(fd)
	network := l.network
	if network == "udp" {
		return el.readUDP(fd, ev, flags)
	}

	nfd, sa, err := socket.Accept(fd)
	switch err {
	case nil:
	case unix.EINTR, unix.EAGAIN, unix.ECONNRESET, unix.ECONNABORTED:
		// ECONNRESET or ECONNABORTED could indicate that a socket
		// in the Accept queue was closed before we Accept()ed it.
		// It's a silly error, let's retry it.
		return nil
	default:
		el.getLogger().Error("Accept() failed due to", zap.Error(err))
		return errorx.ErrAcceptSocket
	}

	remoteAddr := socket.SockaddrToTCPOrUnixAddr(sa)
	if opts := el.engine.opts; opts.TCPKeepAlive > 0 && l.network == "tcp" &&
		(runtime.GOOS != "linux" && runtime.GOOS != "freebsd" && runtime.GOOS != "dragonfly") {
		// TCP keepalive options are not inherited from the listening socket
		// on platforms other than Linux, FreeBSD, or DragonFlyBSD.
		// We therefore need to set them on the accepted socket explicitly.
		//
		// Check out https://github.com/nginx/nginx/pull/337 for details.
		err = setKeepAlive(
			nfd,
			true,
			opts.TCPKeepAlive,
			opts.TCPKeepInterval,
			opts.TCPKeepCount,
		)
		if err != nil {
			el.getLogger().Error("failed to set TCP keepalive", zap.Int("fd", fd), zap.Error(err))
		}
	}

	c := newStreamConn(network, nfd, el, sa, l.addr, remoteAddr)
	return el.register0(c)
}
