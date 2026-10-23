//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package nio

import (
	"errors"
	"net"
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

		l, _ := el.listeners.Lookup(fd)
		network := l.network
		remoteAddr := el.prepareAcceptedSocket(nfd, sa, network)

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
	switch {
	case err == nil:
	case errors.Is(err, unix.EINTR),
		errors.Is(err, unix.EAGAIN),
		errors.Is(err, unix.ECONNRESET),
		errors.Is(err, unix.ECONNABORTED):
		// ECONNRESET or ECONNABORTED could indicate that a socket in the Accept queue
		// was closed before we Accept()ed it. Ignore it for now; the poller will
		// report the listener again if more connections are pending.
		return nil
	default:
		el.getLogger().Error("Accept() failed due to", zap.Error(err))
		return errorx.ErrAcceptSocket
	}

	remoteAddr := el.prepareAcceptedSocket(nfd, sa, network)

	c := newStreamConn(network, nfd, el, sa, l.addr, remoteAddr)
	return el.register0(c)
}

// prepareAcceptedSocket applies per-connection options shared by the accept paths.
// Keepalive failures retain the existing policy: log and continue registration.
func (el *eventloop) prepareAcceptedSocket(fd int, sa unix.Sockaddr, network string) net.Addr {
	if opts := el.engine.opts; opts.TCPKeepAlive > 0 && network == "tcp" &&
		(runtime.GOOS != "linux" && runtime.GOOS != "freebsd" && runtime.GOOS != "dragonfly") {
		// Other platforms do not inherit TCP keepalive from the listening socket.
		// See https://github.com/nginx/nginx/pull/337.
		if err := setKeepAlive(fd, true, opts.TCPKeepAlive, opts.TCPKeepInterval, opts.TCPKeepCount); err != nil {
			el.getLogger().Error("failed to set TCP keepalive", zap.Int("fd", fd), zap.Error(err))
		}
	}
	return socket.SockaddrToTCPOrUnixAddr(sa)
}
