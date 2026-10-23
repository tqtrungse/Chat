/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package nio

import (
	"io"

	"xxx/pkg/nio/netpoll"

	"golang.org/x/sys/unix"
)

func (c *conn) processIO(_ int, ev netpoll.IOEvent, _ netpoll.IOFlags) error {
	el := c.loop
	// First check for any unexpected non-IO events.
	// For these events we just close the connection directly.
	if ev&(netpoll.ErrEvents|unix.EPOLLRDHUP) != 0 && ev&netpoll.ReadWriteEvents == 0 {
		c.outboundBuffer.Release() // don't bother to write to a connection that is already broken
		return el.close(c, io.EOF)
	}
	// Secondly, check for EPOLLOUT before EPOLLIN, the former has a higher priority
	// than the latter regardless of the aliveness of the current connection:
	//
	// 1. When the connection is alive and the system is overloaded, we want to
	// offload the incoming traffic by writing all pending data back to the remotes
	// before continuing to read and handle requests.
	// 2. When the connection is dead, we need to try writing any pending data back
	// to the remote first and then close the connection.
	//
	// We perform eventloop.write for EPOLLOUT because it can take good care of either case.
	if ev&(netpoll.WriteEvents|netpoll.ErrEvents) != 0 {
		if err := el.write(c); err != nil {
			return err
		}
	}
	// Check for EPOLLIN before EPOLLRDHUP in case that there are pending data in
	// the socket buffer.
	if ev&(netpoll.ReadEvents|netpoll.ErrEvents) != 0 {
		if err := el.read(c); err != nil {
			return err
		}
	}
	// Ultimately, check for EPOLLRDHUP, this event indicates that the remote has
	// either closed connection or shut down the writing half of the connection.
	if ev&unix.EPOLLRDHUP != 0 && c.opened {
		if ev&unix.EPOLLIN == 0 { // unreadable EPOLLRDHUP, close the connection directly
			return el.close(c, io.EOF)
		}
		// Received the event of EPOLLIN|EPOLLRDHUP, but the previous eventloop.read
		// failed to drain the socket buffer, so we ensure to get it done this time.
		c.isEOF = true
		return el.read(c)
	}
	return nil
}
