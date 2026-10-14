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
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"xxx/pkg/collection/swiss"
	"xxx/pkg/log"
	errorx "xxx/pkg/nio/error"
	nio "xxx/pkg/nio/io"
	"xxx/pkg/nio/netpoll"
	"xxx/pkg/nio/queue"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

// The default value of UIO_MAXIOV/IOV_MAX is 1024 on Linux and most BSD-like OSs.
const iovMax = 1024

type eventloop struct {
	listeners    *swiss.Table[int, *listener] // ref listeners
	idx          int                          // loop index in the engine loops list
	engine       *engine                      // engine in loop
	poller       *netpoll.Poller              // epoll or kqueue
	buffer       []byte                       // read packet buffer whose capacity is set by user, default value is 64KB
	connections  connMatrix                   // loop connections storage
	eventHandler EventHandler                 // user eventHandler
}

func (el *eventloop) Register(ctx context.Context, addr net.Addr) (<-chan RegisteredResult, error) {
	if el.engine.isShutdown() {
		return nil, errorx.ErrEngineInShutdown
	}
	if addr == nil {
		return nil, errorx.ErrInvalidNetworkAddress
	}
	return el.enroll(ctx, nil, addr)
}

func (el *eventloop) Enroll(ctx context.Context, c net.Conn) (<-chan RegisteredResult, error) {
	if el.engine.isShutdown() {
		return nil, errorx.ErrEngineInShutdown
	}
	if c == nil {
		return nil, errorx.ErrInvalidNetConn
	}
	return el.enroll(ctx, c, c.RemoteAddr())
}

func (el *eventloop) Execute(ctx context.Context, runnable RunnableFunc) error {
	if el.engine.isShutdown() {
		return errorx.ErrEngineInShutdown
	}
	if runnable == nil {
		return errorx.ErrNilRunnable
	}
	return el.poller.Trigger(queue.LowPriority, func(any) error {
		return runnable(ctx)
	}, nil)
}

//func (el *eventloop) Schedule(context.Context, Runnable, time.Duration) error {
//	return errorx.ErrUnsupportedOp
//}

func (el *eventloop) Close(c Conn) error {
	return el.close(c.(*conn), nil)
}

func (el *eventloop) getLogger() *log.Logger {
	return el.engine.opts.Logger
}

func (el *eventloop) countConn() int32 {
	return el.connections.loadCount()
}

func (el *eventloop) closeConns() {
	// Close loops and all outstanding connections
	el.connections.iterate(func(c *conn) bool {
		_ = el.close(c, nil)
		return true
	})
}

func (el *eventloop) register(a any) error {
	return el.register0(a.(*conn))
}

func (el *eventloop) register0(c *conn) error {
	addEvents := el.poller.AddRead
	if el.engine.opts.EdgeTriggeredIO {
		addEvents = el.poller.AddReadWrite
	}
	if err := addEvents(&c.pollAttachment, el.engine.opts.EdgeTriggeredIO); err != nil {
		_ = unix.Close(c.fd)
		c.release()
		return err
	}
	el.connections.addConn(c)
	if c.isDatagram && c.remote != nil {
		return nil
	}
	return el.open(c)
}

func (el *eventloop) readUDP(fd int, _ netpoll.IOEvent, _ netpoll.IOFlags) error {
	n, sa, err := unix.Recvfrom(fd, el.buffer, 0)
	if err != nil {
		if errors.Is(err, unix.EAGAIN) {
			return nil
		}
		return fmt.Errorf(
			"failed to read UDP packet from fd=%d in event-loop(%d), %v",
			fd,
			el.idx,
			os.NewSyscallError("recvfrom", err),
		)
	}

	var c *conn
	if ln, ok := el.listeners.Lookup(fd); ok {
		c = newUDPConn(fd, el, ln.addr, sa, false)
	} else {
		c = el.connections.getConn(fd)
	}
	c.buffer = el.buffer[:n]
	action := el.eventHandler.OnTraffic(c)
	if c.remote != nil {
		c.release()
	}
	if action == Shutdown {
		return errorx.ErrEngineShutdown
	}
	return nil
}

func (el *eventloop) read0(a any) error {
	return el.read(a.(*conn))
}

func (el *eventloop) read(c *conn) error {
	if !c.opened {
		return nil
	}

	var recv int
	isET := el.engine.opts.EdgeTriggeredIO
	chunk := el.engine.opts.EdgeTriggeredIOChunk
loop:
	n, err := unix.Read(c.fd, el.buffer)
	if err != nil || n == 0 {
		if errors.Is(err, unix.EAGAIN) {
			return nil
		}
		if n == 0 {
			err = io.EOF
		}
		return el.close(c, os.NewSyscallError("read", err))
	}
	recv += n

	c.buffer = el.buffer[:n]
	action := el.eventHandler.OnTraffic(c)
	switch action {
	case None:
	case Close:
		return el.close(c, nil)
	case Shutdown:
		return errorx.ErrEngineShutdown
	default:
		return errorx.ErrUnsupportedOp
	}
	_, _ = c.inboundBuffer.Write(c.buffer)
	c.buffer = c.buffer[:0]

	if c.isEOF || (isET && recv < chunk) {
		goto loop
	}

	// To prevent infinite reading in ET mode and starving other events,
	// we need to set up threshold for the maximum read bytes per connection
	// on each event-loop. If the threshold is reached and there are still
	// unread data in the socket buffer, we must issue another read event manually.
	if isET && n == len(el.buffer) {
		return el.poller.Trigger(queue.LowPriority, el.read0, c)
	}

	return nil
}

func (el *eventloop) write0(a any) error {
	return el.write(a.(*conn))
}

func (el *eventloop) write(c *conn) error {
	if c.outboundBuffer.IsEmpty() {
		return nil
	}

	isET := el.engine.opts.EdgeTriggeredIO
	chunk := el.engine.opts.EdgeTriggeredIOChunk
	var (
		n    int
		sent int
		err  error
	)
loop:
	iov, _ := c.outboundBuffer.Peek(-1)
	if len(iov) > 1 {
		if len(iov) > iovMax {
			iov = iov[:iovMax]
		}
		n, err = nio.Writev(c.fd, iov)
	} else {
		n, err = unix.Write(c.fd, iov[0])
	}
	_, _ = c.outboundBuffer.Discard(n)
	switch {
	case err == nil:
	case errors.Is(err, unix.EAGAIN):
		return nil
	default:
		return el.close(c, os.NewSyscallError("write", err))
	}
	sent += n

	if isET && !c.outboundBuffer.IsEmpty() && sent < chunk {
		goto loop
	}

	// All data have been sent, it's no need to monitor the writable events for LT mode,
	// remove the writable event from poller to help the future event-loops if necessary.
	if !isET && c.outboundBuffer.IsEmpty() {
		return el.poller.ModRead(&c.pollAttachment, false)
	}

	// To prevent infinite writing in ET mode and starving other events,
	// we need to set up threshold for the maximum write bytes per connection
	// on each event-loop. If the threshold is reached and there are still
	// pending data to write, we must issue another write event manually.
	if isET && !c.outboundBuffer.IsEmpty() {
		return el.poller.Trigger(queue.HighPriority, el.write0, c)
	}

	return nil
}

func (el *eventloop) open(c *conn) error {
	c.opened = true

	out, action := el.eventHandler.OnOpen(c)
	if out != nil {
		if err := c.open(out); err != nil {
			return err
		}
	}

	if !c.outboundBuffer.IsEmpty() && !el.engine.opts.EdgeTriggeredIO {
		if err := el.poller.ModReadWrite(&c.pollAttachment, false); err != nil {
			return err
		}
	}

	return el.handleAction(c, action)
}

func (el *eventloop) close(c *conn, err error) error {
	if !c.opened || el.connections.getConn(c.fd) == nil {
		return nil // ignore stale connections
	}

	el.connections.delConn(c)
	action := el.eventHandler.OnClose(c, err)

	// Send residual data in buffer back to the remote before actually closing the connection.
	for !c.outboundBuffer.IsEmpty() {
		iov, _ := c.outboundBuffer.Peek(0)
		if len(iov) > iovMax {
			iov = iov[:iovMax]
		}
		n, err := nio.Writev(c.fd, iov)
		if err != nil {
			break
		}
		_, _ = c.outboundBuffer.Discard(n)
	}

	c.release()

	var errStr strings.Builder
	err0, err1 := el.poller.Delete(c.fd), unix.Close(c.fd)
	if err0 != nil {
		err0 = fmt.Errorf(
			"failed to delete fd=%d from poller in event-loop(%d): %v",
			c.fd,
			el.idx,
			os.NewSyscallError("delete", err0),
		)
		errStr.WriteString(err0.Error())
		errStr.WriteString(" | ")
	}
	if err1 != nil {
		err1 = fmt.Errorf(
			"failed to close fd=%d in event-loop(%d): %v",
			c.fd,
			el.idx,
			os.NewSyscallError("close", err1),
		)
		errStr.WriteString(err1.Error())
	}
	if errStr.Len() > 0 {
		return errors.New(strings.TrimSuffix(errStr.String(), " | "))
	}

	return el.handleAction(c, action)
}

func (el *eventloop) wake(c *conn) error {
	if !c.opened || el.connections.getConn(c.fd) == nil {
		return nil // ignore stale connections
	}

	action := el.eventHandler.OnTraffic(c)

	return el.handleAction(c, action)
}

func (el *eventloop) ticker(ctx context.Context) {
	var (
		action Action
		delay  time.Duration
		timer  *time.Timer
	)
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		delay, action = el.eventHandler.OnTick()
		switch action {
		case None, Close:
		case Shutdown:
			// It seems reasonable to mark this as low-priority, waiting for some tasks like asynchronous writes
			// to finish up before shutting down the service.
			err := el.poller.Trigger(queue.LowPriority, func(_ any) error { return errorx.ErrEngineShutdown }, nil)
			el.getLogger().Debug("failed to enqueue shutdown signal of high-priority", zap.Int("event-loop", el.idx), zap.Error(err))
		default:
			panic("unhandled default case")
		}
		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			timer.Reset(delay)
		}
		select {
		case <-ctx.Done():
			el.getLogger().Debug("stopping ticker", zap.Int("event-loop from Engine", el.idx), zap.Error(ctx.Err()))
			return
		case <-timer.C:
		}
	}
}

func (el *eventloop) handleAction(c *conn, action Action) error {
	switch action {
	case None:
		return nil
	case Close:
		return el.close(c, nil)
	case Shutdown:
		return errorx.ErrEngineShutdown
	default:
		return nil
	}
}
