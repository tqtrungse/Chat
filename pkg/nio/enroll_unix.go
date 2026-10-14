//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

// Copyright (c) 2019 The Gnet Authors. All rights reserved.
// Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nio

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"syscall"

	errorx "xxx/pkg/nio/error"
	"xxx/pkg/nio/queue"
	"xxx/pkg/nio/socket"

	"golang.org/x/sys/unix"
)

// connRegistration owns the connection until the event loop claims it.
// Cancellation can close a pending fd, but cannot close an fd already claimed
// by the event loop. A queued callback checks this state before using the fd.
type connRegistration struct {
	loop        *eventloop
	ctx         context.Context
	cancel      context.CancelFunc
	result      chan RegisteredResult
	mu          sync.Mutex
	pending     *conn
	completed   bool
	stopEngine  func() bool
	stopContext func() bool
}

func (el *eventloop) enroll(ctx context.Context, c net.Conn, addr net.Addr) (<-chan RegisteredResult, error) {
	if el.engine.concurrency.ctx.Err() != nil {
		return nil, errorx.ErrEngineInShutdown
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	dialCtx, cancel := context.WithCancel(ctx)
	r := &connRegistration{
		loop:   el,
		ctx:    dialCtx,
		cancel: cancel,
		result: make(chan RegisteredResult, 1),
	}

	// Initialize both hooks while holding mu: an already canceled context can
	// start its callback before AfterFunc returns.
	r.mu.Lock()
	r.stopEngine = context.AfterFunc(el.engine.concurrency.ctx, func() {
		r.complete(nil, errorx.ErrEngineInShutdown)
	})
	r.stopContext = context.AfterFunc(ctx, func() {
		r.complete(nil, ctx.Err())
	})
	r.mu.Unlock()

	go r.prepare(c, addr)
	return r.result, nil
}

func (r *connRegistration) err() error {
	if r.loop.engine.concurrency.ctx.Err() != nil {
		return errorx.ErrEngineInShutdown
	}
	return r.ctx.Err()
}

// complete publishes exactly one result and removes the cancellation hooks.
// It returns true only when c was successfully handed to the caller.
func (r *connRegistration) complete(c *conn, err error) bool {
	r.mu.Lock()
	if r.completed {
		r.mu.Unlock()
		return false
	}
	if ctxErr := r.err(); ctxErr != nil {
		err = ctxErr
	}
	r.completed = true
	pending := r.pending
	r.pending = nil
	r.stopEngine()
	r.stopContext()
	r.cancel()
	r.mu.Unlock()

	if pending != nil {
		closeUnregisteredConn(pending)
	}
	result := RegisteredResult{Err: err}
	if err == nil {
		result.Conn = c
	}
	r.result <- result
	close(r.result)
	return err == nil
}

func (r *connRegistration) prepare(c net.Conn, addr net.Addr) {
	if c != nil {
		defer c.Close() //nolint:errcheck
	}
	if err := r.err(); err != nil {
		r.complete(nil, err)
		return
	}
	if c == nil {
		var err error
		c, err = new(net.Dialer).DialContext(r.ctx, addr.Network(), addr.String())
		if err != nil {
			r.complete(nil, err)
			return
		}
		defer c.Close() //nolint:errcheck
	}

	cc, err := r.loop.duplicateConn(c)
	if err != nil {
		r.complete(nil, err)
		return
	}
	cc.SetContext(FromContext(r.ctx))

	r.mu.Lock()
	if r.completed {
		r.mu.Unlock()
		closeUnregisteredConn(cc)
		return
	}
	if err = r.err(); err != nil {
		r.mu.Unlock()
		closeUnregisteredConn(cc)
		r.complete(nil, err)
		return
	}
	r.pending = cc
	err = r.loop.poller.Trigger(queue.LowPriority, r.register, nil)
	r.mu.Unlock()
	if err != nil {
		// Trigger may have queued the callback before failing to wake the poller.
		// complete clears pending, so that callback cannot use a closed fd.
		r.complete(nil, err)
	}
	// The event loop publishes the result; this goroutine never waits for OnOpen.
}

func (r *connRegistration) register(any) error {
	r.mu.Lock()
	if r.completed {
		r.mu.Unlock()
		return nil
	}
	c := r.pending
	r.pending = nil // Only the event loop may touch c from this point onward.
	r.mu.Unlock()

	if err := r.err(); err != nil {
		closeUnregisteredConn(c)
		r.complete(nil, err)
		return nil
	}

	err := r.loop.register0(c)
	if err == nil && !c.opened {
		err = net.ErrClosed // OnOpen rejected the connection with Close.
	}
	if err != nil {
		if c.opened {
			err = errors.Join(err, r.loop.close(c, err))
		}
		r.complete(nil, err)
		return err
	}
	if !r.complete(c, nil) {
		// Cancellation won while register0/OnOpen was running. Cleanup must
		// remain on the event loop now that the fd has been registered.
		return r.loop.close(c, r.err())
	}
	return nil
}

func closeUnregisteredConn(c *conn) {
	_ = unix.Close(c.fd)
	c.release()
}

// duplicateConn transfers no ownership on error: its duplicate fd is closed
// here, while the caller remains responsible for closing the original net.Conn.
func (el *eventloop) duplicateConn(c net.Conn) (cc *conn, err error) {
	sc, ok := c.(syscall.Conn)
	if !ok {
		return nil, fmt.Errorf("failed to assert syscall.Conn from net.Conn: %s", c.RemoteAddr().String())
	}
	rc, err := sc.SyscallConn()
	if err != nil {
		return nil, err
	}

	dupFD := -1
	defer func() {
		if err != nil && dupFD >= 0 {
			_ = unix.Close(dupFD)
		}
	}()

	var dupErr error
	err = rc.Control(func(fd uintptr) {
		dupFD, dupErr = socket.Dup(int(fd))
	})
	if err != nil {
		return nil, err
	}
	if dupErr != nil {
		return nil, dupErr
	}

	var sockAddr unix.Sockaddr
	switch c.(type) {
	case *net.UnixConn:
		sockAddr, _, _, err = socket.GetUnixSockAddr(c.RemoteAddr().Network(), c.RemoteAddr().String())
		if err != nil {
			return nil, err
		}
		localAddr := *c.LocalAddr().(*net.UnixAddr)
		localAddr.Name = c.RemoteAddr().String() + "." + strconv.Itoa(dupFD)
		cc = newStreamConn("unix", dupFD, el, sockAddr, &localAddr, c.RemoteAddr())
	case *net.TCPConn:
		sockAddr, _, _, _, err = socket.GetTCPSockAddr(c.RemoteAddr().Network(), c.RemoteAddr().String())
		if err != nil {
			return nil, err
		}
		cc = newStreamConn("tcp", dupFD, el, sockAddr, c.LocalAddr(), c.RemoteAddr())
	case *net.UDPConn:
		sockAddr, _, _, _, err = socket.GetUDPSockAddr(c.RemoteAddr().Network(), c.RemoteAddr().String())
		if err != nil {
			return nil, err
		}
		cc = newUDPConn(dupFD, el, c.LocalAddr(), sockAddr, true)
	default:
		return nil, fmt.Errorf("unknown type of conn: %T", c)
	}
	return cc, nil
}
