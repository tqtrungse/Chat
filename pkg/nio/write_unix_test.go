//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package nio

import (
	"bytes"
	"errors"
	"net"
	"os"
	"testing"

	"xxx/pkg/nio/netpoll"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type writeTestHandler struct {
	BuiltinEventEngine
	closed   int
	closeErr error
}

func (h *writeTestHandler) OnClose(_ Conn, err error) Action {
	h.closed++
	h.closeErr = err
	return None
}

// These tests own the loop directly, so reads and flushes can be controlled
// without another goroutine draining the kernel's send buffer unexpectedly.
func newWriteTestConn(t *testing.T, edgeTriggered bool) (*conn, int, *writeTestHandler) {
	t.Helper()

	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	require.NoError(t, err)

	for _, fd := range fds {
		unix.CloseOnExec(fd)
		require.NoError(t, unix.SetNonblock(fd, true))
	}

	logger := newListenerTestLogger(t)
	poller, err := netpoll.OpenPoller(logger)
	require.NoError(t, err)

	handler := new(writeTestHandler)

	el := &eventloop{
		engine: &engine{
			opts: &Options{
				Logger:               logger,
				WriteBufferCap:       4096,
				EdgeTriggeredIO:      edgeTriggered,
				EdgeTriggeredIOChunk: 1 << 20,
			},
		},
		poller:       poller,
		eventHandler: handler,
	}
	el.connections.init(0)

	socket := newStreamConn(
		"unix",
		fds[0],
		el,
		&unix.SockaddrUnix{},
		&net.UnixAddr{},
		&net.UnixAddr{},
	)

	t.Cleanup(func() {
		if socket.opened {
			require.NoError(t, el.close(socket, nil))
		}
		_ = unix.Close(fds[1])
		require.NoError(t, poller.Close())
	})
	require.NoError(t, unix.SetsockoptInt(socket.fd, unix.SOL_SOCKET, unix.SO_SNDBUF, 4096))
	require.NoError(t, el.register0(socket))
	return socket, fds[1], handler
}

func testStreamWriters(t *testing.T, run func(*testing.T, bool, func(*conn, [][]byte) (int, error))) {
	t.Helper()

	for _, edgeTriggered := range []bool{false, true} {
		mode := "LT"
		if edgeTriggered {
			mode = "ET"
		}
		for _, method := range []string{"Write", "Writev"} {
			t.Run(
				mode+"/"+method,
				func(t *testing.T) {
					write := func(c *conn, parts [][]byte) (int, error) {
						if method == "Writev" {
							return c.Writev(parts)
						}
						return c.Write(bytes.Join(parts, nil))
					}
					run(t, edgeTriggered, write)
				},
			)
		}
	}
}

func TestConnWriteReturnsFatalErrorAndCloses(t *testing.T) {
	testStreamWriters(
		t,
		func(t *testing.T, edgeTriggered bool, write func(*conn, [][]byte) (int, error)) {
			socket, _, h := newWriteTestConn(t, edgeTriggered)
			// Shutting down this socket's write side produces EPIPE on both
			// Linux and BSD; shutting down the peer's read side differs by OS.
			require.NoError(t, unix.Shutdown(socket.fd, unix.SHUT_WR))

			n, err := write(socket, [][]byte{[]byte("first"), []byte("second")})
			require.ErrorIs(t, err, unix.EPIPE)
			require.Zero(t, n)
			require.False(t, socket.opened)
			require.Equal(t, 1, h.closed)
			require.ErrorIs(t, h.closeErr, unix.EPIPE)

			var syscallErr *os.SyscallError
			require.ErrorAs(t, h.closeErr, &syscallErr)
			require.Zero(t, socket.OutboundBuffered())
			require.Zero(t, socket.loop.countConn())
		},
	)
}

func TestConnWriteBuffersEAGAINAndPreservesOrder(t *testing.T) {
	testStreamWriters(
		t,
		func(t *testing.T, edgeTriggered bool, write func(*conn, [][]byte) (int, error)) {
			var (
				prefix                []byte
				socket, peer, handler = newWriteTestConn(t, edgeTriggered)
				block                 = bytes.Repeat([]byte{0x55}, 4096)
				full                  = false
			)

			for range 1024 {
				n, err := unix.Write(socket.fd, block)
				if errors.Is(err, unix.EAGAIN) {
					full = true
					break
				}
				require.NoError(t, err)
				require.Positive(t, n)
				prefix = append(prefix, block[:n]...)
			}
			require.True(t, full, "test must fill the send buffer before calling the writer")

			parts := [][]byte{[]byte("first payload"), []byte("second payload")}
			payload := bytes.Join(parts, nil)
			n, err := write(socket, parts)
			require.NoError(t, err)
			require.Equal(t, len(payload), n)
			require.Equal(t, len(payload), socket.OutboundBuffered())

			last := []byte("queued after EAGAIN")
			n, err = write(socket, [][]byte{last})
			require.NoError(t, err)
			require.Equal(t, len(last), n)

			want := append(append(prefix, payload...), last...)
			assertWriteTestDelivery(t, socket, peer, want)
			require.True(t, socket.opened)
			require.Zero(t, handler.closed)
		},
	)
}

func TestConnWritePartialSendPreservesOrder(t *testing.T) {
	testStreamWriters(
		t,
		func(t *testing.T, edgeTriggered bool, write func(*conn, [][]byte) (int, error)) {
			socket, peer, handler := newWriteTestConn(t, edgeTriggered)
			capacity, err := unix.GetsockoptInt(
				socket.fd,
				unix.SOL_SOCKET,
				unix.SO_SNDBUF,
			)
			require.NoError(t, err)

			parts := [][]byte{
				bytes.Repeat([]byte("abc"), capacity),
				bytes.Repeat([]byte("XYZ"), capacity),
			}
			want := bytes.Join(parts, nil)
			n, err := write(socket, parts)
			require.NoError(t, err)
			require.Equal(t, len(want), n)
			require.Positive(
				t,
				socket.OutboundBuffered(),
				"the payload must exceed the kernel send buffer",
			)
			require.Less(
				t,
				socket.OutboundBuffered(),
				len(want),
				"some bytes must be sent before buffering the remainder",
			)

			last := []byte("queued after partial write")
			n, err = write(socket, [][]byte{last})
			require.NoError(t, err)
			require.Equal(t, len(last), n)
			assertWriteTestDelivery(t, socket, peer, append(want, last...))
			require.True(t, socket.opened)
			require.Zero(t, handler.closed)
		},
	)
}

func assertWriteTestDelivery(t *testing.T, c *conn, peer int, want []byte) {
	t.Helper()

	var (
		got []byte
		buf = make([]byte, 64*1024)
	)

	for attempts := 0; attempts < 4096 && len(got) < len(want); attempts++ {
		n, err := unix.Read(peer, buf)
		if err != nil {
			require.ErrorIs(t, err, unix.EAGAIN)
		} else {
			require.Positive(t, n, "peer must stay connected")
			got = append(got, buf[:n]...)
		}
		require.NoError(t, c.loop.write(c))
	}

	require.Equal(t, len(want), len(got))
	require.Zero(t, c.OutboundBuffered())
	require.True(
		t,
		bytes.Equal(want, got),
		"writes must preserve the byte order across syscall and buffer boundaries",
	)
}
