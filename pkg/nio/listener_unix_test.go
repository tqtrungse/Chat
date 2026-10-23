//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package nio

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"xxx/pkg/log"
	errorx "xxx/pkg/nio/error"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func newListenerTestLogger(t *testing.T) *log.Logger {
	t.Helper()

	cfg := log.FileConfig{
		Filename:      filepath.Join(t.TempDir(), "listener.log"),
		BufferSize:    1024,
		FlushInterval: time.Hour,
	}
	cfg.Load(nil)
	logger, closeLogger, loggerErr := log.NewLogger(&cfg)
	require.NoError(t, loggerErr)
	t.Cleanup(func() { _ = closeLogger() })
	return logger
}

func TestInitListenerPreservesBindErrorWithKeepAlive(t *testing.T) {
	occupied, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = occupied.Close() })

	ln, err := initListener("tcp4", occupied.Addr().String(), &Options{
		Logger:       newListenerTestLogger(t),
		TCPKeepAlive: 30 * time.Second,
	})
	require.ErrorIs(t, err, unix.EADDRINUSE)

	var syscallErr *os.SyscallError
	require.ErrorAs(t, err, &syscallErr)
	require.Equal(t, "bind", syscallErr.Syscall)
	require.Nil(t, ln)
}

func TestCreateListenersClosesEarlierListenersOnFailure(t *testing.T) {
	for _, network := range []string{"tcp4", "unix"} {
		t.Run(
			network,
			func(t *testing.T) {
				occupied, err := net.Listen("tcp4", "127.0.0.1:0")
				require.NoError(t, err)
				t.Cleanup(func() { _ = occupied.Close() })

				// Keep Unix socket paths short enough for sockaddr_un on macOS.
				dir, err := os.MkdirTemp("", "nio-ln-")
				require.NoError(t, err)
				t.Cleanup(func() { _ = os.RemoveAll(dir) })

				var addrs []string
				var reservations []net.Listener
				for i := range 3 {
					addr := "127.0.0.1:0"
					if network == "unix" {
						addr = filepath.Join(dir, fmt.Sprintf("s%d", i))
					}

					reservation, err := net.Listen(network, addr)
					require.NoError(t, err)

					t.Cleanup(func() { _ = reservation.Close() })
					reservations = append(reservations, reservation)
					addrs = append(addrs, network+"://"+reservation.Addr().String())
				}

				for _, reservation := range reservations {
					require.NoError(t, reservation.Close())
				}
				addrs = append(addrs, "tcp4://"+occupied.Addr().String())

				listeners, options, err := createListeners(addrs,
					WithLogger(newListenerTestLogger(t)),
					WithTCPKeepAlive(30*time.Second),
				)
				require.ErrorIs(t, err, unix.EADDRINUSE)
				require.ErrorContains(t, err, "listen "+addrs[3]+": ")

				var syscallErr *os.SyscallError
				require.ErrorAs(t, err, &syscallErr)
				require.Equal(t, "bind", syscallErr.Syscall)
				require.Nil(t, listeners)
				require.Nil(t, options)

				for _, addr := range addrs[:3] {
					proto, address, err := parseProtoAddr(addr)
					require.NoError(t, err)

					if proto == "unix" {
						require.NoFileExists(t, address)
					}

					rebound, err := net.Listen(proto, address)
					require.NoError(t, err, "earlier listener must release %s", addr)
					require.NoError(t, rebound.Close())
				}
			},
		)
	}
}

func TestInitListenerClosesSocketOnKeepAliveFailure(t *testing.T) {
	if runtime.GOOS != "linux" &&
		runtime.GOOS != "freebsd" &&
		runtime.GOOS != "dragonfly" {
		t.Skip("keepalive is configured on accepted connections on this platform")
	}

	reservation, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)

	addr := reservation.Addr().String()
	require.NoError(t, reservation.Close())

	ln, err := initListener("tcp4", addr, &Options{
		Logger:          newListenerTestLogger(t),
		TCPKeepAlive:    30 * time.Second,
		TCPKeepInterval: -time.Second,
	})
	require.Error(t, err)
	require.Nil(t, ln)

	rebound, err := net.Listen("tcp4", addr)
	require.NoError(t, err, "keepalive failure must release the bound socket")
	require.NoError(t, rebound.Close())
}

func TestCreateListenersKeepsSuccessfulListenersOpen(t *testing.T) {
	listeners, _, err := createListeners(
		[]string{
			"tcp4://127.0.0.1:0",
			"tcp4://127.0.0.1:0",
			"tcp4://127.0.0.1:0",
		},
		WithLogger(newListenerTestLogger(t)),
		WithTCPKeepAlive(30*time.Second),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		for _, ln := range listeners {
			ln.close()
		}
	})

	require.Len(t, listeners, 3)
	for _, ln := range listeners {
		_, err = unix.Getsockname(ln.fd)
		require.NoError(t, err, "successful initialization must keep the listener open")
	}
}

func TestInitListenerReportsAssignedTCPPort(t *testing.T) {
	ln, err := initListener(
		"tcp4",
		"127.0.0.1:0",
		&Options{Logger: newListenerTestLogger(t)},
	)
	require.NoError(t, err)
	t.Cleanup(ln.close)

	addr, ok := ln.addr.(*net.TCPAddr)
	require.True(t, ok)
	require.Positive(t, addr.Port)

	bound, err := unix.Getsockname(ln.fd)
	require.NoError(t, err)

	actual, ok := bound.(*unix.SockaddrInet4)
	require.True(t, ok)
	require.Equal(t, actual.Port, addr.Port)
}

func TestListenerOpenAndCloseOnce(t *testing.T) {
	assertEnrollmentNoFDLeak(t)

	var wg sync.WaitGroup
	errs := make(chan error, 16)
	ln := &listener{
		fd:      -1,
		network: "tcp4",
		address: "127.0.0.1:0",
		logger:  newListenerTestLogger(t),
	}
	t.Cleanup(ln.close)

	for range cap(errs) {
		wg.Go(func() {
			errs <- ln.open()
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	fd, addr := ln.fd, ln.addr
	require.NoError(t, ln.open())
	require.Equal(t, fd, ln.fd)
	require.Equal(t, addr, ln.addr)

	_, err := unix.Getsockname(fd)
	require.NoError(t, err)

	for range 16 {
		wg.Go(func() { ln.close() })
	}

	wg.Wait()
	require.Equal(t, -1, ln.fd)

	_, err = unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	require.ErrorIs(t, err, unix.EBADF)
}

func TestListenerOpenPreservesFirstError(t *testing.T) {
	occupied, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = occupied.Close() })
	ln := &listener{fd: -1, network: "tcp4", address: occupied.Addr().String()}
	firstErr := ln.open()
	require.ErrorIs(t, firstErr, unix.EADDRINUSE)
	// Removing the original bind conflict must not cause a second open attempt.
	require.NoError(t, occupied.Close())

	for range 3 {
		require.Same(t, firstErr, ln.open())
	}
}

func TestParseProtoAddr(t *testing.T) {
	for _, tc := range []struct {
		input   string
		network string
		address string
		err     error
	}{
		{
			input:   "unix:///tmp/hub.sock",
			network: "unix",
			address: "/tmp/hub.sock",
		},
		{
			input:   "unix://relative/hub.sock",
			network: "unix",
			address: "relative/hub.sock",
		},
		{
			input: "unix://",
			err:   errorx.ErrInvalidNetworkAddress,
		},
		{
			input:   "tcp://127.0.0.1:9000",
			network: "tcp",
			address: "127.0.0.1:9000",
		},
		{
			input: "tcp://127.0.0.1:9000/path",
			err:   errorx.ErrInvalidNetworkAddress,
		},
		{
			input:   "udp://[ff02::3%lo0]:9000",
			network: "udp",
			address: "[ff02::3%lo0]:9000",
		},
	} {
		t.Run(
			tc.input,
			func(t *testing.T) {
				network, address, err := parseProtoAddr(tc.input)
				if tc.err != nil {
					require.ErrorIs(t, err, tc.err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.network, network)
				require.Equal(t, tc.address, address)
			},
		)
	}
}

func TestUnixAbsolutePathListenerAcceptsConnection(t *testing.T) {
	dir, err := os.MkdirTemp("", "nio-unix-")
	require.NoError(t, err)

	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	path := filepath.Join(dir, "hub.sock")
	listeners, _, err := createListeners(
		[]string{"unix://" + path},
		WithLogger(newListenerTestLogger(t)),
	)
	require.NoError(t, err)

	ln := listeners[0]
	t.Cleanup(ln.close)
	require.Equal(t, path, ln.addr.String())

	client, err := net.DialTimeout("unix", path, time.Second)
	require.NoError(t, err)

	t.Cleanup(func() { _ = client.Close() })
	fd, _, err := unix.Accept(ln.fd)
	require.NoError(t, err)
	require.NoError(t, unix.Close(fd))

	ln.close()
	require.NoFileExists(t, path)
}

func TestInitListenerReportsAssignedUDPPort(t *testing.T) {
	ln, err := initListener(
		"udp4",
		"127.0.0.1:0",
		&Options{Logger: newListenerTestLogger(t)},
	)
	require.NoError(t, err)
	t.Cleanup(ln.close)

	addr, ok := ln.addr.(*net.UDPAddr)
	require.True(t, ok)
	require.Positive(t, addr.Port)

	bound, err := unix.Getsockname(ln.fd)
	require.NoError(t, err)

	actual, ok := bound.(*unix.SockaddrInet4)
	require.True(t, ok)
	require.Equal(t, actual.Port, addr.Port)
}
