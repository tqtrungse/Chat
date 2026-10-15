//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

package nio

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"xxx/pkg/log"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func newListenerTestLogger(t *testing.T) *log.Logger {
	t.Helper()
	logger, closeLogger := log.NewLogger(&log.FileConfig{
		Filename:      filepath.Join(t.TempDir(), "listener.log"),
		BufferSize:    1024,
		FlushInterval: time.Hour,
	})
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
		t.Run(network, func(t *testing.T) {
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
		})
	}
}

func TestInitListenerClosesSocketOnKeepAliveFailure(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "freebsd" && runtime.GOOS != "dragonfly" {
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
	listeners, _, err := createListeners([]string{
		"tcp4://127.0.0.1:0",
		"tcp4://127.0.0.1:0",
		"tcp4://127.0.0.1:0",
	}, WithLogger(newListenerTestLogger(t)), WithTCPKeepAlive(30*time.Second))
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, ln := range listeners {
			ln.close()
		}
	})
	require.Len(t, listeners, 3)
	for _, ln := range listeners {
		_, err := unix.Getsockname(ln.fd)
		require.NoError(t, err, "successful initialization must keep the listener open")
	}
}
