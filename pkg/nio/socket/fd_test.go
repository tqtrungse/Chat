//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package socket

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestDupSetsCloseOnExecAndOwnsDescriptor(t *testing.T) {
	for _, tc := range []struct {
		name string
		dup  func(int) (int, error)
	}{
		{"Dup", Dup},
		{"portable fallback", dupCloseOnExecOld},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
				require.NoError(t, err)

				original := fds[0]
				t.Cleanup(func() {
					if original >= 0 {
						_ = unix.Close(original)
					}
					_ = unix.Close(fds[1])
				})

				fd, err := tc.dup(original)
				require.NoError(t, err)
				t.Cleanup(func() { _ = unix.Close(fd) })
				require.NotEqual(t, original, fd)

				flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
				require.NoError(t, err)
				require.NotZero(t, flags&unix.FD_CLOEXEC)
				require.NoError(t, unix.Close(original))

				original = -1
				n, err := unix.Write(fd, []byte("probe"))
				require.NoError(t, err)
				require.Equal(t, 5, n)

				var got [5]byte
				n, err = unix.Read(fds[1], got[:])
				require.NoError(t, err)
				require.Equal(t, 5, n)
				require.Equal(t, "probe", string(got[:]))
			},
		)
	}
}
