//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package socket

import (
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIP6ZoneToStringFallback(t *testing.T) {
	zone := ^uint32(0)

	_, err := net.InterfaceByIndex(int(zone))
	if err == nil {
		t.Skip("test zone index resolves to an interface")
	}

	want := strconv.FormatUint(uint64(zone), 10)
	got := ip6ZoneToString(zone)
	require.Equal(t, want, got)
}
