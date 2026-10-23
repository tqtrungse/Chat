//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package socket

import (
	"net"
	"strconv"
	"testing"
)

func TestIP6ZoneToStringFallback(t *testing.T) {
	zone := ^uint32(0)
	if _, err := net.InterfaceByIndex(int(zone)); err == nil {
		t.Skip("test zone index resolves to an interface")
	}
	want := strconv.FormatUint(uint64(zone), 10)
	if got := ip6ZoneToString(zone); got != want {
		t.Fatalf("ip6ZoneToString(%d) = %q, want %q", zone, got, want)
	}
}
