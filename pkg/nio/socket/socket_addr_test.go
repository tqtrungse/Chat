//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
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

package socket

import (
	"net"
	"strconv"
	"testing"
)

func TestItod(t *testing.T) {
	for _, value := range []uint{0, 1, 9, 10, 99, 100, 1_234_567_890, ^uint(0)} {
		want := strconv.FormatUint(uint64(value), 10)
		t.Run(want, func(t *testing.T) {
			if got := itod(value); got != want {
				t.Fatalf("itod(%d) = %q, want %q", value, got, want)
			}
		})
	}
}

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
