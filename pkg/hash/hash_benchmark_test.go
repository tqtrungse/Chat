/*
 * Copyright 2014 The Go Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 *
 * This file contains modifications made by tqtrungse@gmail.com
 * to the Go runtime.
 */

package hash

import (
	"strings"
	"testing"
)

func benchmarkHash(b *testing.B, n int) {
	s := strings.Repeat("A", n)

	for i := 0; i < b.N; i++ {
		String(s, 0)
	}
	b.SetBytes(int64(n))
}

func BenchmarkHash_5(b *testing.B)     { benchmarkHash(b, 5) }
func BenchmarkHash_16(b *testing.B)    { benchmarkHash(b, 16) }
func BenchmarkHash_32(b *testing.B)    { benchmarkHash(b, 32) }
func BenchmarkHash_64(b *testing.B)    { benchmarkHash(b, 64) }
func BenchmarkHash_1024(b *testing.B)  { benchmarkHash(b, 1024) }
func BenchmarkHash_65536(b *testing.B) { benchmarkHash(b, 65536) }
