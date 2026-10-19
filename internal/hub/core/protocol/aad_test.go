/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package protocol

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The domain is hard-coded here on PURPOSE: changing it means changing the wire format
// (the two ends will not be able to authenticate each other's frames). If you actually change the domain,
// must edit both this constant and the golden vector below.
const wantAADDomain = "xxx/hub/packet/gcm/v1\x00"

func le16(v uint16) []byte { return []byte{byte(v), byte(v >> 8)} }

func le64(v uint64) []byte {
	b := make([]byte, 8)
	for i := range b {
		b[i] = byte(v >> (8 * i))
	}
	return b
}

// wantAAD creates the expected AAD according to the layout in the securePacketAAD comment:
// domain | frameSize(2) | pkgType(2) | cipherSize(2) | seq(8) | expand.
func wantAAD(frameSize, pkgType, cipherSize uint16, seq uint64, expand []byte) []byte {
	var b []byte
	b = append(b, wantAADDomain...)
	b = append(b, le16(frameSize)...)
	b = append(b, le16(pkgType)...)
	b = append(b, le16(cipherSize)...)
	b = append(b, le64(seq)...)
	b = append(b, expand...)
	return b
}

func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i*7 + 3)
	}
	return b
}

// decodeAAD separates the reverse AAD by encoding/binary (read path is different from write path).
func decodeAAD(t *testing.T, aad []byte) (fs, pt, cs uint16, seq uint64, expand []byte) {
	t.Helper()
	minLen := len(wantAADDomain) + headerSize + seqSize
	require.GreaterOrEqual(t, len(aad), minLen)

	got := string(aad[:len(wantAADDomain)])
	require.Equal(t, wantAADDomain, got)

	p := aad[len(wantAADDomain):]
	fs = binary.LittleEndian.Uint16(p[0:2])
	pt = binary.LittleEndian.Uint16(p[2:4])
	cs = binary.LittleEndian.Uint16(p[4:6])
	seq = binary.LittleEndian.Uint64(p[6:14])
	expand = p[14:]
	return
}

func TestSeqBytes(t *testing.T) {
	cases := []struct {
		seq  uint64
		want [seqSize]byte
	}{
		{0, [seqSize]byte{}},
		{1, [seqSize]byte{1}},
		{0xFF, [seqSize]byte{0xFF}},
		{0x100, [seqSize]byte{0, 1}},
		{0x0102030405060708, [seqSize]byte{8, 7, 6, 5, 4, 3, 2, 1}}, // little-endian
		{math.MaxUint64, [seqSize]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}},
	}
	for _, c := range cases {
		got := seqBytes(c.seq)
		assert.Truef(
			t,
			bytes.Equal(got[:], c.want[:]),
			"seqBytes(%#x) = % x, want % x", c.seq, got, c.want,
		)
	}
}

func TestSeqBytes_RoundTrip(t *testing.T) {
	values := []uint64{0, 1, 2, math.MaxUint64, math.MaxUint64 - 1}
	for k := range uint(64) {
		values = append(values, 1<<k, 1<<k-1, 1<<k+1)
	}
	for _, v := range values {
		b := seqBytes(v)
		got := binary.LittleEndian.Uint64(b[:])
		require.Equalf(t, v, got, "round trip %#x -> % x -> %#x", v, b, got)
	}
}

// Golden vector handwritten byte by byte: protects wire format compatibility.
func TestSecurePacketAAD_Golden(t *testing.T) {
	got := securePacketAAD(
		0x1234,
		0x0002,
		0xBEEF,
		0x0102030405060708,
		[]byte{0xAA, 0xBB},
	)
	want := append([]byte("xxx/hub/packet/gcm/v1\x00"),
		0x34, 0x12, // frameSize
		0x02, 0x00, // pkgType
		0xEF, 0xBE, // cipherSize
		0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, // seq
		0xAA, 0xBB, // expand
	)
	require.True(t, bytes.Equal(got, want))
}

func TestSecurePacketAAD_Layout(t *testing.T) {
	cases := []struct {
		name                           string
		frameSize, pkgType, cipherSize uint16
		seq                            uint64
		expand                         []byte
	}{
		{"all zero, nil expand", 0, 0, 0, 0, nil},
		{"empty expand", 1, 2, 3, 4, []byte{}},
		{"one byte expand", 0x1234, 0x5678, 0x9ABC, 0xDEF0, []byte{0x42}},
		{"max fields", math.MaxUint16, math.MaxUint16, math.MaxUint16, math.MaxUint64, pattern(16)},
		{"seq low byte only", 0, 0, 0, 1, nil},
		{"seq high byte only", 0, 0, 0, 1 << 56, nil},
		{"large expand", 100, 5, 4096, 1<<40 + 12345, pattern(4096)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := securePacketAAD(c.frameSize, c.pkgType, c.cipherSize, c.seq, c.expand)
			want := wantAAD(c.frameSize, c.pkgType, c.cipherSize, c.seq, c.expand)
			require.Equal(t, len(want), len(got))
			require.True(t, bytes.Equal(want, got))
		})
	}
}

// nil and empty must be the same result.
func TestSecurePacketAAD_NilEqualsEmptyExpand(t *testing.T) {
	a := securePacketAAD(1, 2, 3, 4, nil)
	b := securePacketAAD(1, 2, 3, 4, []byte{})
	require.True(t, bytes.Equal(a, b))
}

// Each field is validated: changing any field requires changing the AAD.
// Also, check that subsequent calls do not overwrite the buffer of the first call.
func TestSecurePacketAAD_EachFieldIsAuthenticated(t *testing.T) {
	exp := []byte{1, 2, 3}
	base := securePacketAAD(100, 5, 80, 7, exp)

	variants := []struct {
		name string
		aad  []byte
	}{
		{"frameSize", securePacketAAD(101, 5, 80, 7, exp)},
		{"pkgType", securePacketAAD(100, 6, 80, 7, exp)},
		{"cipherSize", securePacketAAD(100, 5, 81, 7, exp)},
		{"seq", securePacketAAD(100, 5, 80, 8, exp)},
		{"expand content", securePacketAAD(100, 5, 80, 7, []byte{1, 2, 4})},
		{"expand length", securePacketAAD(100, 5, 80, 7, []byte{1, 2, 3, 0})},
		{"expand removed", securePacketAAD(100, 5, 80, 7, nil)},
	}
	for _, v := range variants {
		require.Falsef(t, bytes.Equal(base, v.aad), "changes %s but AAD remains unchanged", v.name)
	}

	require.True(
		t,
		bytes.Equal(base, wantAAD(100, 5, 80, 7, exp)),
		"first AAD changed by subsequent calls (shared buffer?)\n got: % x", base,
	)
}

// Replay protection based on seq: consecutive seq must give different AAD.
func TestSecurePacketAAD_SeqProducesDistinctAAD(t *testing.T) {
	seen := make(map[string]uint64)
	for seq := range uint64(5000) {
		key := string(securePacketAAD(10, 1, 32, seq, []byte{9}))
		prev, ok := seen[key]
		require.False(t, ok, "", "sequence %d and %d for the same AAD", prev, seq)
		seen[key] = seq
	}
}

// The result cannot share memory with expand (copy, not alias).
func TestSecurePacketAAD_DoesNotAliasExpand(t *testing.T) {
	expand := []byte{1, 2, 3, 4}
	aad := securePacketAAD(1, 2, 3, 4, expand)
	snapshot := append([]byte(nil), aad...)

	expand[0] = 0xFF // Modify input after calling
	require.True(t, bytes.Equal(aad, snapshot), "edit expand changes the returned AAD")

	aad[len(aad)-1] = 0x00 // sửa kết quả
	require.Equal(t, byte(4), expand[3], "modifying AAD changes the expand of the caller.")
}

// Random round trip (fixed seed): encode then decode using encoding/binary.
func TestSecurePacketAAD_RoundTripRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for i := range 2000 {
		fs, pt, cs := uint16(rng.Uint32()), uint16(rng.Uint32()), uint16(rng.Uint32())
		seq := rng.Uint64()
		expand := make([]byte, rng.Intn(65))
		rng.Read(expand)

		aad := securePacketAAD(fs, pt, cs, seq, expand)
		gfs, gpt, gcs, gseq, gexp := decodeAAD(t, aad)
		if gfs != fs || gpt != pt || gcs != cs || gseq != seq || !bytes.Equal(gexp, expand) {
			require.Failf(
				t,
				"",
				"round trip off-center on the lap %d:\n in : %d %d %d %d % x\n out: %d %d %d %d % x",
				i, fs, pt, cs, seq, expand, gfs, gpt, gcs, gseq, gexp)
		}
	}
}

// Simultaneous call: run with -race to capture shared buffer from pool.
func TestSecurePacketAAD_Concurrent(t *testing.T) {
	const goroutines, iters = 8, 500

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range iters {
				fs, pt, cs := uint16(g), uint16(i), uint16(i*3)
				seq := uint64(g)<<32 | uint64(i)
				expand := pattern(i % 33)

				got := securePacketAAD(fs, pt, cs, seq, expand)
				if !bytes.Equal(got, wantAAD(fs, pt, cs, seq, expand)) {
					assert.Failf(t, "", "goroutine %d round %d: AAD wrong\n got: % x", g, i, got)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}
