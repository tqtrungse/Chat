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
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"slices"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

// Smhasher is a torture test for hash functions.
// https://code.google.com/p/smhasher/
// This code is a service of some of the Smhasher tests to Go.
//
// The current AES hash function passes Smhasher. Our fallback
// hash functions don't, so we only enable the difficult tests when
// we know the AES implementation is available.

// Sanity checks.
// hash should not depend on values outside key.
// hash should not depend on alignment.
func TestSmhasherSanity(t *testing.T) {
	r := rand.New(rand.NewSource(1234))
	const REP = 10
	const KEYMAX = 128
	const PAD = 16
	const OFFMAX = 16
	for range REP {
		for n := range KEYMAX {
			for i := range OFFMAX {
				var b [KEYMAX + OFFMAX + 2*PAD]byte
				var c [KEYMAX + OFFMAX + 2*PAD]byte
				randBytes(r, b[:])
				randBytes(r, c[:])
				copy(c[PAD+i:PAD+i+n], b[PAD:PAD+n])
				assert.Equal(
					t,
					Bytes(b[PAD:PAD+n], 0),
					Bytes(c[PAD+i:PAD+i+n], 0),
					"hash depends on bytes outside key",
				)
			}
		}
	}
}

type HashSet struct {
	list []uintptr // list of hashes added
}

func newHashSet() *HashSet {
	return &HashSet{list: make([]uintptr, 0, 1024)}
}

func (s *HashSet) add(h uintptr) {
	s.list = append(s.list, h)
}

func (s *HashSet) addS(x string) {
	s.add(uintptr(String(x, 0)))
}

func (s *HashSet) addB(x []byte) {
	s.add(uintptr(Bytes(x, 0)))
}

func (s *HashSet) addS_seed(x string, seed uintptr) {
	s.add(uintptr(String(x, uint64(seed))))
}

func (s *HashSet) check(t *testing.T) {
	list := s.list
	slices.Sort(list)

	collisions := 0
	for i := 1; i < len(list); i++ {
		if list[i] == list[i-1] {
			collisions++
		}
	}
	n := len(list)

	const SLOP = 50.0
	pairs := int64(n) * int64(n-1) / 2
	expected := float64(pairs) / math.Pow(2.0, float64(hashSize))
	stddev := math.Sqrt(expected)

	threshold := expected + SLOP*(3*stddev+1)
	assert.LessOrEqualf(
		t,
		float64(collisions),
		threshold,
		"unexpected number of collisions: got=%d mean=%f stddev=%f threshold=%f",
		collisions,
		expected,
		stddev,
		threshold,
	)
	// Reset for reuse
	s.list = s.list[:0]
}

// a string plus adding zeros must make distinct hashes
func TestSmhasherAppendedZeros(t *testing.T) {
	s := "hello" + strings.Repeat("\x00", 256)
	h := newHashSet()
	for i := 0; i <= len(s); i++ {
		h.addS(s[:i])
	}
	h.check(t)
}

// All 0-3 byte strings have distinct hashes.
func TestSmhasherSmallKeys(t *testing.T) {
	h := newHashSet()
	var b [3]byte
	for i := range 256 {
		b[0] = byte(i)
		h.addB(b[:1])
		for j := range 256 {
			b[1] = byte(j)
			h.addB(b[:2])
			if !testing.Short() {
				for k := range 256 {
					b[2] = byte(k)
					h.addB(b[:3])
				}
			}
		}
	}
	h.check(t)
}

// Different length strings of all zeros have distinct hashes.
func TestSmhasherZeros(t *testing.T) {
	t.Parallel()
	N := 256 * 1024
	if testing.Short() {
		N = 1024
	}
	h := newHashSet()
	b := make([]byte, N)
	for i := 0; i <= N; i++ {
		h.addB(b[:i])
	}
	h.check(t)
}

// Strings with up to two nonzero bytes all have distinct hashes.
func TestSmhasherTwoNonzero(t *testing.T) {
	if runtime.GOARCH == "wasm" {
		t.Skip("Too slow on wasm")
	}
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}
	t.Parallel()

	h := newHashSet()
	for n := 2; n <= 16; n++ {
		twoNonZero(h, n)
	}
	h.check(t)
}

func twoNonZero(h *HashSet, n int) {
	b := make([]byte, n)

	// all zero
	h.addB(b)

	// one non-zero byte
	for i := range n {
		for x := 1; x < 256; x++ {
			b[i] = byte(x)
			h.addB(b)
			b[i] = 0
		}
	}

	// two non-zero bytes
	for i := range n {
		for x := 1; x < 256; x++ {
			b[i] = byte(x)
			for j := i + 1; j < n; j++ {
				for y := 1; y < 256; y++ {
					b[j] = byte(y)
					h.addB(b)
					b[j] = 0
				}
			}
			b[i] = 0
		}
	}
}

// Test strings with repeats, like "abcdabcdabcdabcd..."
func TestSmhasherCyclic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}
	t.Parallel()
	r := rand.New(rand.NewSource(1234))
	const REPEAT = 8
	const N = 1000000
	h := newHashSet()
	for n := 4; n <= 12; n++ {
		b := make([]byte, REPEAT*n)
		for i := range N {
			b[0] = byte(i * 79 % 97)
			b[1] = byte(i * 43 % 137)
			b[2] = byte(i * 151 % 197)
			b[3] = byte(i * 199 % 251)
			randBytes(r, b[4:n])
			for j := n; j < n*REPEAT; j++ {
				b[j] = b[j-n]
			}
			h.addB(b)
		}
		h.check(t)
	}
}

// Test strings with only a few bits set
func TestSmhasherSparse(t *testing.T) {
	if runtime.GOARCH == "wasm" {
		t.Skip("Too slow on wasm")
	}
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}
	t.Parallel()
	h := newHashSet()
	sparse(t, h, 32, 6)
	sparse(t, h, 40, 6)
	sparse(t, h, 48, 5)
	sparse(t, h, 56, 5)
	sparse(t, h, 64, 5)
	sparse(t, h, 96, 4)
	sparse(t, h, 256, 3)
	sparse(t, h, 2048, 2)
}
func sparse(t *testing.T, h *HashSet, n int, k int) {
	b := make([]byte, n/8)
	setbits(h, b, 0, k)
	h.check(t)
}

// set up to k bits at index i and greater
func setbits(h *HashSet, b []byte, i int, k int) {
	h.addB(b)
	if k == 0 {
		return
	}
	for j := i; j < len(b)*8; j++ {
		b[j/8] |= byte(1 << uint(j&7))
		setbits(h, b, j+1, k-1)
		b[j/8] &= byte(^(1 << uint(j&7)))
	}
}

// Test all possible combinations of n blocks from the set s.
// "permutation" is a bad name here, but it is what Smhasher uses.
func TestSmhasherPermutation(t *testing.T) {
	if runtime.GOARCH == "wasm" {
		t.Skip("Too slow on wasm")
	}
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}
	t.Parallel()
	h := newHashSet()
	permutation(t, h, []uint32{0, 1, 2, 3, 4, 5, 6, 7}, 8)
	permutation(t, h, []uint32{0, 1 << 29, 2 << 29, 3 << 29, 4 << 29, 5 << 29, 6 << 29, 7 << 29}, 8)
	permutation(t, h, []uint32{0, 1}, 20)
	permutation(t, h, []uint32{0, 1 << 31}, 20)
	permutation(t, h, []uint32{0, 1, 2, 3, 4, 5, 6, 7, 1 << 29, 2 << 29, 3 << 29, 4 << 29, 5 << 29, 6 << 29, 7 << 29}, 6)
}
func permutation(t *testing.T, h *HashSet, s []uint32, n int) {
	b := make([]byte, n*4)
	genPerm(h, b, s, 0)
	h.check(t)
}
func genPerm(h *HashSet, b []byte, s []uint32, n int) {
	h.addB(b[:n])
	if n == len(b) {
		return
	}
	for _, v := range s {
		binary.LittleEndian.PutUint32(b[n:], v)
		genPerm(h, b, s, n+4)
	}
}

type Key interface {
	clear()              // set bits all to 0
	random(r *rand.Rand) // set key to something random
	bits() int           // how many bits key has
	flipBit(i int)       // flip bit i of the key
	hash() uintptr       // hash the key
	name() string        // for error reporting
}

type BytesKey struct {
	b []byte
}

func (k *BytesKey) clear() {
	clear(k.b)
}
func (k *BytesKey) random(r *rand.Rand) {
	randBytes(r, k.b)
}
func (k *BytesKey) bits() int {
	return len(k.b) * 8
}
func (k *BytesKey) flipBit(i int) {
	k.b[i>>3] ^= byte(1 << uint(i&7))
}
func (k *BytesKey) hash() uintptr {
	return uintptr(Bytes(k.b, 0))
}
func (k *BytesKey) name() string {
	return fmt.Sprintf("bytes%d", len(k.b))
}

type Int32Key struct {
	i uint32
}

func (k *Int32Key) clear() {
	k.i = 0
}
func (k *Int32Key) random(r *rand.Rand) {
	k.i = r.Uint32()
}
func (k *Int32Key) bits() int {
	return 32
}
func (k *Int32Key) flipBit(i int) {
	k.i ^= 1 << uint(i)
}
func (k *Int32Key) hash() uintptr {
	return uintptr(Int32(k.i, 0))
}
func (k *Int32Key) name() string {
	return "int32"
}

type Int64Key struct {
	i uint64
}

func (k *Int64Key) clear() {
	k.i = 0
}
func (k *Int64Key) random(r *rand.Rand) {
	k.i = uint64(r.Uint32()) + uint64(r.Uint32())<<32
}
func (k *Int64Key) bits() int {
	return 64
}
func (k *Int64Key) flipBit(i int) {
	k.i ^= 1 << uint(i)
}
func (k *Int64Key) hash() uintptr {
	return uintptr(Int64(k.i, 0))
}
func (k *Int64Key) name() string {
	return "int64"
}

// Flipping a single bit of a key should flip each output bit with 50% probability.
func TestSmhasherAvalanche(t *testing.T) {
	if runtime.GOARCH == "wasm" {
		t.Skip("Too slow on wasm")
	}
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}

	t.Parallel()
	avalancheTest1(t, &BytesKey{make([]byte, 2)})
	avalancheTest1(t, &BytesKey{make([]byte, 4)})
	avalancheTest1(t, &BytesKey{make([]byte, 8)})
	avalancheTest1(t, &BytesKey{make([]byte, 16)})
	avalancheTest1(t, &BytesKey{make([]byte, 32)})
	avalancheTest1(t, &BytesKey{make([]byte, 200)})
	avalancheTest1(t, &Int32Key{})
	avalancheTest1(t, &Int64Key{})
}

func avalancheTest1(t *testing.T, k Key) {
	const REP = 100000
	r := rand.New(rand.NewSource(1234))
	n := k.bits()

	// grid[i][j] is a count of whether flipping
	// input bit i affects output bit j.
	grid := make([][hashSize]int, n)

	for range REP {
		// pick a random key, hash it
		k.random(r)
		h := k.hash()

		// flip each bit, hash & compare the results
		for i := range n {
			k.flipBit(i)
			d := h ^ k.hash()
			k.flipBit(i)

			// record the effects of that bit flip
			g := &grid[i]
			for j := range hashSize {
				g[j] += int(d & 1)
				d >>= 1
			}
		}
	}

	// Each entry in the grid should be about REP/2.
	// More precisely, we did N = k.bits() * hashSize experiments where
	// each is the sum of REP coin flips. We want to find bounds on the
	// sum of coin flips such that a truly random experiment would have
	// all sums inside those bounds with 99% probability.
	N := n * hashSize
	var c float64
	// find c such that Prob(mean-c*stddev < x < mean+c*stddev)^N > .9999
	for c = 0.0; math.Pow(math.Erf(c/math.Sqrt(2)), float64(N)) < .9999; c += .1 {
	}
	c *= 11.0 // allowed slack: 40% to 60% - we don't need to be perfectly random
	mean := .5 * REP
	stddev := .5 * math.Sqrt(REP)
	low := int(mean - c*stddev)
	high := int(mean + c*stddev)
	for i := range n {
		for j := range hashSize {
			x := grid[i][j]
			assert.Truef(
				t,
				x >= low && x <= high,
				"bad bias for %s bit %d -> bit %d: %d/%d",
				k.name(),
				i,
				j,
				x,
				REP,
			)
		}
	}
}

// All bit rotations of a set of distinct keys
func TestSmhasherWindowed(t *testing.T) {
	t.Parallel()
	h := newHashSet()
	t.Logf("32 bit keys")
	windowed(t, h, &Int32Key{})
	t.Logf("64 bit keys")
	windowed(t, h, &Int64Key{})
	t.Logf("string keys")
	windowed(t, h, &BytesKey{make([]byte, 128)})
}

func windowed(t *testing.T, h *HashSet, k Key) {
	if runtime.GOARCH == "wasm" {
		t.Skip("Too slow on wasm")
	}
	if unsafe.Sizeof(uintptr(0)) == 4 {
		// This test tends to be flaky on 32-bit systems.
		// There's not enough bits in the hash output, so we
		// expect a nontrivial number of collisions, and it is
		// often quite a bit higher than expected. See issue 43130.
		t.Skip("Flaky on 32-bit systems")
	}
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}
	const BITS = 16

	for r := 0; r < k.bits(); r++ {
		for i := range 1 << BITS {
			k.clear()
			for j := range BITS {
				if i>>uint(j)&1 != 0 {
					k.flipBit((j + r) % k.bits())
				}
			}
			h.add(k.hash())
		}
		h.check(t)
	}
}

// All keys of the form prefix + [A-Za-z0-9]*N + suffix.
func TestSmhasherText(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping in short mode")
	}
	t.Parallel()
	h := newHashSet()
	text(t, h, "Foo", "Bar")
	text(t, h, "FooBar", "")
	text(t, h, "", "FooBar")
}

func text(t *testing.T, h *HashSet, prefix, suffix string) {
	const N = 4
	const S = "ABCDEFGHIJKLMNOPQRSTabcdefghijklmnopqrst0123456789"
	const L = len(S)
	b := make([]byte, len(prefix)+N+len(suffix))
	copy(b, prefix)
	copy(b[len(prefix)+N:], suffix)
	c := b[len(prefix):]
	for i := range L {
		c[0] = S[i]
		for j := range L {
			c[1] = S[j]
			for k := range L {
				c[2] = S[k]
				for x := range L {
					c[3] = S[x]
					h.addB(b)
				}
			}
		}
	}
	h.check(t)
}

func TestSmhasherSeed(t *testing.T) {
	h := newHashSet()
	const N = 100000
	s := "hello"
	for i := range N {
		h.addS_seed(s, uintptr(i))
	}
	h.check(t)
}

// size of the hash output (32 or 64 bits)
const hashSize = 32 + int(^uintptr(0)>>63<<5)

func randBytes(r *rand.Rand, b []byte) {
	for i := range b {
		b[i] = byte(r.Uint32())
	}
}
