/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package buffer

import (
	"bytes"
	"crypto/rand"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRingBuffer_Write(t *testing.T) {
	rb := newCoreRing(64)

	_, err := rb.ReadByte()
	assert.ErrorIs(t, err, io.EOF, "expect nil err, but got nil")
	// check empty or full
	assert.True(t, rb.IsEmpty(), "expect IsEmpty is true but got false")
	assert.False(t, rb.IsFull(), "expect IsFull is false but got true")
	assert.Equalf(
		t,
		0,
		rb.Buffered(),
		"expect len 0 bytes but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		64,
		rb.Available(),
		"expect free 64 bytes but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)

	// write 4 * 4 = 16 bytes
	data := []byte(strings.Repeat("abcd", 4))
	n, _ := rb.Write(data)
	assert.Equalf(t, 16, n, "expect write 16 bytes but got %d", n)
	assert.Equalf(
		t,
		16,
		rb.Buffered(),
		"expect len 16 bytes but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		48,
		rb.Available(),
		"expect free 48 bytes but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		data,
		rb.Bytes(),
		"expect 4 abcd but got %s. r.w=%d, r.r=%d", rb.Bytes(), rb.w, rb.r,
	)

	// check empty or full
	assert.False(t, rb.IsEmpty(), "expect IsEmpty is false but got true")
	assert.False(t, rb.IsFull(), "expect IsFull is false but got true")

	// write 48 bytes, should full
	data = []byte(strings.Repeat("abcd", 12))
	n, _ = rb.Write(data)
	assert.Equalf(
		t,
		48,
		n,
		"expect write 48 bytes but got %d", n,
	)
	assert.Equalf(
		t,
		64,
		rb.Buffered(),
		"expect len 64 bytes but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		0,
		rb.Available(),
		"expect free 0 bytes but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		0,
		rb.w,
		"expect r.w=0 but got %d. r.r=%d", rb.w, rb.r,
	)
	assert.Equalf(
		t,
		[]byte(strings.Repeat("abcd", 16)),
		rb.Bytes(),
		"expect 16 abcd but got %s. r.w=%d, r.r=%d", rb.Bytes(), rb.w, rb.r,
	)

	// check empty or full
	assert.False(t, rb.IsEmpty(), "expect IsEmpty is false but got true")
	assert.True(t, rb.IsFull(), "expect IsFull is true but got false")
	assert.True(t, rb.IsFull(), "expect IsFull is true but got false")

	// write more 4 bytes, should scale from 64 to 128 bytes.
	n, _ = rb.Write([]byte(strings.Repeat("abcd", 1)))
	assert.Equalf(t, 4, n, "expect write 4 bytes but got %d", n)

	size := rb.Cap()
	assert.Equalf(
		t,
		68,
		rb.Buffered(),
		"expect len 68 bytes but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		size-68,
		rb.Available(),
		"expect free 60 bytes but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)

	// check empty or full
	assert.False(t, rb.IsEmpty(), "expect IsEmpty is false but got true")
	assert.False(t, rb.IsFull(), "expect IsFull is false but got true")

	// reset this ringbuffer and set a long slice
	rb.Reset()
	n, _ = rb.Write([]byte(strings.Repeat("abcd", 20)))
	assert.Equalf(t, 80, n, "expect write 80 bytes but got %d", n)
	assert.Equalf(
		t,
		80,
		rb.Buffered(),
		"expect len 80 bytes but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		size-80,
		rb.Available(),
		"expect free 48 bytes but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)
	assert.Greaterf(t, rb.w, 0, "expect r.w>=0 but got %d. r.r=%d", rb.w, rb.r)

	// check empty or full
	assert.False(t, rb.IsEmpty(), "expect IsEmpty is false but got true")
	assert.False(t, rb.IsFull(), "expect IsFull is false but got true")

	assert.Equalf(
		t,
		[]byte(strings.Repeat("abcd", 20)),
		rb.Bytes(),
		"expect 16 abcd but got %s. r.w=%d, r.r=%d", rb.Bytes(), rb.w, rb.r,
	)

	rb.Reset()
	size = rb.Cap()
	// write 4 * 2 = 8 bytes
	n, _ = rb.Write([]byte(strings.Repeat("abcd", 2)))
	assert.Equalf(t, 8, n, "expect write 16 bytes but got %d", n)
	assert.Equalf(
		t,
		8,
		rb.Buffered(),
		"expect len 16 bytes but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		size-8,
		rb.Available(),
		"expect free 48 bytes but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)

	buf := make([]byte, 5)
	_, _ = rb.Read(buf)
	assert.Equalf(
		t,
		3,
		rb.Buffered(),
		"expect len 3 bytes but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	_, _ = rb.Write([]byte(strings.Repeat("abcd", 15)))

	assert.Equalf(
		t,
		[]byte("bcd"+strings.Repeat("abcd", 15)),
		rb.Bytes(),
		"expect 63 ... but got %s. r.w=%d, r.r=%d", rb.Bytes(), rb.w, rb.r,
	)

	rb.Reset()
	n, _ = rb.Write([]byte(strings.Repeat("abcd", 32)))
	assert.Equalf(t, 128, n, "expect write 128 bytes but got %d", n)
	assert.Equalf(
		t,
		0,
		rb.Available(),
		"expect free 0 bytes but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)

	buf = make([]byte, 16)
	_, _ = rb.Read(buf)
	n, _ = rb.Write([]byte(strings.Repeat("1234", 4)))
	assert.Equalf(t, 16, n, "expect write 16 bytes but got %d", n)
	assert.Equalf(
		t,
		0,
		rb.Available(),
		"expect free 0 bytes but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		[]byte(strings.Repeat("abcd", 32)+strings.Repeat("1234", 4)),
		append(buf, rb.Bytes()...),
		"expect 16 abcd and 4 1234 but got %s. r.w=%d, r.r=%d", rb.Bytes(), rb.w, rb.r,
	)
}

func TestZeroRingBuffer(t *testing.T) {
	rb := newCoreRing(0)
	head, tail := rb.Peek(2)
	assert.Empty(t, head, "head should be empty")
	assert.Empty(t, tail, "tail should be empty")

	head, tail = rb.Peek(0)
	assert.Empty(t, head, "head should be empty")
	assert.Empty(t, tail, "tail should be empty")
	assert.Equal(t, 0, rb.Buffered(), "expect length is 0")
	assert.Equal(t, 0, rb.Available(), "expect free is 0")

	buf := []byte(strings.Repeat("1234", 12))
	_, _ = rb.Write(buf)
	assert.Equalf(
		t,
		RingDefaultBufferSize,
		rb.Len(),
		"expect rb.Len()=%d, but got rb.Len()=%d", RingDefaultBufferSize, rb.Len(),
	)
	assert.Equalf(
		t,
		RingDefaultBufferSize,
		rb.Cap(),
		"expect rb.Cap()=%d, but got rb.Cap()=%d", RingDefaultBufferSize, rb.Cap(),
	)
	assert.Truef(
		t,
		rb.r == 0 && rb.w == 48 && rb.size == RingDefaultBufferSize,
		"expect rb.r=0, rb.w=48, rb.size=64, rb.mask=63, but got rb.r=%d, rb.w=%d, rb.size=%d",
		rb.r,
		rb.w,
		rb.size,
	)
	assert.Equal(t, buf, rb.Bytes(), "expect it is equal")

	_, _ = rb.Discard(48)
	assert.Truef(
		t,
		rb.IsEmpty() && rb.r == 0 && rb.w == 0,
		"expect rb is empty and rb.r=rb.w=0, but got rb.r=%d and rb.w=%d", rb.r, rb.w,
	)
}

func TestRingBufferGrow(t *testing.T) {
	rb := newCoreRing(0)
	head, tail := rb.Peek(2)
	assert.Empty(t, head, "head should be empty")
	assert.Empty(t, tail, "tail should be empty")

	data := make([]byte, RingDefaultBufferSize+1)
	n, err := rand.Read(data)
	assert.NoError(t, err, "failed to generate random data")
	assert.Equalf(
		t,
		RingDefaultBufferSize+1,
		n,
		"expect random data length is %d but got %d", RingDefaultBufferSize+1, n,
	)

	m, err := rb.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, RingDefaultBufferSize+1, m)
	assert.Equal(t, 2*RingDefaultBufferSize, rb.Cap())
	assert.Equal(t, 2*RingDefaultBufferSize, rb.Len())
	assert.Equal(t, RingDefaultBufferSize+1, rb.Buffered())
	assert.Equal(t, RingDefaultBufferSize-1, rb.Available())
	assert.Equal(t, data, rb.Bytes())

	rb = newCoreRing(RingDefaultBufferSize)
	newData := make([]byte, 3*512)
	n, err = rand.Read(newData)
	assert.NoError(t, err, "failed to generate random data")
	assert.Equalf(t, 3*512, n, "expect random data length is %d but got %d", 3*512, n)

	m, err = rb.Write(newData)
	assert.NoError(t, err)
	assert.Equal(t, 3*512, m)
	assert.Equal(t, 2*RingDefaultBufferSize, rb.Cap())
	assert.Equal(t, 2*RingDefaultBufferSize, rb.Len())
	assert.Equal(t, 3*512, rb.Buffered())
	assert.Equal(t, 512, rb.Available())
	assert.Equal(t, newData, rb.Bytes())

	rb.Reset()
	data = make([]byte, bufferGrowThreshold)
	n, err = rand.Read(data)
	assert.NoError(t, err, "failed to generate random data")
	assert.Equalf(
		t,
		bufferGrowThreshold,
		n,
		"expect random data length is %d but got %d", bufferGrowThreshold, n,
	)

	m, err = rb.Write(data)
	assert.NoError(t, err)
	assert.Equal(t, bufferGrowThreshold, m)
	assert.Equal(t, bufferGrowThreshold, rb.Cap())
	assert.Equal(t, bufferGrowThreshold, rb.Len())
	assert.Equal(t, bufferGrowThreshold, rb.Buffered())
	assert.Equal(t, 0, rb.Available())
	assert.Equal(t, data, rb.Bytes())

	newData = make([]byte, bufferGrowThreshold/2+1)
	n, err = rand.Read(newData)
	assert.NoError(t, err, "failed to generate random data")
	assert.Equalf(
		t,
		bufferGrowThreshold/2+1,
		n,
		"expect random data length is %d but got %d", bufferGrowThreshold, n,
	)

	m, err = rb.Write(newData)
	assert.NoError(t, err)
	assert.Equal(t, bufferGrowThreshold/2+1, m)
	// grow asks for 1.25*1.25*bufferGrowThreshold bytes. slicepool.Get rounds this up to
	// 2*bufferGrowThreshold, and the ring uses all of this capacity.
	assert.Equal(t, 2*bufferGrowThreshold, rb.Cap())
	assert.Equal(t, 2*bufferGrowThreshold, rb.Len())
	assert.Equal(t, 1.5*bufferGrowThreshold+1, float64(rb.Buffered()))
	assert.Equal(t, 2*bufferGrowThreshold-rb.Buffered(), rb.Available())
	assert.Equal(t, append(data, newData...), rb.Bytes())
}

func TestRingBuffer_Read(t *testing.T) {
	rb := newCoreRing(64)

	// check empty or full
	assert.True(t, rb.IsEmpty(), "expect IsEmpty is true but got false")
	assert.False(t, rb.IsFull(), "expect isfull is false but got true")
	assert.Equalf(
		t,
		0,
		rb.Buffered(),
		"expect len 0 bytes but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		64,
		rb.Available(),
		"expect free 64 bytes but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)

	// read empty
	buf := make([]byte, 1024)
	n, err := rb.Read(buf)
	assert.ErrorIs(t, err, io.EOF, "expect ErrIsEmpty but got nil")
	assert.Equalf(t, 0, n, "expect read 0 bytes but got %d", n)
	assert.Equalf(
		t,
		0,
		rb.Buffered(),
		"expect len 0 bytes but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		64,
		rb.Available(),
		"expect free 64 bytes but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)
	assert.Equalf(t, 0, rb.r, "expect r.r=0 but got %d. r.w=%d", rb.r, rb.w)

	// write 16 bytes to read
	_, _ = rb.Write([]byte(strings.Repeat("abcd", 4)))
	// read all data from buffer, it will be shrunk from 64 to 32.
	n, err = rb.Read(buf)
	assert.NoErrorf(t, err, "read failed: %v", err)
	assert.Equalf(t, 16, n, "expect read 16 bytes but got %d", n)
	assert.Equalf(
		t,
		0,
		rb.Buffered(),
		"expect len 0 bytes but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		64,
		rb.Available(),
		"expect free 64 bytes but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)
	assert.Equalf(t, 0, rb.r, "expect r.r=0 but got %d. r.w=%d", rb.r, rb.w)

	// write long slice to read, it will scale from 32 to 128 bytes.
	_, _ = rb.Write([]byte(strings.Repeat("abcd", 20)))
	// read all data from buffer, it will be shrunk from 128 to 64.
	n, err = rb.Read(buf)
	assert.NoErrorf(t, err, "read failed: %v", err)
	assert.Equalf(t, 80, n, "expect read 80 bytes but got %d", n)
	assert.Equalf(
		t,
		0,
		rb.Buffered(),
		"expect len 0 bytes but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		128,
		rb.Available(),
		"expect free 128 bytes but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)
	assert.Equalf(t, 0, rb.r, "expect r.r=0 but got %d. r.w=%d", rb.r, rb.w)

	rb.Reset()
	_, _ = rb.Write([]byte(strings.Repeat("1234", 32)))
	assert.True(t, rb.IsFull(), "ring buffer should be full")
	assert.Equalf(
		t,
		0,
		rb.Available(),
		"expect free 0 bytes but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)
	assert.Equalf(t, 0, rb.w, "expect r.2=0 but got %d. r.r=%d", rb.w, rb.r)

	head, tail := rb.Peek(64)
	assert.Truef(
		t,
		len(head) == 64 && tail == nil,
		"expect len(head)=64 and tail=nil, yet len(head)=%d and tail != nil", len(head),
	)
	assert.Equalf(t, 0, rb.r, "expect r.r=0 but got %d", rb.r)
	assert.Equal(t, []byte(strings.Repeat("1234", 16)), head)
	_, _ = rb.Discard(64)
	assert.Equalf(t, 64, rb.r, "expect r.r=64 but got %d", rb.r)
	_, _ = rb.Write([]byte(strings.Repeat("1234", 4)))
	assert.Equalf(t, 16, rb.w, "expect r.w=16 but got %d", rb.w)

	head, tail = rb.Peek(128)
	assert.Truef(
		t,
		len(head) == 64 && len(tail) == 16,
		"expect len(head)=64 and len(tail)=16, yet len(head)=%d and len(tail)=%d",
		len(head),
		len(tail),
	)
	assert.Equal(t, []byte(strings.Repeat("1234", 16)), head)
	assert.Equal(t, []byte(strings.Repeat("1234", 4)), tail)

	head, tail = rb.Peek(0)
	assert.Truef(
		t,
		len(head) == 64 && len(tail) == 16,
		"expect len(head)=64 and len(tail)=16, yet len(head)=%d and len(tail)=%d",
		len(head),
		len(tail),
	)
	assert.Equal(t, []byte(strings.Repeat("1234", 16)), head)
	assert.Equal(t, []byte(strings.Repeat("1234", 4)), tail)

	_, _ = rb.Discard(64)
	_, _ = rb.Discard(16)
	assert.True(t, rb.IsEmpty(), "should be empty")
}

func TestRingBuffer_ByteInterface(t *testing.T) {
	rb := newCoreRing(2)

	// write one
	_ = rb.WriteByte('a')
	assert.Equalf(
		t,
		1,
		rb.Buffered(),
		"expect len 1 byte but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		1,
		rb.Available(),
		"expect free 1 byte but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		[]byte{'a'},
		rb.Bytes(),
		"expect a but got %s. r.w=%d, r.r=%d", rb.Bytes(), rb.w, rb.r,
	)
	// check empty or full
	assert.Falsef(t, rb.IsEmpty(), "expect IsEmpty is false but got true")
	assert.False(t, rb.IsFull(), "expect IsFull is false but got true")

	// write two, isFull
	_ = rb.WriteByte('b')
	assert.Equalf(
		t,
		2,
		rb.Buffered(),
		"expect len 2 bytes but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		0,
		rb.Available(),
		"expect free 0 byte but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		[]byte{'a', 'b'},
		rb.Bytes(),
		"expect a but got %s. r.w=%d, r.r=%d", rb.Bytes(), rb.w, rb.r,
	)
	// check empty or full
	assert.False(t, rb.IsEmpty(), "expect IsEmpty is false but got true")
	assert.True(t, rb.IsFull(), "expect IsFull is true but got false")

	// write, it will scale from 2 to 4 bytes.
	_ = rb.WriteByte('c')
	assert.Equalf(
		t,
		3,
		rb.Buffered(),
		"expect len 3 bytes but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		1,
		rb.Available(),
		"expect free 1 byte but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		[]byte{'a', 'b', 'c'},
		rb.Bytes(),
		"expect a but got %s. r.w=%d, r.r=%d", rb.Bytes(), rb.w, rb.r,
	)
	// check empty or full
	assert.False(t, rb.IsEmpty(), "expect IsEmpty is false but got true")
	assert.False(t, rb.IsFull(), "expect IsFull is false but got true")

	// read one
	b, err := rb.ReadByte()
	assert.NoErrorf(t, err, "ReadByte failed: %v", err)
	assert.Equal(t, byte('a'), b, "expect a but got %c. r.w=%d, r.r=%d", b, rb.w, rb.r)
	assert.Equalf(
		t,
		2,
		rb.Buffered(),
		"expect len 2 byte but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		2,
		rb.Available(),
		"expect free 2 byte but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		[]byte{'b', 'c'},
		rb.Bytes(),
		"expect a but got %s. r.w=%d, r.r=%d", rb.Bytes(), rb.w, rb.r,
	)
	// check empty or full
	assert.False(t, rb.IsEmpty(), "expect IsEmpty is false but got true")
	assert.False(t, rb.IsFull(), "expect IsFull is false but got true")

	// read two
	b, _ = rb.ReadByte()
	assert.Equal(t, byte('b'), b, "expect b but got %c. r.w=%d, r.r=%d", b, rb.w, rb.r)
	assert.Equalf(
		t,
		1,
		rb.Buffered(),
		"expect len 1 byte but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		3,
		rb.Available(),
		"expect free 3 byte but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)
	// check empty or full
	assert.False(t, rb.IsEmpty(), "expect IsEmpty is false but got true")
	assert.False(t, rb.IsFull(), "expect IsFull is false but got true")

	// read three
	_, _ = rb.ReadByte()
	assert.Equalf(
		t,
		0,
		rb.Buffered(),
		"expect len 0 byte but got %d. r.w=%d, r.r=%d", rb.Buffered(), rb.w, rb.r,
	)
	assert.Equalf(
		t,
		4,
		rb.Available(),
		"expect free 4 byte but got %d. r.w=%d, r.r=%d", rb.Available(), rb.w, rb.r,
	)
	// check empty or full
	assert.True(t, rb.IsEmpty(), "expect IsEmpty is true but got false")
	assert.False(t, rb.IsFull(), "expect IsFull is false but got true")
}

func TestRingBuffer_WriteByte_GrowsFullLargeRing(t *testing.T) {
	for _, tt := range []struct {
		name    string
		size    int
		wrapped bool
	}{
		{
			name:    "4096/contiguous",
			size:    4096,
			wrapped: false,
		},
		{
			name:    "4096/wrapped",
			size:    4096,
			wrapped: true,
		},
		{
			name:    "8192/contiguous",
			size:    8192,
			wrapped: false,
		},
		{
			name:    "8192/wrapped",
			size:    8192,
			wrapped: true,
		},
	} {
		t.Run(
			tt.name,
			func(t *testing.T) {
				rb := newCoreRing(tt.size)
				data := make([]byte, tt.size)
				for i := range data {
					data[i] = byte(i % 251)
				}
				n, err := rb.Write(data)
				require.NoError(t, err)
				require.Equal(t, len(data), n)

				if tt.wrapped {
					b, err := rb.ReadByte()
					require.NoError(t, err)
					require.Equal(t, data[0], b)
					require.NoError(t, rb.WriteByte('w'))
				}
				require.True(t, rb.IsFull())

				want := append(rb.Bytes(), 'x')
				require.NoError(t, rb.WriteByte('x'))
				require.Greater(t, rb.Cap(), tt.size)
				require.Equal(t, tt.size+1, rb.Buffered())

				out := make([]byte, len(want))
				n, err = rb.Read(out)
				require.NoError(t, err)
				require.Equal(t, len(want), n)
				require.Equal(t, want, out)
				require.True(t, rb.IsEmpty())
			},
		)
	}
}

func TestRingBuffer_ReadFrom(t *testing.T) {
	const dataLen = 4 * 1024
	const prefixLen = 2 * 1024
	const partLen = 1024
	const initLen = 5 * 1024

	rb := newCoreRing(0)
	data := make([]byte, dataLen)
	_, err := rand.Read(data)
	require.NoError(t, err)

	r := bytes.NewReader(data)
	n, err := rb.ReadFrom(r)
	require.NoError(t, err)
	require.False(t, rb.IsEmpty())
	require.Equalf(t, dataLen, int(n), "ringbuffer should read %d bytes, but got %d", dataLen, n)
	require.Equalf(
		t,
		dataLen,
		rb.Buffered(),
		"ringbuffer should have %d bytes, but got %d", dataLen, rb.Buffered(),
	)
	buf, _ := rb.Peek(0)
	require.Equal(t, data, buf)

	buf = make([]byte, dataLen)
	var m int
	m, err = rb.Read(buf)
	require.NoError(t, err)
	require.Equalf(t, dataLen, m, "ringbuffer should read %d bytes, but got %d", dataLen, m)
	require.Equal(t, data, buf)
	require.Truef(t, rb.IsEmpty(), "ringbuffer should be empty, but it isn't")
	require.Zerof(
		t,
		rb.Buffered(),
		"ringbuffer should be empty, but still have %d bytes", rb.Buffered(),
	)

	rb = newCoreRing(0)
	prefix := make([]byte, prefixLen)
	_, err = rand.Read(prefix)
	require.NoError(t, err)

	_, err = rand.Read(data)
	require.NoError(t, err)

	r.Reset(data)
	m, err = rb.Write(prefix)
	require.NoError(t, err)
	require.Equalf(t, prefixLen, m, "ringbuffer should read %d bytes, but got %d", prefixLen, m)

	n, err = rb.ReadFrom(r)
	require.NoError(t, err)
	require.Equalf(t, dataLen, int(n), "ringbuffer should read %d bytes, but got %d", dataLen, n)
	require.Equalf(
		t,
		prefixLen+dataLen,
		rb.Buffered(),
		"ringbuffer should have %d bytes, but got %d", prefixLen+dataLen, rb.Buffered(),
	)

	head, tail := rb.Peek(prefixLen)
	require.Nil(t, tail)
	require.Equal(t, prefix, head)
	_, _ = rb.Discard(prefixLen)
	require.Equalf(
		t,
		dataLen,
		rb.Buffered(),
		"ringbuffer should have %d bytes, but got %d", dataLen, rb.Buffered(),
	)

	head, tail = rb.Peek(0)
	require.Nil(t, tail)
	require.Equal(t, data, head)
	_, _ = rb.Discard(dataLen)
	require.Truef(t, rb.IsEmpty(), "ringbuffer should be empty, but it isn't")
	require.Zerof(
		t,
		rb.Buffered(),
		"ringbuffer should be empty, but still have %d bytes", rb.Buffered(),
	)

	rb = newCoreRing(initLen)
	_, err = rand.Read(prefix)
	require.NoError(t, err)

	_, err = rand.Read(data)
	require.NoError(t, err)

	r.Reset(data)
	m, err = rb.Write(prefix)
	require.NoError(t, err)
	require.Equalf(t, prefixLen, m, "ringbuffer should read %d bytes, but got %d", prefixLen, m)

	head, tail = rb.Peek(partLen)
	require.Nil(t, tail)
	require.Equal(t, prefix[:partLen], head)

	_, _ = rb.Discard(partLen)
	n, err = rb.ReadFrom(r)
	require.NoError(t, err)
	require.Equalf(t, dataLen, int(n), "ringbuffer should read %d bytes, but got %d", dataLen, n)
	buf, tail = rb.Peek(0)

	buf = append(buf, tail...)
	require.Equal(t, append(prefix[partLen:], data...), buf)
	_, _ = rb.Discard(prefixLen + dataLen - partLen)
	require.Truef(t, rb.IsEmpty(), "ringbuffer should be empty, but it isn't")
	require.Zerof(
		t,
		rb.Buffered(),
		"ringbuffer should be empty, but still have %d bytes", rb.Buffered(),
	)
}

func TestRingBuffer_WriteTo(t *testing.T) {
	const dataLen = 4 * 1024
	const partLen = 1024

	rb := newCoreRing(5 * 1024)
	data := make([]byte, dataLen)
	_, err := rand.Read(data)
	require.NoError(t, err)

	n, err := rb.Write(data)
	require.NoError(t, err)
	require.Equalf(t, dataLen, n, "ringbuffer should write %d bytes, but got %d", dataLen, n)

	buf := bytes.NewBuffer(nil)
	var m int64
	m, err = rb.WriteTo(buf)
	require.NoError(t, err)
	require.True(t, rb.IsEmpty())
	require.Equalf(t, dataLen, int(m), "ringbuffer should write %d bytes, but got %d", dataLen, m)
	require.Equal(t, data, buf.Bytes())

	buf.Reset()
	_, err = rand.Read(data)
	require.NoError(t, err)

	rb = newCoreRing(dataLen)
	n, err = rb.Write(data)
	require.NoError(t, err)
	require.Equalf(t, dataLen, n, "ringbuffer should write %d bytes, but got %d", dataLen, n)
	require.Truef(t, rb.IsFull(), "ringbuffer should be full, but it isn't")

	m, err = rb.WriteTo(buf)
	require.NoError(t, err)
	require.True(t, rb.IsEmpty())
	require.Equalf(t, dataLen, int(m), "ringbuffer should write %d bytes, but got %d", dataLen, m)
	require.Equal(t, data, buf.Bytes())

	buf.Reset()
	rb.Reset()
	_, err = rand.Read(data)
	require.NoError(t, err)

	n, err = rb.Write(data)
	require.NoError(t, err)
	require.Equalf(t, dataLen, n, "ringbuffer should write %d bytes, but got %d", dataLen, n)
	require.Truef(t, rb.IsFull(), "ringbuffer should be full, but it isn't")

	head, tail := rb.Peek(partLen)
	require.Nil(t, tail)
	require.Equal(t, data[:partLen], head)

	_, _ = rb.Discard(partLen)
	partData := make([]byte, partLen/2)
	_, err = rand.Read(partData)
	require.NoError(t, err)

	n, err = rb.Write(partData)
	require.NoError(t, err)
	require.Equalf(t, partLen/2, n, "ringbuffer should write %d bytes, but got %d", dataLen, n)
	require.Equal(t, partLen/2, rb.Available())

	m, err = rb.WriteTo(buf)
	require.NoError(t, err)
	require.Equalf(
		t,
		dataLen-partLen/2,
		int(m),
		"ringbuffer should write %d bytes, but got %d", dataLen-partLen/2, m,
	)
	require.Equal(t, append(data[partLen:], partData...), buf.Bytes())
	require.True(t, rb.IsEmpty())
}

func TestGap_WriteString(t *testing.T) {
	rb := newCoreRing(16)
	n, err := rb.WriteString("hello world")
	require.NoError(t, err)
	require.Equal(t, 11, n)
	require.Equal(t, []byte("hello world"), rb.Bytes())
}

func TestGap_DiscardMoreThanBuffered(t *testing.T) {
	rb := newCoreRing(16)
	_, _ = rb.Write([]byte("0123456789"))
	discarded, err := rb.Discard(1000)
	require.NoError(t, err)
	require.Equal(t, 10, discarded)
	require.True(t, rb.IsEmpty())
}

func TestGap_WriteTo_PartialWriteWithError_PreservesRemainder(t *testing.T) {
	rb := newCoreRing(128)
	_, _ = rb.Write(make([]byte, 100))

	w := &partialErrWriter{
		accept: 40,
		err:    errBoom,
	}
	m, err := rb.WriteTo(w)
	require.Error(t, err)
	require.Equal(t, int64(40), m)
	// unlike LinkedList.WriteTo, Ring.WriteTo advances rb.r by exactly the
	// bytes actually written, so the remaining 60 bytes must stay in the
	// buffer, retryable.
	require.Equal(t, 60, rb.Buffered())
}

func TestGap_WriteTo_ShortWriteNilErr_PreservesRemainder(t *testing.T) {
	rb := newCoreRing(128)
	_, _ = rb.Write([]byte("0123456789"))

	w := &shortWriter{accept: 4}
	m, err := rb.WriteTo(w)
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Equal(t, int64(4), m)
	require.Equal(t, 6, rb.Buffered())

	out := make([]byte, 6)
	n, rerr := rb.Read(out)
	require.NoError(t, rerr)
	require.Equal(t, 6, n)
	require.Equal(t, []byte("456789"), out)
}

func TestGap_ReadFrom_DataAndEOFTogether(t *testing.T) {
	data := make([]byte, 100)
	_, _ = rand.Read(data)
	r := &dataThenEOFReader{data: data}

	rb := newCoreRing(0)
	n, err := rb.ReadFrom(r)
	require.NoError(t, err)
	require.Equal(t, int64(100), n)
	require.Equal(t, 100, rb.Buffered())
	require.Equal(t, data, rb.Bytes())
}

type errReaderAfter struct {
	data []byte
	sent bool
	err  error
}

func (r *errReaderAfter) Read(p []byte) (int, error) {
	if !r.sent {
		n := copy(p, r.data)
		r.sent = true
		return n, nil
	}
	return 0, r.err
}

func TestGap_ReadFrom_ErrorMidway_PreservesWhatWasRead(t *testing.T) {
	data := make([]byte, 100)
	_, _ = rand.Read(data)
	r := &errReaderAfter{
		data: data,
		err:  errBoom,
	}

	rb := newCoreRing(0)
	n, err := rb.ReadFrom(r)
	require.ErrorIs(t, err, errBoom)
	require.Equal(t, int64(100), n)
	require.Equal(t, 100, rb.Buffered())
	require.Equal(t, data, rb.Bytes())
}

func TestGap_DiscardZero(t *testing.T) {
	rb := newCoreRing(16)
	_, _ = rb.Write([]byte("abcdef"))
	discarded, err := rb.Discard(0)
	require.NoError(t, err)
	require.Equal(t, 0, discarded)
	require.Equal(t, 6, rb.Buffered())
	require.False(t, rb.IsEmpty())
}

// countReader counts the calls to Read. A test uses it to make sure that readFrom stops at the limit.
type countReader struct {
	r     io.Reader
	calls int
}

func (c *countReader) Read(p []byte) (int, error) {
	c.calls++
	return c.r.Read(p)
}

func TestCoreRing_ReadFromLimit_StopsAtLimit(t *testing.T) {
	data := make([]byte, 5000)
	_, _ = rand.Read(data)
	src := bytes.NewReader(data)

	rb := newCoreRing(0)
	n, err := rb.readFrom(src, 3000)
	require.NoError(t, err)
	require.Equal(t, int64(3000), n)
	require.Equal(t, data[:3000], rb.Bytes())
	// readFrom must not read more than the limit from the source.
	require.Equal(t, 2000, src.Len())
}

func TestCoreRing_ReadFromLimit_ReturnsEOFBeforeLimit(t *testing.T) {
	data := make([]byte, 100)
	_, _ = rand.Read(data)

	rb := newCoreRing(0)
	n, err := rb.readFrom(bytes.NewReader(data), 3000)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int64(100), n)
	require.Equal(t, data, rb.Bytes())
}

func TestCoreRing_ReadFromLimit_NoReadAfterLimit(t *testing.T) {
	src := &countReader{r: bytes.NewReader(make([]byte, 100))}
	rb := newCoreRing(0)
	n, err := rb.readFrom(src, 100)
	require.NoError(t, err)
	require.Equal(t, int64(100), n)
	require.Equal(t, 1, src.calls)
}

func TestCoreRing_ReadFromLimit_ZeroLimitIsNoop(t *testing.T) {
	src := &countReader{r: bytes.NewReader(make([]byte, 100))}
	rb := newCoreRing(0)
	n, err := rb.readFrom(src, 0)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Zero(t, src.calls)
	require.True(t, rb.IsEmpty())
}

func TestCoreRing_ReadFromLimit_GrowRequestStaysInLimit(t *testing.T) {
	// A ring of 4096 bytes that grows to hold 5000 bytes must not ask for more than 5000 bytes.
	rb := newCoreRing(4096)
	n, err := rb.readFrom(bytes.NewReader(make([]byte, 1<<20)), 5000)
	require.NoError(t, err)
	require.Equal(t, int64(5000), n)
	require.Less(t, rb.Cap(), 2*5000)
}

// grow must use all the capacity that slicepool.Get gives. Then the next grow is not necessary
// until the ring fills this capacity.
func TestCoreRing_GrowUsesFullPoolCapacity(t *testing.T) {
	rb := newCoreRing(bufferGrowThreshold)
	_, err := rb.Write(make([]byte, bufferGrowThreshold+1))
	require.NoError(t, err)
	require.Equal(t, cap(rb.buf), rb.Cap())
	require.Equal(t, len(rb.buf), rb.Cap())

	before := rb.Cap()
	_, err = rb.Write(make([]byte, rb.Available()))
	require.NoError(t, err)
	require.Equal(t, before, rb.Cap(), "the ring must fill its capacity without a new grow")
}

// The 1.25 growth loop can go above two times the old size. grow must limit the request to doubleCap.
// Otherwise, slicepool.Get rounds the request up to four times the old size.
func TestCoreRing_GrowDoesNotGoAboveDoubleCap(t *testing.T) {
	rb := newCoreRing(bufferGrowThreshold)
	// 1.25^3 is approximately 1.95. A request of 1.97 times the size makes the loop go to 1.25^4.
	_, err := rb.Write(make([]byte, bufferGrowThreshold*197/100))
	require.NoError(t, err)
	require.Equal(t, 2*bufferGrowThreshold, rb.Cap())
}

// negWriter returns a negative count after okCalls calls that accept all bytes. This breaks the io.Writer contract.
type negWriter struct {
	okCalls int
	calls   int
}

func (w *negWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls <= w.okCalls {
		return len(p), nil
	}
	return -1, nil
}

// WriteTo must panic before it changes the read pointer. Thus, the data in the ring stays correct.
func TestCoreRing_WriteToNegativeCountPanics(t *testing.T) {
	cases := []struct {
		name    string
		setup   func() *coreRing
		okCalls int
	}{
		{
			name: "contiguous",
			setup: func() *coreRing {
				rb := newCoreRing(16)
				_, _ = rb.WriteString("abcdef")
				return rb
			},
		},
		{
			name: "full",
			setup: func() *coreRing {
				rb := newCoreRing(16)
				_, _ = rb.Write(bytes.Repeat([]byte("x"), 16))
				return rb
			},
		},
		{
			name:  "wrapped first part",
			setup: wrappedRing,
		},
		{
			name:    "wrapped second part",
			setup:   wrappedRing,
			okCalls: 1,
		},
	}
	for _, tc := range cases {
		t.Run(
			tc.name,
			func(t *testing.T) {
				rb := tc.setup()
				before := rb.Bytes()
				require.Panics(t, func() { _, _ = rb.WriteTo(&negWriter{okCalls: tc.okCalls}) })
				if tc.okCalls == 0 {
					require.Equal(t, before, rb.Bytes())
				}
				require.GreaterOrEqual(t, rb.r, 0)
			},
		)
	}
}

// wrappedRing returns a ring of 16 bytes whose data goes past the end of the buffer.
func wrappedRing() *coreRing {
	rb := newCoreRing(16)
	_, _ = rb.Write(make([]byte, 12))
	_, _ = rb.Discard(8)
	_, _ = rb.WriteString("abcdefgh") // w goes past the end and starts at 0 again
	return rb
}

func TestLinkedListBuffer_WriteToNegativeCountPanics(t *testing.T) {
	var llb LinkedList
	llb.PushBack([]byte("abcdef"))
	require.Panics(t, func() { _, _ = llb.WriteTo(&negWriter{}) })
}
