// Copyright (c) 2016 Uber Technologies, Inc.
// SPDX-License-Identifier: MIT
//
// Forked from go.uber.org/zap/buffer/buffer.go at v1.28.0.

// Package buffer gives a thin wrapper around a byte slice. The bytes.Buffer of
// the standard library does not support the zero-allocation formatters of
// strconv. This package supports some of them.
package buffer

import (
	"strconv"
	"time"
)

const size = 1024 // default capacity of a new buffer: 1 KiB

// Buffer is a thin wrapper around a byte slice. A pool holds each Buffer.
// Therefore, Get is the only way to make a Buffer.
type Buffer struct {
	bs []byte
}

// AppendByte writes a single byte to the Buffer.
func (b *Buffer) AppendByte(v byte) {
	b.bs = append(b.bs, v)
}

// AppendBytes writes the given slice of bytes to the Buffer.
func (b *Buffer) AppendBytes(v []byte) {
	b.bs = append(b.bs, v...)
}

// AppendString writes a string to the Buffer.
func (b *Buffer) AppendString(s string) {
	b.bs = append(b.bs, s...)
}

// AppendInt appends an integer in base 10 to the underlying buffer.
func (b *Buffer) AppendInt(i int64) {
	b.bs = strconv.AppendInt(b.bs, i, 10)
}

// AppendTime appends the time formatted using the specified layout.
func (b *Buffer) AppendTime(t time.Time, layout string) {
	b.bs = t.AppendFormat(b.bs, layout)
}

// AppendUint appends an unsigned integer in base 10 to the underlying buffer.
func (b *Buffer) AppendUint(i uint64) {
	b.bs = strconv.AppendUint(b.bs, i, 10)
}

// AppendBool appends a bool to the underlying buffer.
func (b *Buffer) AppendBool(v bool) {
	b.bs = strconv.AppendBool(b.bs, v)
}

// AppendFloat appends a float to the underlying buffer. It does not quote NaN
// or +/- Inf.
func (b *Buffer) AppendFloat(f float64, bitSize int) {
	b.bs = strconv.AppendFloat(b.bs, f, 'f', -1, bitSize)
}

// Len returns the length of the underlying byte slice.
func (b *Buffer) Len() int {
	return len(b.bs)
}

// Cap returns the capacity of the underlying byte slice.
func (b *Buffer) Cap() int {
	return cap(b.bs)
}

// Bytes returns a mutable reference to the underlying byte slice.
func (b *Buffer) Bytes() []byte {
	return b.bs
}

// String returns a string copy of the underlying byte slice.
func (b *Buffer) String() string {
	return string(b.bs)
}

// Reset sets the length of the underlying byte slice to zero. Each later write
// uses the same backing array again.
func (b *Buffer) Reset() {
	b.bs = b.bs[:0]
}

// Write implements io.Writer.
func (b *Buffer) Write(bs []byte) (int, error) {
	b.bs = append(b.bs, bs...)
	return len(bs), nil
}

// WriteByte writes a single byte to the Buffer.
//
// The returned error is always nil. The signature is the same as in
// bytes.Buffer and bufio.Writer.
func (b *Buffer) WriteByte(v byte) error {
	b.AppendByte(v)
	return nil
}

// WriteString writes a string to the Buffer.
//
// The returned error is always nil. The signature is the same as in
// bytes.Buffer and bufio.Writer.
func (b *Buffer) WriteString(s string) (int, error) {
	b.AppendString(s)
	return len(s), nil
}

// TrimNewline removes the last byte of the buffer if that byte is "\n".
func (b *Buffer) TrimNewline() {
	if i := len(b.bs) - 1; i >= 0 {
		if b.bs[i] == '\n' {
			b.bs = b.bs[:i]
		}
	}
}

// Free returns the Buffer to the pool of this package.
//
// The caller must not keep a reference to the Buffer after it calls Free.
func (b *Buffer) Free() {
	put(b)
}
