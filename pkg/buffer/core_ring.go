/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package buffer

import (
	"io"

	"xxx/pkg/bit"
	"xxx/pkg/bs"
	slicepool "xxx/pkg/pool/slice"
)

const (
	// RingMinRead is the minimum slice size that Ring.ReadFrom and LinkedList.ReadFrom give to a Read call.
	// Ring.ReadFrom does not grow the ring while the ring has RingMinRead free bytes or more.
	RingMinRead = 512
	// RingDefaultBufferSize is the first-time allocation on a ring-buffer.
	RingDefaultBufferSize = 1024     // 1KB
	bufferGrowThreshold   = 4 * 1024 // 4KB

	// noReadLimit tells readFrom to read until io.EOF or an error.
	noReadLimit = -1

	// maxConsecutiveEmptyReads is the number of Read calls in sequence that can return 0, nil.
	// After this number, ReadFrom returns io.ErrNoProgress. Without this limit, a bad io.Reader can make
	// ReadFrom loop forever. bufio uses the same limit.
	maxConsecutiveEmptyReads = 100
)

// coreRing is a circular buffer. It implements io.Reader, io.Writer, io.ReaderFrom, and io.WriterTo.
type coreRing struct {
	buf     []byte
	size    int
	r       int // next position to read
	w       int // next position to write
	isEmpty bool
}

// newCoreRing returns a new coreRing whose buffer has the given size.
func newCoreRing(size int) *coreRing {
	if size == 0 {
		return &coreRing{isEmpty: true}
	}
	size = int(bit.NextPowerOfTwo(size))
	return &coreRing{
		buf:     slicepool.Get(size),
		size:    size,
		isEmpty: true,
	}
}

// Buffered returns the length of available bytes to read.
func (rb *coreRing) Buffered() int {
	if rb.r == rb.w {
		if rb.isEmpty {
			return 0
		}
		return rb.size
	}
	if rb.w > rb.r {
		return rb.w - rb.r
	}
	return rb.size - rb.r + rb.w
}

// Len returns the length of the underlying buffer.
func (rb *coreRing) Len() int {
	return len(rb.buf)
}

// Cap returns the size of the underlying buffer.
func (rb *coreRing) Cap() int {
	return rb.size
}

// Available returns the length of available bytes to write.
func (rb *coreRing) Available() int {
	if rb.r == rb.w {
		if rb.isEmpty {
			return rb.size
		}
		return 0
	}
	if rb.w < rb.r {
		return rb.r - rb.w
	}
	return rb.size - rb.w + rb.r
}

// IsFull tells if this ring-buffer is full.
func (rb *coreRing) IsFull() bool {
	return rb.r == rb.w && !rb.isEmpty
}

// IsEmpty tells if this ring-buffer is empty.
func (rb *coreRing) IsEmpty() bool {
	return rb.isEmpty
}

// Reset the read pointer and write pointer to zero.
func (rb *coreRing) Reset() {
	rb.isEmpty = true
	rb.r = 0
	rb.w = 0
}

// Peek returns the next n bytes without advancing the read pointer,
// it returns all bytes when n <= 0.
func (rb *coreRing) Peek(n int) (head []byte, tail []byte) {
	if rb.isEmpty {
		return
	}
	if n <= 0 {
		return rb.peekAll()
	}
	if rb.w > rb.r {
		// length of ring-buffer
		m := min(rb.w-rb.r, n)
		head = rb.buf[rb.r : rb.r+m]
		return
	}

	// length of ring-buffer
	m := min(rb.size-rb.r+rb.w, n)
	if rb.r+m <= rb.size {
		head = rb.buf[rb.r : rb.r+m]
	} else {
		c1 := rb.size - rb.r
		head = rb.buf[rb.r:]
		c2 := m - c1
		tail = rb.buf[:c2]
	}
	return
}

// Discard skips the next n bytes by advancing the read pointer.
func (rb *coreRing) Discard(n int) (discarded int, err error) {
	if n <= 0 {
		return 0, nil
	}

	discarded = rb.Buffered()
	if n < discarded {
		rb.r += n
		if rb.r >= rb.size {
			rb.r -= rb.size
		}
		return n, nil
	}
	rb.Reset()
	return
}

// Read reads up to len(p) bytes into p. It returns the number of bytes read (0 <= n <= len(p)) and any error
// encountered.
// Even if Read returns n < len(p), it may use all of p as scratch space during the call.
// If some data is available but not len(p) bytes, Read conventionally returns what is available instead of waiting
// for more.
// When Read encounters an error or end-of-file condition after successfully reading n > 0 bytes,
// it returns the number of bytes read. It may return the (non-nil) error from the same call or return the
// error (and n == 0) from a subsequent call.
// Callers should always process the n > 0 bytes returned before considering the error err.
// Doing so correctly handles I/O errors that happen after reading some bytes and also both of the allowed EOF
// behaviors.
func (rb *coreRing) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	if rb.isEmpty {
		return 0, io.EOF
	}

	l := len(p)
	if rb.w > rb.r {
		n = min(rb.w-rb.r, l)
		copy(p, rb.buf[rb.r:rb.r+n])
		rb.r += n
		if rb.r == rb.w {
			rb.Reset()
		}
		return
	}

	n = min(rb.size-rb.r+rb.w, l)
	if rb.r+n <= rb.size {
		copy(p, rb.buf[rb.r:rb.r+n])
	} else {
		c1 := rb.size - rb.r
		copy(p, rb.buf[rb.r:])
		c2 := n - c1
		copy(p[c1:], rb.buf[:c2])
	}

	rb.r += n
	if rb.r >= rb.size {
		rb.r -= rb.size
	}
	if rb.r == rb.w {
		rb.Reset()
	}
	return
}

// ReadByte reads and returns the next byte from the input or io.EOF.
func (rb *coreRing) ReadByte() (b byte, err error) {
	if rb.isEmpty {
		return 0, io.EOF
	}

	b = rb.buf[rb.r]
	rb.r++
	if rb.r == rb.size {
		rb.r = 0
	}
	if rb.r == rb.w {
		rb.Reset()
	}
	return
}

// Bytes returns all available read bytes. It does not move the read pointer and only copy the available data.
func (rb *coreRing) Bytes() []byte {
	if rb.isEmpty {
		return nil
	}
	if rb.w == rb.r {
		var bb []byte
		bb = append(bb, rb.buf[rb.r:]...)
		bb = append(bb, rb.buf[:rb.w]...)
		return bb
	}

	var bb []byte
	if rb.w > rb.r {
		bb = append(bb, rb.buf[rb.r:rb.w]...)
		return bb
	}

	bb = append(bb, rb.buf[rb.r:]...)
	if rb.w != 0 {
		bb = append(bb, rb.buf[:rb.w]...)
	}
	return bb
}

// ReadFrom implements io.ReaderFrom.
func (rb *coreRing) ReadFrom(r io.Reader) (int64, error) {
	n, err := rb.readFrom(r, noReadLimit)
	if err == io.EOF {
		return n, nil
	}
	return n, err
}

// readFrom reads from r until io.EOF, an error, or until it reads limit bytes.
// A negative limit, for example noReadLimit, sets no limit.
// readFrom returns io.EOF unchanged. Thus, the caller can know if r has no more data.
// When readFrom reads limit bytes, it returns a nil error and does not call r.Read again.
// readFrom does not ask grow for more than the bytes that are necessary for limit.
// readFrom returns io.ErrNoProgress after maxConsecutiveEmptyReads calls in sequence that return 0, nil.
func (rb *coreRing) readFrom(r io.Reader, limit int64) (n int64, err error) {
	var empty int
	for limit < 0 || n < limit {
		want := RingMinRead
		if limit >= 0 {
			want = int(min(int64(RingMinRead), limit-n))
		}
		if rb.Available() < want {
			rb.grow(rb.Buffered() + want)
		}

		var p []byte
		if rb.w >= rb.r {
			p = rb.buf[rb.w:]
		} else {
			p = rb.buf[rb.w:rb.r]
		}
		if limit >= 0 && int64(len(p)) > limit-n {
			p = p[:limit-n]
		}

		var m int
		m, err = r.Read(p)
		if m < 0 || m > len(p) {
			panic("buffer: Ring.ReadFrom: invalid Read count")
		}
		if m > 0 {
			rb.isEmpty = false
			rb.w += m
			if rb.w >= rb.size {
				rb.w -= rb.size
			}
			n += int64(m)
		}
		if err != nil {
			return
		}
		if m > 0 {
			empty = 0
		} else if empty++; empty >= maxConsecutiveEmptyReads {
			return n, io.ErrNoProgress
		}
	}
	return n, nil
}

// Write writes len(p) bytes from p to the underlying buf.
// It returns the number of bytes written from p (n == len(p) > 0) and any error encountered that caused the write to
// stop early.
// If the length of p is greater than the writable capacity of this ring-buffer, it will allocate more memory to
// this ring-buffer.
// Write must not modify the slice data, even temporarily.
func (rb *coreRing) Write(p []byte) (n int, err error) {
	n = len(p)
	if n == 0 {
		return
	}

	free := rb.Available()
	if n > free {
		rb.grow(rb.size + n - free)
	}

	if rb.w >= rb.r {
		c1 := rb.size - rb.w
		if c1 >= n {
			copy(rb.buf[rb.w:], p)
			rb.w += n
		} else {
			copy(rb.buf[rb.w:], p[:c1])
			c2 := n - c1
			copy(rb.buf, p[c1:])
			rb.w = c2
		}
	} else {
		copy(rb.buf[rb.w:], p)
		rb.w += n
	}

	if rb.w == rb.size {
		rb.w = 0
	}
	rb.isEmpty = false
	return
}

// WriteByte writes one byte into buffer.
func (rb *coreRing) WriteByte(c byte) error {
	if rb.Available() < 1 {
		rb.grow(rb.Buffered() + 1)
	}

	rb.buf[rb.w] = c
	rb.w++

	if rb.w == rb.size {
		rb.w = 0
	}
	rb.isEmpty = false

	return nil
}

// WriteString writes the contents of the string s to buffer, which accepts a slice of bytes.
func (rb *coreRing) WriteString(s string) (int, error) {
	return rb.Write(bs.StringToBytes(s))
}

// WriteTo implements io.WriterTo.
func (rb *coreRing) WriteTo(w io.Writer) (int64, error) {
	if rb.isEmpty {
		return 0, nil
	}

	if rb.w > rb.r {
		n := rb.w - rb.r
		m, err := w.Write(rb.buf[rb.r : rb.r+n])
		if m < 0 || m > n {
			panic("buffer: Ring.WriteTo: invalid Write count")
		}
		rb.r += m
		if rb.r == rb.w {
			rb.Reset()
		}
		if err != nil {
			return int64(m), err
		}
		if !rb.isEmpty {
			return int64(m), io.ErrShortWrite
		}
		return int64(m), nil
	}

	n := rb.size - rb.r + rb.w
	if rb.r+n <= rb.size {
		m, err := w.Write(rb.buf[rb.r : rb.r+n])
		if m < 0 || m > n {
			panic("buffer: Ring.WriteTo: invalid Write count")
		}
		rb.r += m
		if rb.r >= rb.size {
			rb.r -= rb.size
		}
		if rb.r == rb.w {
			rb.Reset()
		}
		if err != nil {
			return int64(m), err
		}
		if !rb.isEmpty {
			return int64(m), io.ErrShortWrite
		}
		return int64(m), nil
	}

	c1 := rb.size - rb.r
	m, err := w.Write(rb.buf[rb.r:])
	if m < 0 || m > c1 {
		panic("buffer: Ring.WriteTo: invalid Write count")
	}
	rb.r += m
	if rb.r >= rb.size {
		rb.r -= rb.size
	}
	if err != nil {
		return int64(m), err
	}
	if m < c1 {
		return int64(m), io.ErrShortWrite
	}
	cum := int64(m)
	c2 := n - c1
	m, err = w.Write(rb.buf[:c2])
	if m < 0 || m > c2 {
		panic("buffer: Ring.WriteTo: invalid Write count")
	}
	rb.r = m
	cum += int64(m)
	if rb.r == rb.w {
		rb.Reset()
	}
	if err != nil {
		return cum, err
	}
	if !rb.isEmpty {
		return cum, io.ErrShortWrite
	}
	return cum, nil
}

// peekAll returns all bytes without advancing the read pointer.
func (rb *coreRing) peekAll() (head []byte, tail []byte) {
	if rb.isEmpty {
		return
	}
	if rb.w > rb.r {
		head = rb.buf[rb.r:rb.w]
		return
	}
	head = rb.buf[rb.r:]
	if rb.w != 0 {
		tail = rb.buf[:rb.w]
	}
	return
}

func (rb *coreRing) grow(newCap int) {
	rbSize := rb.size
	if rbSize == 0 {
		if newCap <= RingDefaultBufferSize {
			newCap = RingDefaultBufferSize
		} else {
			newCap = int(bit.NextPowerOfTwo(newCap))
		}
	} else {
		doubleCap := rbSize + rbSize
		if newCap <= doubleCap {
			if rbSize < bufferGrowThreshold {
				newCap = doubleCap
			} else {
				// Check 0 < n to detect overflow and prevent an infinite loop.
				for 0 < rbSize && rbSize < newCap {
					rbSize += rbSize / 4
				}
				// The n calculation doesn't overflow, set n to newCap.
				// The loop can go above doubleCap. slicepool.Get then rounds the capacity up to
				// four times rb.size. The limit doubleCap prevents this waste.
				if rbSize > 0 {
					newCap = min(rbSize, doubleCap)
				}
			}
		}
	}

	// slicepool.Get rounds the capacity up, usually to a power of two.
	// The ring uses all of this capacity. Otherwise, the bytes above newCap are not used,
	// and the next grow allocates a new buffer that is not larger.
	newBuf := slicepool.Get(newCap)
	newBuf = newBuf[:cap(newBuf)]
	oldLen := rb.Buffered()
	_, _ = rb.Read(newBuf)
	slicepool.Put(rb.buf)

	rb.buf = newBuf
	rb.r = 0
	rb.w = oldLen
	rb.size = len(newBuf)
	if rb.w > 0 {
		rb.isEmpty = false
	}
}
