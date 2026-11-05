/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package buffer

import (
	"errors"
	"io"
)

// ErrElasticInvalidSize tells that the size for New is 0 or less. New accepts only a positive size.
var ErrElasticInvalidSize = errors.New("elastic-buffer: size must be positive")

// Elastic combines a Ring and a LinkedList.
// Elastic writes data to the Ring first. When the Ring contains maxStaticBytes, Elastic writes new data to the
// LinkedList. Elastic writes to the Ring again only after the LinkedList is empty. Thus, the data stays in order.
// The LinkedList gets memory only for the data that it keeps. This decreases the memory that the application uses.
type Elastic struct {
	maxStaticBytes int
	ring           Ring
	list           LinkedList
}

// New instantiates an Elastic and returns it.
func New(maxStaticBytes int) (*Elastic, error) {
	if maxStaticBytes <= 0 {
		return nil, ErrElasticInvalidSize
	}
	return &Elastic{maxStaticBytes: maxStaticBytes}, nil
}

// Peek returns n bytes as [][]byte. Peek does not remove these bytes. Discard removes them.
//
// The slices refer to the internal buffers. Peek does not copy the bytes.
// The caller must not use the slices after a call to Discard, Read, WriteTo, Write, Writev, Reset, or Release.
// When Discard empties the ring, the ring goes back to the pool, and other users can change the bytes.
func (e *Elastic) Peek(n int) ([][]byte, error) {
	if n <= 0 {
		n = e.Buffered()
	} else if n > e.Buffered() {
		return nil, io.ErrShortBuffer
	}

	head, tail := e.ring.Peek(n)
	if e.ring.Buffered() < n {
		return e.list.PeekWithBytes(n, head, tail)
	}

	// The write path calls Peek for each write. Thus, Peek makes this slice only when the list is not necessary.
	bs := make([][]byte, 0, 2)
	if len(head) > 0 {
		bs = append(bs, head)
	}
	if len(tail) > 0 {
		bs = append(bs, tail)
	}
	return bs, nil
}

// Discard discards n bytes in this buffer.
func (e *Elastic) Discard(n int) (discarded int, err error) {
	discarded, err = e.ring.Discard(n)
	if n <= discarded {
		return
	}

	n -= discarded
	var m int
	m, err = e.list.Discard(n)
	discarded += m
	return
}

// Read reads data from the Elastic.
func (e *Elastic) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	n, err = e.ring.Read(p)
	if n == len(p) {
		return n, err
	}
	var m int
	m, err = e.list.Read(p[n:])
	n += m
	return
}

// ReadFrom implements io.ReaderFrom.
func (e *Elastic) ReadFrom(r io.Reader) (int64, error) {
	if !e.list.IsEmpty() || e.ring.Buffered() >= e.maxStaticBytes {
		return e.list.ReadFrom(r)
	}

	n, err := e.ring.readFrom(r, int64(e.ringWritable()))
	if err == io.EOF {
		return n, nil
	}
	if err != nil {
		return n, err
	}

	// The ring is at its limit, and r can have more data. The list keeps the remaining data.
	m, err := e.list.ReadFrom(r)
	return n + m, err
}

// Write appends data to this buffer.
func (e *Elastic) Write(p []byte) (int, error) {
	if !e.list.IsEmpty() || e.ring.Buffered() >= e.maxStaticBytes {
		e.list.PushBack(p)
		return len(p), nil
	}
	w := e.ringWritable()
	if len(p) <= w {
		return e.ring.Write(p)
	}
	_, _ = e.ring.Write(p[:w])
	e.list.PushBack(p[w:])
	return len(p), nil
}

// Writev appends multiple byte slices to this buffer.
func (e *Elastic) Writev(bs [][]byte) (int, error) {
	if !e.list.IsEmpty() || e.ring.Buffered() >= e.maxStaticBytes {
		var n int
		for _, b := range bs {
			e.list.PushBack(b)
			n += len(b)
		}
		return n, nil
	}

	writable := e.ringWritable()
	var pos, cum int
	for i, b := range bs {
		pos = i
		cum += len(b)
		if len(b) > writable {
			_, _ = e.ring.Write(b[:writable])
			e.list.PushBack(b[writable:])
			break
		}
		n, _ := e.ring.Write(b)
		writable -= n
	}
	for pos++; pos < len(bs); pos++ {
		cum += len(bs[pos])
		e.list.PushBack(bs[pos])
	}
	return cum, nil
}

// WriteTo implements io.WriterTo.
func (e *Elastic) WriteTo(w io.Writer) (n int64, err error) {
	if !e.ring.IsEmpty() {
		n, err = e.ring.WriteTo(w)
		if err != nil {
			return
		}
	}

	m, err := e.list.WriteTo(w)
	n += m
	return
}

// Buffered returns the number of bytes that can be read from the current buffer.
func (e *Elastic) Buffered() int {
	return e.ring.Buffered() + e.list.Buffered()
}

// IsEmpty indicates whether this buffer is empty.
func (e *Elastic) IsEmpty() bool {
	return e.ring.IsEmpty() && e.list.IsEmpty()
}

// Reset resets the buffer.
func (e *Elastic) Reset(maxStaticBytes int) {
	e.ring.Reset()
	e.list.Reset()
	if maxStaticBytes > 0 {
		e.maxStaticBytes = maxStaticBytes
	}
}

// Release frees all resource of this buffer.
func (e *Elastic) Release() {
	e.ring.Done()
	e.list.Reset()
}

// ringWritable returns the number of bytes that the ring can accept now.
// Write, Writev, and ReadFrom use this one limit. Thus, they divide data between the ring and the list in the same way.
//
// When the capacity of the ring is less than maxStaticBytes, the ring can grow.
// The request to grow is not more than maxStaticBytes.
// When the capacity is equal to or more than maxStaticBytes, the ring does not grow.
// Then the ring uses only its free space.
//
// maxStaticBytes limits the growth of the ring. It does not limit the capacity of the ring.
// The capacity can be more than maxStaticBytes for these reasons:
//   - grow rounds the capacity up. Usually, it doubles the capacity. The ring uses all of the capacity that
//     slicepool.Get gives, and this capacity is usually a power of two.
//   - The first allocation is RingDefaultBufferSize or the next power of two.
//   - The pool can give a ring that is already larger than maxStaticBytes.
//
// When the ring grows, its capacity is less than two times maxStaticBytes or equal to RingDefaultBufferSize.
func (e *Elastic) ringWritable() int {
	if e.ring.Len() >= e.maxStaticBytes {
		return e.ring.Available()
	}
	return max(e.maxStaticBytes-e.ring.Buffered(), 0)
}
