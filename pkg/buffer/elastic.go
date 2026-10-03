/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
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

package buffer

import (
	"errors"
	"io"
	"math"
)

var ErrNegativeSize = errors.New("nio: negative size is not allowed")

// Elastic combines ring-buffer and list-buffer.
// coreRing-buffer is the top-priority buffer to store response data, it will only switch to
// LinkedList-buffer if the data size of ring-buffer reaches the maximum(MaxStackingBytes), list-buffer is more
// flexible and scalable, which helps the application reduce memory footprint.
type Elastic struct {
	maxStaticBytes int
	ring           Ring
	list           LinkedList
}

// New instantiates an elastic.Buffer and returns it.
func New(maxStaticBytes int) (*Elastic, error) {
	if maxStaticBytes <= 0 {
		return nil, ErrNegativeSize
	}
	return &Elastic{maxStaticBytes: maxStaticBytes}, nil
}

// Peek returns n bytes as [][]byte, these bytes won't be discarded until Buffer.Discard() is called.
func (e *Elastic) Peek(n int) ([][]byte, error) {
	if n <= 0 || n == math.MaxInt32 {
		n = math.MaxInt32
	} else if n > e.Buffered() {
		return nil, io.ErrShortBuffer
	}
	head, tail := e.ring.Peek(n)
	if e.ring.Buffered() >= n {
		return [][]byte{head, tail}, nil
	}
	return e.list.PeekWithBytes(n, head, tail)
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

// Read reads data from the Buffer.
func (e *Elastic) Read(p []byte) (n int, err error) {
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
	return e.ring.ReadFrom(r)
}

// Write appends data to this buffer.
func (e *Elastic) Write(p []byte) (n int, err error) {
	if !e.list.IsEmpty() || e.ring.Buffered() >= e.maxStaticBytes {
		e.list.PushBack(p)
		return len(p), nil
	}
	if e.ring.Len() >= e.maxStaticBytes {
		writable := e.ring.Available()
		if n = len(p); n > writable {
			_, _ = e.ring.Write(p[:writable])
			e.list.PushBack(p[writable:])
			return
		}
	}
	return e.ring.Write(p)
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

	writable := e.ring.Available()
	if e.ring.Len() < e.maxStaticBytes {
		writable = e.maxStaticBytes - e.ring.Buffered()
	}
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
	if n, err = e.ring.WriteTo(w); err != nil {
		return
	}
	var m int64
	m, err = e.list.WriteTo(w)
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
