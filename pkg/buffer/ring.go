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

import "io"

type Ring struct {
	rb *coreRing
}

func (rb *Ring) instance() *coreRing {
	if rb.rb == nil {
		rb.rb = builtinPool.Get()
	}

	return rb.rb
}

// Done checks and returns the internal ring-buffer to pool.
func (rb *Ring) Done() {
	if rb.rb != nil {
		builtinPool.Put(rb.rb)
		rb.rb = nil
	}
}

func (rb *Ring) done() {
	if rb.rb != nil && rb.rb.IsEmpty() {
		builtinPool.Put(rb.rb)
		rb.rb = nil
	}
}

// Peek returns the next n bytes without advancing the read pointer,
// it returns all bytes when n <= 0.
func (rb *Ring) Peek(n int) (head []byte, tail []byte) {
	if rb.rb == nil {
		return nil, nil
	}
	return rb.rb.Peek(n)
}

// Discard skips the next n bytes by advancing the read pointer.
func (rb *Ring) Discard(n int) (int, error) {
	if rb.rb == nil {
		return 0, ErrIsEmpty
	}

	defer rb.done()
	return rb.rb.Discard(n)
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
func (rb *Ring) Read(p []byte) (int, error) {
	if rb.rb == nil {
		return 0, ErrIsEmpty
	}

	defer rb.done()
	return rb.rb.Read(p)
}

// ReadByte reads and returns the next byte from the input or ErrIsEmpty.
func (rb *Ring) ReadByte() (byte, error) {
	if rb.rb == nil {
		return 0, ErrIsEmpty
	}

	defer rb.done()
	return rb.rb.ReadByte()
}

// Write writes len(p) bytes from p to the underlying buf.
// It returns the number of bytes written from p (n == len(p) > 0) and any error encountered that caused the write to
// stop early.
// If the length of p is greater than the writable capacity of this ring-buffer, it will allocate more memory to
// this ring-buffer.
// Write must not modify the slice data, even temporarily.
func (rb *Ring) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return rb.instance().Write(p)
}

// WriteByte writes one byte into buffer.
func (rb *Ring) WriteByte(c byte) error {
	return rb.instance().WriteByte(c)
}

// Buffered returns the length of available bytes to read.
func (rb *Ring) Buffered() int {
	if rb.rb == nil {
		return 0
	}
	return rb.rb.Buffered()
}

// Len returns the length of the underlying buffer.
func (rb *Ring) Len() int {
	if rb.rb == nil {
		return 0
	}
	return rb.rb.Len()
}

// Cap returns the size of the underlying buffer.
func (rb *Ring) Cap() int {
	if rb.rb == nil {
		return 0
	}
	return rb.rb.Cap()
}

// Available returns the length of available bytes to write.
func (rb *Ring) Available() int {
	if rb.rb == nil {
		return 0
	}
	return rb.rb.Available()
}

// WriteString writes the contents of the string s to buffer, which accepts a slice of bytes.
func (rb *Ring) WriteString(s string) (int, error) {
	if len(s) == 0 {
		return 0, nil
	}
	return rb.instance().WriteString(s)
}

// Bytes returns all available read bytes. It does not move the read pointer and only copy the available data.
func (rb *Ring) Bytes() []byte {
	if rb.rb == nil {
		return nil
	}
	return rb.rb.Bytes()
}

// ReadFrom implements io.ReaderFrom.
func (rb *Ring) ReadFrom(r io.Reader) (int64, error) {
	return rb.instance().ReadFrom(r)
}

// WriteTo implements io.WriterTo.
func (rb *Ring) WriteTo(w io.Writer) (int64, error) {
	if rb.rb == nil {
		return 0, ErrIsEmpty
	}

	defer rb.done()
	return rb.instance().WriteTo(w)
}

// IsFull tells if this ring-buffer is full.
func (rb *Ring) IsFull() bool {
	if rb.rb == nil {
		return false
	}
	return rb.rb.IsFull()
}

// IsEmpty tells if this ring-buffer is empty.
func (rb *Ring) IsEmpty() bool {
	if rb.rb == nil {
		return true
	}
	return rb.rb.IsEmpty()
}

// Reset the read pointer and write pointer to zero.
func (rb *Ring) Reset() {
	if rb.rb == nil {
		return
	}
	rb.rb.Reset()
}
