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
	"io"
	"math"

	slicepool "xxx/pkg/pool/slice"
)

type node struct {
	buf  []byte
	next *node
}

func (b *node) len() int {
	return len(b.buf)
}

// LinkedList is a linked list of node.
type LinkedList struct {
	head  *node
	tail  *node
	size  int
	bytes int
}

// AllocNode allocates a []byte with the given length that is expected to
// be pushed into the Elastic.
func (ll *LinkedList) AllocNode(n int) []byte {
	return slicepool.Get(n)
}

// FreeNode puts the given []byte back to the pool to free the memory.
func (ll *LinkedList) FreeNode(p []byte) {
	slicepool.Put(p)
}

// Append is like PushBack but appends b without copying it.
func (ll *LinkedList) Append(p []byte) {
	n := len(p)
	if n <= 0 {
		return
	}
	ll.pushBack(&node{buf: p})
}

// Pop removes and returns the buffer of the head or nil if the list is empty.
func (ll *LinkedList) Pop() []byte {
	n := ll.pop()
	if n == nil {
		return nil
	}
	return n.buf
}

// PushFront is a wrapper of pushFront, which accepts []byte as its argument.
func (ll *LinkedList) PushFront(p []byte) {
	n := len(p)
	if n <= 0 {
		return
	}
	b := slicepool.Get(n)
	copy(b, p)
	ll.pushFront(&node{buf: b})
}

// PushBack is a wrapper of pushBack, which accepts []byte as its argument.
func (ll *LinkedList) PushBack(p []byte) {
	n := len(p)
	if n == 0 {
		return
	}
	b := slicepool.Get(n)
	copy(b, p)
	ll.pushBack(&node{buf: b})
}

// Peek assembles the up to maxBytes of [][]byte based on the list of node,
// it won't remove these nodes from l until Discard() is called.
func (ll *LinkedList) Peek(maxBytes int) ([][]byte, error) {
	if maxBytes == 0 || maxBytes == math.MaxInt32 {
		maxBytes = math.MaxInt32
	} else if maxBytes > ll.Buffered() {
		return nil, io.ErrShortBuffer
	}
	var bs [][]byte
	var cum int
	for iter := ll.head; iter != nil; iter = iter.next {
		offset := iter.len()
		if cum+offset > maxBytes {
			offset = maxBytes - cum
		}
		bs = append(bs, iter.buf[:offset])
		if cum += offset; cum == maxBytes {
			break
		}
	}
	return bs, nil
}

// PeekWithBytes is like Peek but accepts [][]byte and puts them onto head.
func (ll *LinkedList) PeekWithBytes(maxBytes int, bs ...[]byte) ([][]byte, error) {
	if maxBytes == 0 || maxBytes == math.MaxInt32 {
		maxBytes = math.MaxInt32
	} else {
		var prefixLen int
		for _, b := range bs {
			prefixLen += len(b)
		}

		if maxBytes > prefixLen+ll.Buffered() {
			return nil, io.ErrShortBuffer
		}
	}
	var bss [][]byte
	var cum int
	for _, b := range bs {
		if n := len(b); n > 0 {
			offset := n
			if cum+offset > maxBytes {
				offset = maxBytes - cum
			}
			bss = append(bss, b[:offset])
			if cum += offset; cum == maxBytes {
				return bss, nil
			}
		}
	}
	for iter := ll.head; iter != nil; iter = iter.next {
		offset := iter.len()
		if cum+offset > maxBytes {
			offset = maxBytes - cum
		}
		bss = append(bss, iter.buf[:offset])
		if cum += offset; cum == maxBytes {
			break
		}
	}
	return bss, nil
}

// Discard removes some nodes based on n bytes.
func (ll *LinkedList) Discard(n int) (discarded int, err error) {
	if n <= 0 {
		return
	}
	for n != 0 {
		b := ll.pop()
		if b == nil {
			break
		}
		if n < b.len() {
			b.buf = b.buf[n:]
			discarded += n
			ll.pushFront(b)
			break
		}
		n -= b.len()
		discarded += b.len()
		slicepool.Put(b.buf)
	}
	return
}

const minRead = 512

// ReadFrom implements io.ReaderFrom.
func (ll *LinkedList) ReadFrom(r io.Reader) (n int64, err error) {
	var m int
	for {
		b := slicepool.Get(minRead)
		m, err = r.Read(b)
		if m < 0 {
			panic("LinkedList.ReadFrom: reader returned negative count from Read")
		}
		// io.Reader is allowed to return (n>0, io.EOF) in the same call.
		n += int64(m)
		b = b[:m]
		if m > 0 {
			ll.pushBack(&node{buf: b})
		} else {
			slicepool.Put(b)
		}
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
	}
}

// Read reads data from the LinkedList.
func (ll *LinkedList) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	for b := ll.pop(); b != nil; b = ll.pop() {
		m := copy(p[n:], b.buf)
		n += m
		if m < b.len() {
			b.buf = b.buf[m:]
			ll.pushFront(b)
		} else {
			slicepool.Put(b.buf)
		}
		if n == len(p) {
			return
		}
	}
	if n == 0 {
		err = io.EOF
	}
	return
}

// WriteTo implements io.WriterTo.
func (ll *LinkedList) WriteTo(w io.Writer) (n int64, err error) {
	var m int
	for b := ll.pop(); b != nil; b = ll.pop() {
		m, err = w.Write(b.buf)
		if m > b.len() {
			panic("LinkedList.WriteTo: invalid Write count")
		}
		// io.Writer is allowed to return n < len(p) with err != nil in the same call.
		n += int64(m)
		if m < b.len() {
			b.buf = b.buf[m:]
			ll.pushFront(b)
			if err != nil {
				return n, err
			}
			return n, io.ErrShortWrite
		}
		slicepool.Put(b.buf)
		if err != nil {
			return
		}
	}
	return
}

// Len returns the length of the list.
func (ll *LinkedList) Len() int {
	return ll.size
}

// Buffered returns the number of bytes that can be read from the current buffer.
func (ll *LinkedList) Buffered() int {
	return ll.bytes
}

// IsEmpty reports whether l is empty.
func (ll *LinkedList) IsEmpty() bool {
	return ll.head == nil
}

// Reset removes all elements from this list.
func (ll *LinkedList) Reset() {
	for b := ll.pop(); b != nil; b = ll.pop() {
		slicepool.Put(b.buf)
	}
	ll.head = nil
	ll.tail = nil
	ll.size = 0
	ll.bytes = 0
}

// pop returns and removes the head of l. If l is empty, it returns nil.
func (ll *LinkedList) pop() *node {
	if ll.head == nil {
		return nil
	}
	b := ll.head
	ll.head = b.next
	if ll.head == nil {
		ll.tail = nil
	}
	b.next = nil
	ll.size--
	ll.bytes -= b.len()
	return b
}

// pushFront adds the new node to the head of l.
func (ll *LinkedList) pushFront(b *node) {
	if b == nil {
		return
	}
	if ll.head == nil {
		b.next = nil
		ll.tail = b
	} else {
		b.next = ll.head
	}
	ll.head = b
	ll.size++
	ll.bytes += b.len()
}

// pushBack adds a new node to the tail of l.
func (ll *LinkedList) pushBack(b *node) {
	if b == nil {
		return
	}
	if ll.tail == nil {
		ll.head = b
	} else {
		ll.tail.next = b
	}
	b.next = nil
	ll.tail = b
	ll.size++
	ll.bytes += b.len()
}
