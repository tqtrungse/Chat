/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package buffer

import (
	"io"

	slicepool "xxx/pkg/pool/slice"
)

// listMaxRead is the maximum slice size that LinkedList.ReadFrom gives to one Read call.
const listMaxRead = 64 * 1024 // 64KB

// node keeps buf unchanged and moves off when the list consumes a part of the data.
// Thus, slicepool.Put always gets the slice that slicepool.Get gave. A slice that starts after
// the start of its array makes slicepool put it in a smaller bucket, and the array stays in memory.
type node struct {
	buf  []byte
	off  int // number of bytes at the start of buf that the list consumed
	next *node
}

// data returns the bytes that the list did not consume.
func (b *node) data() []byte {
	return b.buf[b.off:]
}

func (b *node) len() int {
	return len(b.buf) - b.off
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

// Append adds p to the tail of the list.
//
// If clone is true, Append gets a new buffer from slicepool and copies p into it.
// The caller keeps the ownership of p.
//
// If clone is false, Append adds p without a copy, and the list takes the ownership of p.
// After the list consumes p, it gives p back to slicepool.
// Thus, p must come from AllocNode, and the caller must not access p after Append.
func (ll *LinkedList) Append(p []byte, clone bool) {
	if len(p) == 0 {
		return
	}
	if clone {
		ll.PushBack(p)
		return
	}
	ll.pushBack(&node{buf: p})
}

// Pop removes and returns the buffer of the head or nil if the list is empty.
// Pop gives the ownership of the bytes to the caller. The list does not give them back to slicepool.
// The bytes can start after the start of the slice from slicepool, when the list consumed a part of them.
func (ll *LinkedList) Pop() []byte {
	n := ll.pop()
	if n == nil {
		return nil
	}
	return n.data()
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
//
// If maxBytes is less than or equal to 0, Peek returns all bytes.
// If maxBytes is more than Buffered, Peek returns io.ErrShortBuffer.
func (ll *LinkedList) Peek(maxBytes int) ([][]byte, error) {
	if maxBytes <= 0 {
		maxBytes = ll.Buffered()
	} else if maxBytes > ll.Buffered() {
		return nil, io.ErrShortBuffer
	}
	if maxBytes == 0 {
		return nil, nil
	}

	var bs [][]byte
	var cum int
	for iter := ll.head; iter != nil; iter = iter.next {
		offset := iter.len()
		if cum+offset > maxBytes {
			offset = maxBytes - cum
		}
		bs = append(bs, iter.data()[:offset])
		if cum += offset; cum == maxBytes {
			break
		}
	}
	return bs, nil
}

// PeekWithBytes is like Peek but accepts [][]byte and puts them onto head.
// The limit maxBytes applies to bs and to the list together.
func (ll *LinkedList) PeekWithBytes(maxBytes int, bs ...[]byte) ([][]byte, error) {
	total := ll.Buffered()
	for _, b := range bs {
		total += len(b)
	}
	if maxBytes <= 0 {
		maxBytes = total
	} else if maxBytes > total {
		return nil, io.ErrShortBuffer
	}
	if maxBytes == 0 {
		return nil, nil
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
		bss = append(bss, iter.data()[:offset])
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
			b.off += n
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

// ReadFrom implements io.ReaderFrom.
//
// ReadFrom returns io.ErrNoProgress after maxConsecutiveEmptyReads calls in sequence that return 0, nil.
//
// The first Read uses RingMinRead bytes. When a Read fills the buffer, the next Read uses two times more bytes,
// to a maximum of listMaxRead. Thus, a large source gives few large nodes.
func (ll *LinkedList) ReadFrom(r io.Reader) (n int64, err error) {
	size := RingMinRead
	var empty int
	for {
		b := slicepool.Get(size)
		var m int
		m, err = r.Read(b)
		if m < 0 || m > len(b) {
			panic("buffer: LinkedList.ReadFrom: invalid Read count")
		}
		// io.Reader is allowed to return (n>0, io.EOF) in the same call.
		n += int64(m)
		switch {
		case m == 0:
			slicepool.Put(b)
		case m < len(b)/4:
			// A node keeps all of the capacity of its buffer. Thus, a small Read into a large buffer
			// moves to a buffer that has the correct size.
			small := slicepool.Get(m)
			copy(small, b[:m])
			slicepool.Put(b)
			ll.pushBack(&node{buf: small})
		default:
			ll.pushBack(&node{buf: b[:m]})
		}
		if m == len(b) && size < listMaxRead {
			size *= 2
		}
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		if m > 0 {
			empty = 0
		} else if empty++; empty >= maxConsecutiveEmptyReads {
			return n, io.ErrNoProgress
		}
	}
}

// Read reads data from the LinkedList.
func (ll *LinkedList) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	for b := ll.pop(); b != nil; b = ll.pop() {
		m := copy(p[n:], b.data())
		n += m
		if m < b.len() {
			b.off += m
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
		m, err = w.Write(b.data())
		if m < 0 || m > b.len() {
			panic("buffer: LinkedList.WriteTo: invalid Write count")
		}
		// io.Writer is allowed to return n < len(p) with err != nil in the same call.
		n += int64(m)
		if m < b.len() {
			b.off += m
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
