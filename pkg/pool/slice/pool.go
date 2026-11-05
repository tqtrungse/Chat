/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package slice

import (
	"math"
	"math/bits"
	"sync"
	"unsafe"
)

var builtinPool pool

// Get returns a byte slice that has the length size.
// It uses memory from the pool when this is available.
// Get does not clear the bytes. The bytes can contain data from an earlier use.
// The caller must set each byte before the caller reads it.
//
// Get(0) returns nil. A size of zero can come from normal size calculations,
// so it is not an error.
// A negative size is an error. Get then causes a panic.
func Get(size int) []byte {
	return builtinPool.Get(size)
}

// Put gives buf back to the pool if the capacity of buf is correct for the pool.
// Put does not clear or change buf.
// If buf contains sensitive data, the caller must clear buf before it calls Put.
// The pool takes ownership of buf.
// The caller must not access buf after it calls Put.
func Put(buf []byte) {
	builtinPool.Put(buf)
}

// PutZeroed clears buf up to its capacity. Then it gives buf back to the pool.
// Use PutZeroed for buffers that contain keys, tokens, or other secret data.
// Then the next caller of Get cannot see this data.
// The ownership rule is the same as for Put.
// The caller must not access buf after it calls PutZeroed.
func PutZeroed(buf []byte) {
	clear(buf[:cap(buf)])
	Put(buf)
}

// maxPooledSize is the largest capacity that the buckets cover.
// For a larger size, index() gives bucket 31. The pool has no slot for bucket 31.
// Therefore, Get must allocate such a size directly. Get must not use the index.
const maxPooledSize = 1 << 30

// pool has 31 sync.Pool objects.
// They hold byte slices that have a capacity of 2^0 to 2^30, in powers of 2.
type pool struct {
	pools [31]sync.Pool
}

func (p *pool) Get(size int) []byte {
	if size < 0 {
		panic("slice: Get with negative size")
	}

	if size == 0 {
		return nil
	}

	if size > maxPooledSize {
		return make([]byte, size)
	}

	idx := index(uint32(size))
	ptr, _ := p.pools[idx].Get().(*byte)
	if ptr == nil {
		return make([]byte, size, 1<<uint64(idx))
	}
	return unsafe.Slice(ptr, 1<<idx)[:size]
}

func (p *pool) Put(buf []byte) {
	size := cap(buf)
	if size == 0 || size > math.MaxInt32 {
		return
	}

	idx := index(uint32(size))
	// This byte slice is not from Pool.Get(). Put it in the next lower bucket.
	if size != 1<<uint64(idx) {
		idx--
	}
	// A capacity above maxPooledSize gives bucket 31, and the pool has no slot
	// for bucket 31. The decrement above moves each such capacity to bucket 30.
	// Only a capacity of exactly 1<<31 skips the decrement, and this value is
	// larger than MaxInt32.
	// Store the pointer to the underlying array, not the pointer to the slice.
	// This stops buf from moving from the stack to the heap.
	p.pools[idx].Put(unsafe.SliceData(buf))
}

func index(n uint32) uint32 {
	return uint32(bits.Len32(n - 1))
}
