/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package slice

import (
	"math"
	"math/bits"
	"sync"
	"unsafe"
)

var builtinPool pool

// Get returns a byte slice with length size, using pooled storage when available.
// The returned bytes are not cleared and may contain data from a previous use.
// The caller must initialize every byte it reads.
func Get(size int) []byte {
	return builtinPool.Get(size)
}

// Put returns buf to the pool when its capacity is eligible.
// It does not clear or otherwise modify buf. If buf contains sensitive data,
// callers must clear it before calling Put. Ownership of buf is transferred;
// callers must not access it after Put.
func Put(buf []byte) {
	builtinPool.Put(buf)
}

// pool consists of 32 sync.Pool, representing byte slices of length from 0 to 32 in powers of 2.
type pool struct {
	pools [32]sync.Pool
}

func (p *pool) Get(size int) []byte {
	if size <= 0 {
		return nil
	}

	if size > math.MaxInt32 {
		return make([]byte, size)
	}

	idx := index(uint32(size))
	ptr, _ := p.pools[idx].Get().(*byte)
	if ptr == nil {
		return make([]byte, size, 1<<idx)
	}

	return unsafe.Slice(ptr, 1<<idx)[:size]
}

func (p *pool) Put(buf []byte) {
	size := cap(buf)
	if size == 0 || size > math.MaxInt32 {
		return
	}

	idx := index(uint32(size))
	// this byte slice is not from Pool.Get(), put it into the previous interval of idx.
	if size != 1<<idx {
		idx--
	}
	// Store the pointer to the underlying array instead of the pointer to the slice itself,
	// which circumvents the escape of buf from the stack to the heap.
	p.pools[idx].Put(unsafe.SliceData(buf))
}

func index(n uint32) uint32 {
	return uint32(bits.Len32(n - 1))
}
