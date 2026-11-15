/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package mpsc

import (
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"

	"xxx/pkg/cpu"
)

const (
	closedFlag   = uint64(1)                 // Bit 0: closed flag
	pusherShift  = uint64(1)                 // Bit 1 -> 31: count the number of pushers
	pusherOne    = uint64(1) << pusherShift  // Value of a pusher
	sendIdxShift = uint64(32)                // Bit 32 -> 63: Index
	sendIdxOne   = uint64(1) << sendIdxShift // Value of an index
	pushAdd      = sendIdxOne | pusherOne    // pushAdd combine the two operations of incrementing sendIdx and activePushers into a single addition
)

type entry[V any] struct {
	Value V
	Cycle atomic.Uint32
}

type segment[V any] struct {
	b *bounded[V]
	_ [cpu.CacheLineSize - bits.UintSize/8]byte

	mtx sync.Mutex
	_   [cpu.CacheLineSize - unsafe.Sizeof(sync.Mutex{})]byte

	next     atomic.Pointer[segment[V]]
	initNext atomic.Bool
	_        [cpu.CacheLineSize - bits.UintSize/8 - 4]byte
}

// Elastic is MPSC unbounded queue assembled from a linked chain of fixed-size ring buffers.
type Elastic[V any] struct {
	head *segment[V]
	_    [cpu.CacheLineSize - bits.UintSize/8]byte

	tail   atomic.Pointer[segment[V]]
	closed atomic.Bool
	_      [cpu.CacheLineSize - bits.UintSize/8 - 4]byte
}

func NewElastic[V any](segmentCap int) (*Elastic[V], error) {
	if segmentCap <= 0 {
		return nil, fmt.Errorf("invalid segment capacity: %d", segmentCap)
	}
	node := &segment[V]{
		b:        newBounded[V](segmentCap),
		mtx:      sync.Mutex{},
		initNext: atomic.Bool{},
		next:     atomic.Pointer[segment[V]]{},
	}
	e := &Elastic[V]{
		head: node,
	}
	e.tail.Store(node)
	return e, nil
}

func (e *Elastic[V]) Push(value V) (closed bool) {
	var contention cpu.Backoff
	for {
		if e.IsClosed() {
			return true
		}

		tail := e.tail.Load()
		added := tail.b.tryPush(value)
		if added {
			return false
		}
		tail.b.close()

		tail.mtx.Lock()
		if e.IsClosed() {
			tail.mtx.Unlock()
			return true
		}

		if tail.next.Load() != nil {
			tail.mtx.Unlock()
			contention.Spin()
			continue
		}

		nb := newBounded[V](int(tail.b.capacity))
		node := &segment[V]{
			b:        nb,
			mtx:      sync.Mutex{},
			initNext: atomic.Bool{},
			next:     atomic.Pointer[segment[V]]{},
		}
		tail.initNext.Store(true)
		e.tail.Store(node)
		tail.next.Store(node)
		tail.initNext.Store(false)
		tail.mtx.Unlock()
	}
}

func (e *Elastic[V]) TryPop() (value V, ok bool) {
	var closed bool
	for {
		value, ok, closed = e.head.b.tryPop()
		if ok {
			return value, true
		}

		if !closed {
			return value, false
		}

		if e.head.initNext.Load() {
			return value, false
		}

		next := e.head.next.Load()
		if next == nil {
			return value, false
		}
		e.head = next
	}
}

func (e *Elastic[V]) Pop() (value V, ok bool) {
	var (
		closed     bool
		contention cpu.Backoff
	)
	for {
		value, ok, closed = e.head.b.tryPop()
		if ok {
			return value, true
		}
		if !closed {
			return value, false
		}

	retry:
		if e.head.initNext.Load() {
			contention.Snooze()
			goto retry
		}

		next := e.head.next.Load()
		if next == nil {
			return value, false
		}
		e.head = next
	}
}

func (e *Elastic[V]) IsClosed() bool {
	return e.closed.Load()
}

func (e *Elastic[V]) Close() {
	if e.closed.Swap(true) {
		return
	}

	for {
		tail := e.tail.Load()
		tail.mtx.Lock()

		if e.tail.Load() == tail {
			tail.b.close()
			tail.mtx.Unlock()
			return
		}
		tail.mtx.Unlock()
	}
}
