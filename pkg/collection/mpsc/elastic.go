/*
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

package mpsc

import (
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"
	"unsafe"

	"xxx/pkg"
	"xxx/pkg/backoff"
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

// Elastic is MPSC unbounded queue assembled from a linked chain of fixed-size ring buffers.
type Elastic[V any] struct {
	head *rbNode[V]
	_    [pkg.CacheLineSize - pkg.PtrSize]byte

	tail   atomic.Pointer[rbNode[V]]
	closed atomic.Bool
	_      [pkg.CacheLineSize - pkg.PtrSize - 4]byte
}

func NewElastic[V any](nodeCap int) (*Elastic[V], error) {
	if nodeCap <= 0 {
		return nil, fmt.Errorf("invalid node capacity: %d", nodeCap)
	}
	node := &rbNode[V]{
		rb:       newRb[V](nodeCap),
		mtx:      sync.Mutex{},
		initNext: atomic.Bool{},
		next:     atomic.Pointer[rbNode[V]]{},
	}
	e := &Elastic[V]{
		head: node,
	}
	e.tail.Store(node)
	return e, nil
}

func (e *Elastic[V]) Push(value V) (closed bool) {
	var bc backoff.Cpu
	for {
		if e.IsClosed() {
			return true
		}

		tail := e.tail.Load()
		added := tail.rb.tryPush(value)
		if added {
			return false
		}
		tail.rb.close()

		tail.mtx.Lock()
		if e.IsClosed() {
			tail.mtx.Unlock()
			return true
		}

		if tail.next.Load() != nil {
			tail.mtx.Unlock()
			bc.Spin()
			continue
		}

		nrb := newRb[V](int(tail.rb.capacity))
		node := &rbNode[V]{
			rb:       nrb,
			mtx:      sync.Mutex{},
			initNext: atomic.Bool{},
			next:     atomic.Pointer[rbNode[V]]{},
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
		value, ok, closed = e.head.rb.tryPop()
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
		closed bool
		bkc    backoff.Cpu
	)
	for {
		value, ok, closed = e.head.rb.tryPop()
		if ok {
			return value, true
		}
		if !closed {
			return value, false
		}

	retry:
		if e.head.initNext.Load() {
			bkc.Snooze()
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
			tail.rb.close()
			tail.mtx.Unlock()
			return
		}
		tail.mtx.Unlock()
	}
}

type rbNode[V any] struct {
	rb *rb[V]
	_  [pkg.CacheLineSize - pkg.PtrSize]byte

	mtx sync.Mutex
	_   [pkg.CacheLineSize - unsafe.Sizeof(sync.Mutex{})]byte

	next     atomic.Pointer[rbNode[V]]
	initNext atomic.Bool
	_        [pkg.CacheLineSize - pkg.PtrSize - 4]byte
}

type rb[V any] struct {
	readIdx uint32
	_       [pkg.CacheLineSize - 4]byte

	// state store [sendIdx (32bit) | activePushers (31bit) | closed (1bit)]
	state atomic.Uint64
	_     [pkg.CacheLineSize - 8]byte

	ring      []entry[V]
	entrySize uint32
	capacity  uint32
	n         uint32
	_         [pkg.CacheLineSize - unsafe.Sizeof([]entry[V]{}) - 12]byte
}

func newRb[V any](capacity int) *rb[V] {
	rawCapacity := uint32(pkg.NextPowerOfTwo(capacity))
	return &rb[V]{
		ring:      make([]entry[V], rawCapacity),
		entrySize: uint32(unsafe.Sizeof(entry[V]{})),
		capacity:  rawCapacity,
		n:         uint32(bits.TrailingZeros64(uint64(rawCapacity))),
	}
}

func (r *rb[V]) tryPush(value V) (added bool) {
	// Wait-Free FAA: Add sendIdx and activePushers at the same time.
	s := r.state.Add(pushAdd)

	// Check closed flag.
	if s&closedFlag != 0 {
		// Decrement pusher, not sendIdx (abandoned slot)
		r.state.Add(^(pusherOne - 1))
		return false
	}

	var (
		// Separate the sendIdx. Subtract 1 to get back the exact index that was just reserved.
		idx    = uint32(s>>sendIdxShift) - 1
		cycle  = (idx >> r.n) << 1
		mapIdx = pkg.CacheRemap(idx, r.capacity, r.entrySize)
		ent    = &r.ring[mapIdx]
	)

	cycleEnt := ent.Cycle.Load()
	if cycle == cycleEnt {
		ent.Value = value
		ent.Cycle.Store(cycleEnt + 1)
		r.state.Add(^(pusherOne - 1))
		return true
	}
	r.state.Add(^(pusherOne - 1))
	return false
}

func (r *rb[V]) tryPop() (value V, ok bool, closed bool) {
	var (
		cycle    = (r.readIdx>>r.n)<<1 + 1
		mapIdx   = pkg.CacheRemap(r.readIdx, r.capacity, r.entrySize)
		ent      = &r.ring[mapIdx]
		cycleEnt = ent.Cycle.Load()
	)

	if cycle == cycleEnt {
		value = ent.Value
		ent.Value = pkg.ZeroValue[V]()
		ent.Cycle.Store(cycleEnt + 1)
		r.readIdx++
		return value, true, false
	}

	// Read the full status (Point-In-Time).
	s := r.state.Load()
	isClosed := s&closedFlag != 0
	activePushers := (s >> pusherShift) & 0x7FFFFFFF

	if !isClosed || activePushers != 0 {
		return value, false, false
	}

	// Get the latest sendIdx when reading the state.
	sendIdx := uint32(s >> sendIdxShift)

	// drain.
	for r.readIdx != sendIdx {
		ent = &r.ring[pkg.CacheRemap(r.readIdx, r.capacity, r.entrySize)]
		cycleEnt = ent.Cycle.Load()
		// Skip abandoned slots that no one has written to yet.
		if cycleEnt&1 == 0 {
			r.readIdx++
			continue
		}
		value = ent.Value
		ent.Value = pkg.ZeroValue[V]()
		ent.Cycle.Store(cycleEnt + 1)
		r.readIdx++
		return value, true, false
	}
	return value, false, true
}

func (r *rb[V]) close() {
	for {
		s := r.state.Load()
		if s&closedFlag != 0 {
			return
		}
		if r.state.CompareAndSwap(s, s|closedFlag) {
			return
		}
	}
}
