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

package worker

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"xxx/pkg"
	"xxx/pkg/backoff"
)

const segmentSize = 1024

type entry[V any] struct {
	Value V
	Cycle atomic.Bool
}

// boundedSegment is a single-use, fixed-capacity MPMC slot-claim buffer.
// Producers and consumers claim indices via atomic ops on enq/deq — no
// mutex, no per-item allocation beyond the (fixed-size) buf array.
type boundedSegment[V any] struct {
	sendIdx atomic.Uint64
	_       [pkg.CacheLineSize - 8]byte

	readIdx atomic.Uint64
	_       [pkg.CacheLineSize - 8]byte

	mtx  sync.Mutex
	next atomic.Pointer[boundedSegment[V]]
	_    [pkg.CacheLineSize - pkg.PtrSize - unsafe.Sizeof(sync.Mutex{})]byte

	buf       [segmentSize]entry[V]
	entrySize uint32
}

// tryClaimPush reserves one write slot. ok=false means the segment is
// full — caller must move to the next segment.
func (s *boundedSegment[V]) tryClaimPush() (idx int, ok bool) {
	pos := s.sendIdx.Add(1) - 1
	if pos >= segmentSize {
		return 0, false
	}
	return int(pos), true
}

// tryClaimPushN reserves up to n contiguous write slots in one atomic op
// (the batch analogue of tryClaimPush — mirrors claiming a contiguous
// range via a single fetch-and-add instead of n separate ones).
// claimed may be less than n (segment ran out of room) or zero (segment
// was already full when this call happened).
func (s *boundedSegment[T]) tryClaimPushN(n int) (start, claimed int) {
	pos := s.sendIdx.Add(uint64(n)) - uint64(n)
	if pos >= segmentSize {
		return 0, 0
	}
	end := min(pos+uint64(n), segmentSize)
	return int(pos), int(end - pos)
}

// tryClaimPopN reserves up to want contiguous read slots, but — unlike
// push — never claims past what has actually been claimed for writing
// (s.enq snapshot). Blindly FAA-ing here would let a consumer race ahead
// of production and spin forever on a slot nobody has claimed yet (e.g.
// an idle queue). CAS-loop bounded by the enq snapshot avoids that.
func (s *boundedSegment[V]) tryClaimPopN(want int) (start, claimed int) {
	for {
		pos := s.readIdx.Load()
		avail := min(s.sendIdx.Load(), segmentSize)
		if pos >= avail {
			return 0, 0 // nothing currently claimed-for-write to take
		}
		n := uint64(want)
		if remain := avail - pos; n > remain {
			n = remain
		}
		if s.readIdx.CompareAndSwap(pos, pos+n) {
			return int(pos), int(n)
		}
		// Lost the race to another consumer; retry with fresh values.
	}
}

func (s *boundedSegment[V]) read(idx int) V {
	mapIdx := pkg.CacheRemap(uint32(idx), segmentSize, s.entrySize)
	c := &s.buf[mapIdx]
	b := backoff.Cpu{}
	for !c.Cycle.Load() {
		b.Snooze()
	}
	item := c.Value
	c.Value = pkg.ZeroValue[V]()
	return item
}

func (s *boundedSegment[V]) publish(idx int, value V) {
	mapIdx := pkg.CacheRemap(uint32(idx), segmentSize, s.entrySize)
	s.buf[mapIdx].Value = value
	s.buf[mapIdx].Cycle.Store(true) // release: consumer's Load below pairs with this
}

// elastic is an unbounded MPMC FIFO queue backed by a linked list of
// boundedSegments. Intra-segment claim/publish/read (Push/PushBatch/
// Pop/PopBatch's normal path — the hot path, hit on every single item)
// is lock-free. Segment-list transitions (linking a new tail segment,
// advancing head past a drained one) are guarded by listMu instead of a
// lock-free CAS dance: that transition only fires once every
// segmentSize items, so a mutex there is cheap, and it removes a whole
// class of races between two independent CAS steps racing each other
// across concurrent producers/consumers on the same segment boundary.
// Zero value is not usable; use New.
type elastic[V any] struct {
	head atomic.Pointer[boundedSegment[V]]
	_    [pkg.CacheLineSize - pkg.PtrSize]byte

	tail atomic.Pointer[boundedSegment[V]]
	_    [pkg.CacheLineSize - pkg.PtrSize]byte
}

func newElastic[V any]() *elastic[V] {
	s := &boundedSegment[V]{
		entrySize: uint32(unsafe.Sizeof(entry[V]{})),
	}
	q := &elastic[V]{}
	q.head.Store(s)
	q.tail.Store(s)
	return q
}

// Push enqueues a single item at the tail. Lock-free on the fast path;
// amortized O(1) — falls through to growTail's mutex only once every
// segmentSize pushes.
func (e *elastic[V]) Push(value V) {
	for {
		tail := e.tail.Load()
		if idx, ok := tail.tryClaimPush(); ok {
			tail.publish(idx, value)
			return
		}
		e.growthTail(tail)
	}
}

// PushBatch enqueues items in order, claiming contiguous ranges per
// segment via a single fetch-and-add instead of one per item, spilling
// into new segments as needed.
func (e *elastic[T]) PushBatch(items []T) {
	for len(items) > 0 {
		tail := e.tail.Load()
		start, claimed := tail.tryClaimPushN(len(items))
		if claimed == 0 {
			e.growthTail(tail)
			continue
		}
		for i := range claimed {
			tail.publish(start+i, items[i])
		}
		items = items[claimed:]
	}
}

// PopBatch drains up to max items from the head. Returns nil if the
// queue is empty or max <= 0.
func (e *elastic[V]) PopBatch(max int) ([]V, bool) {
	if max <= 0 {
		return nil, false
	}

	var out []V
	for len(out) < max {
		head := e.head.Load()
		want := max - len(out)
		start, claimed := head.tryClaimPopN(want)
		if claimed == 0 {
			next := head.next.Load()
			if next == nil {
				break
			}
			// next != nil is only ever set after this segment's sendIdx
			// reached segmentSize, and that Store synchronizes-with this
			// Load — so a fresh claim attempt here is guaranteed to see
			// the segment's final, stable state. Without this recheck,
			// items published between our first tryClaimPopN snapshot and
			// observing `next` get silently abandoned when head advances.
			start, claimed = head.tryClaimPopN(want)
			if claimed == 0 {
				e.head.CompareAndSwap(head, next)
				continue
			}
		}
		if out == nil {
			out = make([]V, 0, max)
		}
		for i := range claimed {
			out = append(out, head.read(start+i))
		}
	}
	return out, len(out) > 0
}

func (e *elastic[V]) growthTail(tail *boundedSegment[V]) {
	if next := tail.next.Load(); next != nil {
		e.tail.CompareAndSwap(tail, next)
		return
	}

	tail.mtx.Lock()
	// double check.
	if next := tail.next.Load(); next != nil {
		e.tail.CompareAndSwap(tail, next)
		tail.mtx.Unlock()
		return
	}

	next := &boundedSegment[V]{
		entrySize: tail.entrySize,
	}
	tail.next.Store(next)
	// Use CAS to update the tail safely. Although this operation is inside the Lock,
	// Using CAS will help synchronize with threads that are "pushing" outside.
	e.tail.CompareAndSwap(tail, next)
	tail.mtx.Unlock()
}
