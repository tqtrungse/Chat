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
	"fmt"
	"sync/atomic"
	"unsafe"

	"xxx/pkg"
)

// 1. Each local queue has exactly one owner.
// 2. Owner is the only goroutine allowed to modify tail.
// 3. Thieves may CAS head on a victim queue.
// 4. A thief reserves [share, cur) before copying.
// 5. While share != cur, the reserved range must not be read/written by owner.
// 6. Destination of MoveTo must be exclusively owned by the stealing worker.
// 7. tail is the publication point for writes to buf.

const (
	// capacity must stay a power of two — index masking relies on it.
	capacity = 256
	mask     = capacity - 1
)

func packHead(share, cur uint32) uint64 {
	return uint64(cur) | (uint64(share) << 32)
}

func unpackHead(n uint64) (share, cur uint32) {
	return uint32(n >> 32), uint32(n)
}

// local is the owner handle for one worker's local queue.
//
// The owning worker is the only goroutine that may modify tail or write
// to the queue as its owner. Other workers may concurrently steal from
// the queue by modifying head through MoveTo.
//
// head = (share, cur)
//
// share != cur
//
//	=> a thief is copying a reserved range
//
// tail
//
//	=> publication boundary
//
// owner
//
//	=> only writer of tail/buf
//
// thief
//
//	=> CAS head + read source
type local[V any] struct {
	head atomic.Uint64 // packed (share, cur) — see package doc
	_    [pkg.CacheLineSize - 8]byte

	tail atomic.Uint32 // single writer (the owning Local), many readers
	_    [pkg.CacheLineSize - 4]byte

	buf       [capacity]V
	entrySize uint32
}

// newLocal creates a fresh local queue.
func newLocal[V any]() *local[V] {
	return &local[V]{
		entrySize: uint32(unsafe.Sizeof(pkg.ZeroValue[V]())),
	}
}

// Len returns a snapshot of the number of tasks currently visible from
// the owner's point of view.
func (l *local[V]) Len() int {
	_, cur := unpackHead(l.head.Load())
	tail := l.tail.Load()
	return int(tail - cur)
}

// RemainingSlots returns the number of slots currently available for
// appending without overflowing the queue.
//
// Tasks reserved by an in-progress share are treated as occupied until
// the share completes.
func (l *local[V]) RemainingSlots() int {
	share, _ := unpackHead(l.head.Load())
	tail := l.tail.Load()
	return int(capacity - (tail - share))
}

// Push adds a task to the back of the queue.
//
// If the queue has room, the task is appended locally. If the queue is
// full while no share is in flight, half of the queue is moved to the
// overflow queue together with task. If another worker is currently
// sharing the queue, task is sent directly to overflow instead of waiting
// for the share to complete.
func (l *local[V]) Push(task V, e *elastic[V]) {
	var tail uint32
	for {
		head := l.head.Load()
		share, cur := unpackHead(head)
		tail = l.tail.Load()

		if tail-share < capacity {
			break // room for the task
		} else if share != cur {
			// A share is mid-flight and will free capacity shortly —
			// don't wait for it, just spill this one task.
			e.Push(task)
			return
		}

		if l.pushOverflow(task, cur, tail, e) {
			return
		}
		// Lost the race to a concurrent share claiming a batch; the
		// queue may not be full anymore — retry from the top.
	}
	l.pushFinish(task, tail)
}

// pushFinish writes task into the slot at tail and publishes it by
// advancing tail.
func (l *local[V]) pushFinish(task V, tail uint32) {
	mapIdx := pkg.CacheRemap(tail&mask, capacity, l.entrySize)
	l.buf[mapIdx] = task
	l.tail.Store(tail + 1) // publish
}

// pushOverflow claims the full queue, keeps the first half in the local
// queue, and moves the second half together with task to overflow.
//
// The first half is retained without copying by advancing tail by half
// the capacity; the new logical range aliases the same physical slots
// through the circular buffer.
func (l *local[V]) pushOverflow(task V, head, tail uint32, e *elastic[V]) bool {
	const numTasksTaken = capacity / 2

	if tail-head != capacity {
		panic(fmt.Sprintf("localqueue: pushOverflow called on a non-full queue; tail=%d head=%d", tail, head))
	}

	// Claim everything currently queued. To any concurrent Push or
	// capacity check, the queue now looks momentarily empty.
	if !l.head.CompareAndSwap(packHead(head, head), packHead(tail, tail)) {
		return false
	}

	// "Add back" the first half: advancing tail by numTasksTaken (not by
	// the full capacity) makes the new logical range
	// [head+capacity, tail+numTasksTaken) alias — modulo capacity — the
	// exact same physical slots the original first half already
	// occupied. No data movement needed for those.
	l.tail.Store(tail + numTasksTaken)

	// The second half is what actually leaves the local queue.
	batch := make([]V, numTasksTaken, numTasksTaken+1)
	for i := range numTasksTaken {
		idx := (int(head) + numTasksTaken + i) & mask
		mapIdx := pkg.CacheRemap(uint32(idx), capacity, l.entrySize)
		batch[i] = l.buf[mapIdx]
	}
	batch = append(batch, task)

	e.PushBatch(batch)
	return true
}

// PushBatchNoOverflow appends tasks to the local queue without performing
// overflow handling. The caller must ensure that all tasks fit in the
// queue and that the caller is the queue's owner.
//
// It panics if tasks exceeds the queue capacity or does not fit in the
// currently available space.
//
// This is used to refill a worker's local queue from the shared overflow
// queue at the start of the scheduling loop.
func (l *local[V]) PushBatchNoOverflow(tasks []V) {
	n := len(tasks)
	if n == 0 {
		return
	}
	if uint32(n) > capacity {
		panic(fmt.Sprintf("localqueue: batch of %d exceeds capacity %d", n, capacity))
	}

	share, _ := unpackHead(l.head.Load())
	tail := l.tail.Load()

	if tail-share > capacity-uint32(n) {
		panic(fmt.Sprintf("localqueue: not enough room for batch of %d (remaining=%d)", n, capacity-(tail-share)))
	}

	for _, t := range tasks {
		mapIdx := pkg.CacheRemap(tail&mask, capacity, l.entrySize)
		l.buf[mapIdx] = t
		tail++
	}
	l.tail.Store(tail)
}

// Pop removes and returns one task from the front of the queue.
// It returns false if the queue is empty.
func (l *local[V]) Pop() (item V, ok bool) {
	head := l.head.Load()
	var idx uint32
	for {
		share, cur := unpackHead(head)
		tail := l.tail.Load()

		if cur == tail {
			return pkg.ZeroValue[V](), false
		}

		nextReal := cur + 1
		var next uint64
		if share == cur {
			next = packHead(nextReal, nextReal)
		} else {
			next = packHead(share, nextReal)
		}

		if l.head.CompareAndSwap(head, next) {
			idx = cur & mask
			break
		}
		head = l.head.Load()
	}
	mapIdx := pkg.CacheRemap(idx, capacity, l.entrySize)
	return l.buf[mapIdx], true
}

// MoveTo steals roughly half of the available tasks from l and moves
// them into dst.
//
// One stolen task is returned directly for the caller to execute.
// The remaining stolen tasks, if any, are appended to dst.
//
// dst must be the local queue owned by the calling worker.
func (l *local[V]) MoveTo(dst *local[V]) (item V, ok bool) {
	dstTail := dst.tail.Load()

	// dst may look empty to its own owner but still have capacity
	// reserved if a third party is concurrently sharing INTO dst too.
	dstShare, _ := unpackHead(dst.head.Load())
	if dstTail-dstShare > capacity/2 {
		return pkg.ZeroValue[V](), false
	}

	n := l.moveInto2(dst, dstTail)
	if n == 0 {
		return pkg.ZeroValue[V](), false
	}

	n--
	mapIdx := pkg.CacheRemap((dstTail+n)&mask, capacity, l.entrySize)
	ret := dst.buf[mapIdx]

	if n == 0 {
		return ret, true // only one task total was stolen
	}

	dst.tail.Store(dstTail + n) // publish the rest to dst
	return ret, true
}

// moveInto2 reserves roughly half of l's available tasks, copies the
// reserved range into dst, and completes the share.
//
// While the copy is in progress, the reserved range is protected from
// being consumed by the source owner. Owner operations may continue on
// tasks outside the reserved range.
//
// It returns the number of tasks moved, or zero if the source cannot
// currently be shared.
func (l *local[V]) moveInto2(dst *local[V], dstTail uint32) uint32 {
	prevPacked := l.head.Load()

	var n uint32
	var nextPacked uint64
	for {
		srcShare, srcCur := unpackHead(prevPacked)
		srcTail := l.tail.Load()

		// someone else is already sharing from this queue
		if srcShare != srcCur {
			return 0
		}

		// share ceil(avail/2), leave the victim floor(avail/2)
		avail := srcTail - srcCur
		take := avail - avail/2
		if take == 0 {
			return 0
		}

		shareTo := srcCur + take
		nextPacked = packHead(srcShare, shareTo)

		if l.head.CompareAndSwap(prevPacked, nextPacked) {
			n = take
			break
		}
		prevPacked = l.head.Load()
	}

	first, _ := unpackHead(nextPacked) // == srcShare: the batch's starting index
	for i := uint32(0); i < n; i++ {
		dstMapIdx := pkg.CacheRemap((dstTail+i)&mask, capacity, l.entrySize)
		mapIdx := pkg.CacheRemap((first+i)&mask, capacity, l.entrySize)
		dst.buf[dstMapIdx] = l.buf[mapIdx]
	}

	// Signal completion: advance "share" to match the new "real",
	// reopening this queue for future shares.
	prevPacked = nextPacked
	for {
		_, cur := unpackHead(prevPacked)
		next := packHead(cur, cur)
		if l.head.CompareAndSwap(prevPacked, next) {
			return n
		}
		prevPacked = l.head.Load()
	}
}
