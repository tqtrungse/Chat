//go:build 386 || amd64 || amd64p32

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
	"math"
	"math/bits"
	"sync/atomic"
	"unsafe"

	"xxx/pkg"
	"xxx/pkg/backoff"
)

const (
	readerUnpared  = uint32(0)
	readerParked   = uint32(1)
	readerIdxShift = uint32(16)
)

type Bounded[V any] struct {
	readIdx uint32
	_       [pkg.CacheLineSize - 4]byte

	sem            uint32
	metaReaderWait atomic.Uint32
	_              [pkg.CacheLineSize - 8]byte

	// state store [sendIdx (32bit) | activePushers (31bit) | closed (1bit)]
	state atomic.Uint64
	_     [pkg.CacheLineSize - 8]byte

	ring      []entry[V]
	entrySize uint32
	capacity  uint32
	n         uint32
	_         [pkg.CacheLineSize - unsafe.Sizeof([]entry[V]{}) - 12]byte
}

func NewBounded[V any](capacity int) (*Bounded[V], error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("capacity is zero")
	}
	if capacity > math.MaxUint16 {
		return nil, fmt.Errorf("capacity can not over %d", math.MaxUint16)
	}
	rawCapacity := uint32(pkg.NextPowerOfTwo(capacity))

	return &Bounded[V]{
		ring:      make([]entry[V], rawCapacity),
		entrySize: uint32(unsafe.Sizeof(entry[V]{})),
		capacity:  rawCapacity,
		n:         uint32(bits.TrailingZeros64(uint64(rawCapacity))),
	}, nil
}

func (b *Bounded[V]) TryPush(value V) (added bool) {
	s := b.state.Add(pusherOne)
	if s&closedFlag != 0 {
		b.state.Add(^(pusherOne - 1))
		return false
	}

	for {
		idx := uint32(s >> sendIdxShift)
		cycle := (idx >> b.n) << 1
		mapIdx := pkg.CacheRemap(idx, b.capacity, b.entrySize)
		ent := &b.ring[mapIdx]

		if ent.Cycle.Load() != cycle {
			b.state.Add(^(pusherOne - 1))
			return false
		}
		if !b.state.CompareAndSwap(s, s+sendIdxOne) {
			continue
		}
		ent.Value = value
		ent.Cycle.Store(cycle + 1)

		metaReaderWait := b.metaReaderWait.Load()
		readIdxWait := uint16(metaReaderWait >> readerIdxShift)
		if readIdxWait == uint16(mapIdx) && metaReaderWait&readerParked != 0 {
			b.metaReaderWait.Store(readerUnpared)
			runtimeSemRelease(&b.sem, false, 0)
		}
		b.state.Add(^(pusherOne - 1))
		return true
	}
}

func (b *Bounded[V]) Push(value V) (closed bool) {
	s := b.state.Add(pushAdd)

	if s&closedFlag != 0 {
		// Decrement pusher, not sendIdx (abandoned slot)
		b.state.Add(^(pusherOne - 1))
		return true
	}

	var (
		idx    = uint32(s>>sendIdxShift) - 1
		cycle  = (idx >> b.n) << 1
		mapIdx = pkg.CacheRemap(idx, b.capacity, b.entrySize)
		ent    = &b.ring[mapIdx]
		bc     = backoff.Cpu{}
	)

	for {
		cycleEnt := ent.Cycle.Load()
		if cycle == cycleEnt {
			ent.Value = value
			ent.Cycle.Store(cycleEnt + 1)

			metaReaderWait := b.metaReaderWait.Load()
			readIdxWait := uint16(metaReaderWait >> readerIdxShift)
			if readIdxWait == uint16(mapIdx) && metaReaderWait&readerParked != 0 {
				b.metaReaderWait.Store(readerUnpared)
				runtimeSemRelease(&b.sem, false, 0)
			}
			b.state.Add(^(pusherOne - 1))
			return
		}

		bc.Snooze()
		if b.IsClosed() {
			b.state.Add(^(pusherOne - 1))
			return true
		}
	}
}

func (b *Bounded[V]) TryPop() (value V, ok bool) {
	var (
		cycle    = (b.readIdx>>b.n)<<1 + 1
		mapIdx   = pkg.CacheRemap(b.readIdx, b.capacity, b.entrySize)
		ent      = &b.ring[mapIdx]
		cycleEnt = ent.Cycle.Load()
	)

	if cycle == cycleEnt {
		value = ent.Value
		ent.Value = pkg.ZeroValue[V]()
		ent.Cycle.Store(cycleEnt + 1)
		b.readIdx++
		return value, true
	}
	return pkg.ZeroValue[V](), false
}

// Pop retrieves the next value. After Close() has been called, Pop may
// return closed=true even while buffered values remain -- it does not
// guarantee full drain. Callers that need every remaining value after
// Close must call Drain, not rely on looping Pop until closed=true.
func (b *Bounded[V]) Pop() (value V, closed bool) {
	var (
		bc     = backoff.Cpu{}
		parked = false
		cycle  = (b.readIdx>>b.n)<<1 + 1
		mapIdx = pkg.CacheRemap(b.readIdx, b.capacity, b.entrySize)
		ent    = &b.ring[mapIdx]
	)

	for {
		cycleEnt := ent.Cycle.Load()
		if cycle == cycleEnt {
			value = ent.Value
			ent.Value = pkg.ZeroValue[V]()
			ent.Cycle.Store(cycleEnt + 1)
			if parked {
				b.metaReaderWait.Store(readerUnpared)
			}
			b.readIdx++
			return value, false
		}

		if b.IsClosed() {
			//if b.readIdx >= b.sendIdx.Load() {
			//	// Nothing was ever claimed at or beyond this index: a
			//	// genuine end of stream.
			//	if parked {
			//		b.metaReaderWait.Store(mpscReaderUnpared)
			//	}
			//	return pkg.ZeroValue[V](), true
			//}
			//// Something was claimed at or beyond this index -- it may
			//// still be mid-write (give it a moment), or it may be a slot
			//// internalPush claimed via FAA and abandoned because it
			//// wasn't writable yet. Either way this is not the true end.
			//bc.Snooze()
			//if !bc.IsDone() {
			//	continue
			//}
			// Waited long enough with no resolution: treat this slot as
			// abandoned and move past it instead of reporting a false
			// end-of-stream (and losing whatever comes after it).
			if parked {
				b.metaReaderWait.Store(readerUnpared)
				parked = false
			}
			//b.readIdx++
			//bc.Reset()
			//continue
			return pkg.ZeroValue[V](), true
		}

		bc.Snooze()
		if !bc.IsDone() {
			continue
		}

		if !parked {
			b.metaReaderWait.Store(readerParked + mapIdx<<readerIdxShift)
			parked = true
			continue
		}

		for {
			runtimeSemAcquire(&b.sem)
			if b.metaReaderWait.Load()&readerParked == 0 {
				break
			}
		}
		parked = false
		bc.Reset()
	}
}

func (b *Bounded[V]) IsClosed() bool {
	return b.state.Load()&closedFlag != 0
}

func (b *Bounded[V]) Close() {
	if b.state.Or(closedFlag)&closedFlag != 0 {
		return
	}

	// Wakes reader.
	if b.metaReaderWait.Load()&readerParked != 0 {
		b.metaReaderWait.Store(readerUnpared)
		runtimeSemRelease(&b.sem, false, 0)
	}
}

func (b *Bounded[V]) Drain(fn func(V)) {
	if b.state.Load()&closedFlag == 0 {
		return
	}

	bo := backoff.Cpu{}
	for (b.state.Load()>>pusherShift)&0x7FFFFFFF != 0 {
		bo.Snooze()
	}

	sendIdx := uint32(b.state.Load() >> sendIdxShift)
	for b.readIdx != sendIdx {
		var (
			ent      = &b.ring[pkg.CacheRemap(b.readIdx, b.capacity, b.entrySize)]
			cycleEnt = ent.Cycle.Load()
		)

		if cycleEnt&1 != 0 {
			fn(ent.Value)
			ent.Value = pkg.ZeroValue[V]()
			ent.Cycle.Store(cycleEnt + 1)
		}
		b.readIdx++
	}
}

func (b *Bounded[V]) Capacity() int {
	return int(b.capacity)
}

//go:linkname runtimeSemAcquire sync.runtime_Semacquire
func runtimeSemAcquire(addr *uint32)

//go:linkname runtimeSemRelease sync.runtime_Semrelease
func runtimeSemRelease(addr *uint32, handoff bool, skipframes int)
