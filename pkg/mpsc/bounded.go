/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package mpsc

import (
	"math/bits"
	"sync/atomic"
	"unsafe"

	"xxx/pkg/cpu"
	"xxx/pkg/utils/bit"
	"xxx/pkg/utils/generic"
)

type bounded[V any] struct {
	readIdx uint32
	_       [cpu.CacheLineSize - 4]byte

	// state store [sendIdx (32bit) | activePushers (31bit) | closed (1bit)]
	state atomic.Uint64
	_     [cpu.CacheLineSize - 8]byte

	ring      []entry[V]
	entrySize uint32
	capacity  uint32
	n         uint32
	_         [cpu.CacheLineSize - unsafe.Sizeof([]entry[V]{}) - 12]byte
}

func newBounded[V any](capacity int) *bounded[V] {
	rawCapacity := uint32(bit.NextPowerOfTwo(capacity))
	return &bounded[V]{
		ring:      make([]entry[V], rawCapacity),
		entrySize: uint32(unsafe.Sizeof(entry[V]{})),
		capacity:  rawCapacity,
		n:         uint32(bits.TrailingZeros64(uint64(rawCapacity))),
	}
}

func (b *bounded[V]) tryPush(value V) (added bool) {
	// Wait-Free FAA: Add sendIdx and activePushers at the same time.
	s := b.state.Add(pushAdd)

	// Check closed flag.
	if s&closedFlag != 0 {
		// Decrement pusher, not sendIdx (abandoned slot)
		b.state.Add(^(pusherOne - 1))
		return false
	}

	var (
		// Separate the sendIdx. Subtract 1 to get back the exact index that was just reserved.
		idx    = uint32(s>>sendIdxShift) - 1
		cycle  = (idx >> b.n) << 1
		mapIdx = cpu.CacheRemap(idx, b.capacity, b.entrySize)
		ent    = &b.ring[mapIdx]
	)

	cycleEnt := ent.Cycle.Load()
	if cycle == cycleEnt {
		ent.Value = value
		ent.Cycle.Store(cycleEnt + 1)
		b.state.Add(^(pusherOne - 1))
		return true
	}
	b.state.Add(^(pusherOne - 1))
	return false
}

func (b *bounded[V]) tryPop() (value V, ok bool, closed bool) {
	var (
		cycle    = (b.readIdx>>b.n)<<1 + 1
		mapIdx   = cpu.CacheRemap(b.readIdx, b.capacity, b.entrySize)
		ent      = &b.ring[mapIdx]
		cycleEnt = ent.Cycle.Load()
	)

	if cycle == cycleEnt {
		value = ent.Value
		ent.Value = generic.ZeroValue[V]()
		ent.Cycle.Store(cycleEnt + 1)
		b.readIdx++
		return value, true, false
	}

	// Read the full status (Point-In-Time).
	s := b.state.Load()
	isClosed := s&closedFlag != 0
	activePushers := (s >> pusherShift) & 0x7FFFFFFF

	if !isClosed || activePushers != 0 {
		return value, false, false
	}

	// Get the latest sendIdx when reading the state.
	sendIdx := uint32(s >> sendIdxShift)

	// drain.
	for b.readIdx != sendIdx {
		ent = &b.ring[cpu.CacheRemap(b.readIdx, b.capacity, b.entrySize)]
		cycleEnt = ent.Cycle.Load()
		// Skip abandoned slots that no one has written to yet.
		if cycleEnt&1 == 0 {
			b.readIdx++
			continue
		}
		value = ent.Value
		ent.Value = generic.ZeroValue[V]()
		ent.Cycle.Store(cycleEnt + 1)
		b.readIdx++
		return value, true, false
	}
	return value, false, true
}

func (b *bounded[V]) close() {
	for {
		s := b.state.Load()
		if s&closedFlag != 0 {
			return
		}
		if b.state.CompareAndSwap(s, s|closedFlag) {
			return
		}
	}
}