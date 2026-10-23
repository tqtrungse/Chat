/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package batcher

import (
	"sync/atomic"
	"unsafe"

	"xxx/pkg/cpu"
	"xxx/pkg/os"
)

// partition holds the admission state and the queue of one batching loop.
//
// state packs two values. The top bit is closedBit.
// The remaining bits count currently admitted Submit calls (active).
// Each successful acquire requires one release.
// Once close sets closedBit, no later acquire can succeed.
type partition[T any] struct {
	state atomic.Uint64
	_     [cpu.CacheLineSize - unsafe.Sizeof(atomic.Uint64{})]byte

	incoming chan Req[T]
	_        [cpu.CacheLineSize - os.PtrSize]byte
}

// acquire admits a new Submit call and increments the active count.
//
// acquire uses an optimistic increment.
// First, acquire adds 1 atomically and examines the returned closed bit.
// If admission is closed, acquire reverses the increment and rejects the call.
// A successful caller already contributes to the active count before it can enqueue.
// There is no separate Load-then-Add gap.
//
// After close, a late caller can temporarily increase activeCount before reversal.
// This increase can delay shutdown, but it cannot permit an enqueue.
func (p *partition[T]) acquire() bool {
	if p.state.Add(1)&closedBit != 0 {
		p.state.Add(^uint64(0)) // Reverse the failed admission increment.
		return false
	}
	return true
}

// release decrements the number of Submit calls that have admission.
// Call release once for each successful acquire, after the enqueue attempt completes.
func (p *partition[T]) release() {
	p.state.Add(^uint64(0))
}

func (p *partition[T]) close() {
	p.state.Or(closedBit)
}

func (p *partition[T]) activeCount() uint64 {
	return p.state.Load() & ^closedBit
}
