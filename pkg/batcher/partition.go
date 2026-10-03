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

package batcher

import (
	"sync/atomic"

	"xxx/pkg/backoff"
)

type partition[T any] struct {
	incoming chan Req[T]
	state    atomic.Uint64
}

// acquire atomically admits a new Submit and increments the number of active
// Submit calls.
//
// The closed bit and active count share one atomic word. The CAS makes the
// admission decision and increment one indivisible operation, preventing this
// shutdown race:
//
// Submit: Load -> sees open
// Shutdown: sets closed bit -> observes zero active -> returns
// Submit: Add active
//
// A plain Load followed by Add would allow to Submit to be admitted after
// shutdown had already decided that no active callers remained.
func (p *partition[T]) acquire() bool {
	var bc backoff.Cpu
	for {
		state := p.state.Load()

		if state&closedBit != 0 {
			return false
		}

		if p.state.CompareAndSwap(state, state+1) {
			return true
		}

		bc.Snooze()
		if bc.IsDone() {
			bc.Reset()
		}
	}
}

// release decrements the number of Submit calls that have acquired admission.
// It must be called exactly once for every successful acquire.
func (p *partition[T]) release() {
	p.state.Add(^uint64(0))
}

func (p *partition[T]) close() {
	p.state.Or(closedBit)
}

func (p *partition[T]) activeCount() uint64 {
	return p.state.Load() & ^closedBit
}
