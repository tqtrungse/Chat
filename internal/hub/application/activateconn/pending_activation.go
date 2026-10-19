/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package activateconn

import (
	"sync/atomic"
	"time"

	sharedticket "xxx/internal/shared/ticket"
)

// pendingActivation is the conn context from OnOpen until activation succeeds
// or the conn closes. stage is touched only by the event-loop goroutine.
type pendingActivation struct {
	owner      *connActivator
	ticket     *sharedticket.Ticket // verified; holds session keys until activation completes
	transcript [32]byte
	nonce      [32]byte
	deadline   time.Time   // proof deadline
	timer      *time.Timer // whole pre-auth window
	stage      uint8
	released   atomic.Bool
}

// clear zeroes secrets, stops the timer and releases the pre-auth slot (exactly once).
func (p *pendingActivation) clear() {
	if p == nil {
		return
	}
	if p.timer != nil {
		p.timer.Stop()
	}
	if p.ticket != nil {
		p.ticket.Clear()
		p.ticket = nil
	}
	clear(p.transcript[:])
	clear(p.nonce[:])
	if p.released.CompareAndSwap(false, true) {
		p.owner.preAuth.Add(-1)
	}
}
