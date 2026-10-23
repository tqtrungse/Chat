/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package activateconn

import (
	"sync/atomic"
	"time"

	"xxx/internal/hub/core/transport"

	slicepool "xxx/pkg/pool/slice"
)

type pendingActivation struct {
	owner     *ConnActivator
	channel   transport.Channel
	identity  transport.Identity
	binding   []byte
	challenge []byte
	// deadline is the timestamp used to check expiry of the entire authentication process.
	deadline time.Time
	// phaseDeadline is the timestamp used to check expiry of the current phase:
	// handshake first, then proof submission and verification.
	phaseDeadline time.Time
	// timer enforces PreAuthTimeout for the entire activation, from admission
	// through handshake, authorization, presence registration, and ACK. Phase
	// transitions never reset it; clear stops it when activation ends.
	timer *time.Timer
	// phaseTimer first enforces HandshakeTimeout, then is replaced with the
	// authentication challenge timer after the server hello. That timer covers
	// both waiting for proof and queued/running verification. It stops when
	// registration begins; the overall timer still bounds registration and ACK.
	phaseTimer *time.Timer
	stage      uint8
	released   atomic.Bool
}

// clear runs on the connection event loop. Before registration, the connection
// context owns cleanup; after handoff, the session owns it until activation or close.
func (p *pendingActivation) clear() {
	if p == nil || !p.released.CompareAndSwap(false, true) {
		return
	}

	if p.timer != nil {
		p.timer.Stop()
	}

	if p.phaseTimer != nil {
		p.phaseTimer.Stop()
	}

	if p.channel != nil {
		p.channel.Close()
		p.channel = nil
	}

	clear(p.binding[:cap(p.binding)])
	clear(p.challenge[:cap(p.challenge)])
	slicepool.Put(p.challenge[:cap(p.challenge)])
	p.binding = nil
	p.challenge = nil

	p.owner.preAuth.Add(-1)
}
