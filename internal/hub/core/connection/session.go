/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connection

import (
	"sync/atomic"

	"xxx/internal/hub/core/transport"
	"xxx/internal/shared/device"
)

type sessionState uint32

const (
	sessionStateRegistering sessionState = iota + 1
	sessionStateActive
	sessionStateClosed
)

// Session owns its channel on the connection event loop. Only state is read
// across loops, to invalidate queued sends after replacement.
type Session struct {
	channel       transport.Channel
	deviceID      device.ID
	lastHeartBeat atomic.Int64
	state         atomic.Uint32
	failed        atomic.Bool
	// Owned by the connection event loop, including while presence is pending.
	activationCleanup func()
}

// newSession takes ownership of the channel and initializes a registering session.
func newSession(ch transport.Channel, id device.ID, timestamp int64) *Session {
	s := &Session{
		channel:  ch,
		deviceID: id,
	}
	s.lastHeartBeat.Store(timestamp)
	s.state.Store(uint32(sessionStateRegistering))
	return s
}

// Active reports whether activation completed and the channel has not failed.
func (s *Session) Active() bool {
	return s.state.Load() == uint32(sessionStateActive) && !s.failed.Load()
}

// DecryptRecord decrypts a record on the owning loop only while the session is active.
// The caller must clear the returned pooled buffer and return it to slicepool.
func (s *Session) DecryptRecord(frame []byte) ([]byte, error) {
	if !s.Active() || s.channel == nil {
		return nil, ErrSessionClosed
	}
	return s.channel.DecryptRecord(frame)
}

// isClosed reports whether the session has entered its terminal closed state.
func (s *Session) isClosed() bool {
	return s.state.Load() == uint32(sessionStateClosed)
}

// close atomically marks the session closed and reports whether this call changed its state.
func (s *Session) close() bool {
	return s.state.Swap(uint32(sessionStateClosed)) != uint32(sessionStateClosed)
}

// clear releases activation resources and closes the channel on the owning event loop.
func (s *Session) clear() {
	s.releaseActivation()
	if s.channel != nil {
		s.channel.Close()
		s.channel = nil
	}
}

// releaseActivation detaches and invokes the activation cleanup callback at most once.
// It must run on the owning connection event loop.
func (s *Session) releaseActivation() {
	cleanup := s.activationCleanup
	s.activationCleanup = nil
	if cleanup != nil {
		cleanup()
	}
}
