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

package connection

import (
	"errors"
	"sync"
	"sync/atomic"

	shareddevice "xxx/internal/shared/device"
	sharedsession "xxx/internal/shared/session"
)

type sessionState uint32

const (
	sessionStateActive sessionState = iota + 1
	sessionStateClosed
)

// ErrSendUnavailable means an earlier write failed and the stream cannot
// safely accept another frame.
var ErrSendUnavailable = errors.New("session send unavailable")

// Session is the runtime state attached to an active network connection.
// Its fields are private so connection state can only be changed through this
// package's lifecycle and sender components.
type Session struct {
	keys          sharedsession.Keys
	deviceID      shareddevice.ID
	lastHeartBeat atomic.Int64
	state         atomic.Uint32

	// recvSeq is owned by the event-loop goroutine for this connection.
	recvSeq uint64

	sendMu  sync.Mutex
	sendSeq uint64
	// sendFailed marks the outbound stream unusable after an AsyncWrite error;
	// a partial write may have happened, so later frames must not be queued.
	sendFailed atomic.Bool
}

func newSession(keys sharedsession.Keys, deviceID shareddevice.ID, lastHeartBeat int64) *Session {
	s := &Session{
		keys:     keys,
		deviceID: deviceID,
	}
	s.lastHeartBeat.Store(lastHeartBeat)
	s.state.Store(uint32(sessionStateActive))
	return s
}

func (s *Session) isClosed() bool {
	return s.state.Load() == uint32(sessionStateClosed)
}

// close returns true to the goroutine that wins the active-to-closed change.
func (s *Session) close() bool {
	return s.state.Swap(uint32(sessionStateClosed)) != uint32(sessionStateClosed)
}

func (s *Session) clear() {
	s.keys.Clear()
}

// RecvSeq returns the sequence expected on the next client-to-server frame.
// It is called on the connection's event loop.
func (s *Session) RecvSeq() uint64 {
	return s.recvSeq
}

// AdvanceRecv accepts the next receive sequence. Call it only after a frame
// has been successfully authenticated.
func (s *Session) AdvanceRecv() {
	s.recvSeq++
}

// WithSendSeq serializes sequence allocation with the AsyncWrite enqueue so
// sequence order matches bytes queued on the TCP stream.
func (s *Session) WithSendSeq(enqueue func(seq uint64) (enqueued bool, err error)) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	if s.sendFailed.Load() {
		return ErrSendUnavailable
	}

	enqueued, err := enqueue(s.sendSeq)
	if enqueued {
		if err == nil {
			s.sendSeq++
		} else {
			s.sendFailed.Store(true)
		}
	}
	return err
}

func (s *Session) MarkSendFailed() {
	s.sendFailed.Store(true)
}

func (s *Session) SendFailed() bool {
	return s.sendFailed.Load()
}
