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
	"sync/atomic"

	shareddevice "xxx/internal/shared/device"

	"xxx/pkg/bit"
	"xxx/pkg/collection/swiss"
	"xxx/pkg/cpu"
	"xxx/pkg/hash"
	"xxx/pkg/nio"
)

const (
	nShards = 8
	mask    = nShards - 1
	closed  = uint32(1) << 31

	defaultMaxConns = 128000
)

// connectionRegistry owns the sharded device-to-connection map and its
// capacity accounting. A slot remains charged until either Remove or a
// replacement wins the old session's active-to-closed transition.
type connectionRegistry struct {
	meta atomic.Uint32
	_    [cpu.CacheLineSize - 4]byte

	conns    [nShards]*swiss.TableC2[shareddevice.ID, nio.Conn]
	maxConns uint32
}

func newConnectionRegistry(maxConns uint32) *connectionRegistry {
	if maxConns == 0 {
		maxConns = defaultMaxConns
	}

	r := &connectionRegistry{maxConns: maxConns}
	nItemsPerShard := int(maxConns / nShards)
	if maxConns%uint32(nShards) != 0 {
		nItemsPerShard++
	}
	for i := range nShards {
		r.conns[i] = swiss.NewTableC2WithCap[shareddevice.ID, nio.Conn](
			nItemsPerShard,
			hash.Int64,
		)
	}
	return r
}

// Replace installs conn as the current connection for deviceID and returns
// the prior connection, if any. The provided session becomes conn's context.
func (r *connectionRegistry) Replace(
	deviceID shareddevice.ID,
	conn nio.Conn,
	session *Session,
) (nio.Conn, error) {
	if err := r.reserveSlot(); err != nil {
		return nil, err
	}

	conn.SetContext(session)
	old, exists := r.conns[r.bucketFor(deviceID)].Set(deviceID, conn)
	if !exists {
		return nil, nil
	}

	if old == conn {
		// Re-activation on the same connection consumes no additional slot.
		r.releaseSlot()
		return nil, nil
	}

	if oldSession, ok := old.Context().(*Session); ok && oldSession.close() {
		// The old slot is inherited by this new connection.
		r.releaseSlot()
	}
	return old, nil
}

// Remove closes and removes conn if this call wins its session's close
// transition. A replaced connection has already released its slot and is a
// no-op here.
func (r *connectionRegistry) Remove(conn nio.Conn) (*Session, bool) {
	session, ok := conn.Context().(*Session)
	if !ok || !session.close() {
		return nil, false
	}

	if current, exists := r.Lookup(session.deviceID); exists && current == conn {
		r.conns[r.bucketFor(session.deviceID)].Delete(session.deviceID)
	}
	r.releaseSlot()
	return session, true
}

func (r *connectionRegistry) Lookup(deviceID shareddevice.ID) (nio.Conn, bool) {
	return r.conns[r.bucketFor(deviceID)].Lookup(deviceID)
}

func (r *connectionRegistry) MaxConns() uint32 {
	return r.maxConns
}

func (r *connectionRegistry) CurConns() uint32 {
	current := r.meta.Load() &^ closed
	if current >= r.maxConns {
		return r.maxConns + 1
	}
	return current
}

func (r *connectionRegistry) Close() {
	r.meta.Or(closed)
}

func (r *connectionRegistry) reserveSlot() error {
	for {
		current := r.meta.Load()
		if current&closed != 0 {
			return ErrManagerClosed
		}
		if current >= r.maxConns {
			return ErrConnReachMax
		}
		if r.meta.CompareAndSwap(current, current+1) {
			return nil
		}
	}
}

func (r *connectionRegistry) releaseSlot() {
	r.meta.Add(^uint32(0))
}

func (r *connectionRegistry) bucketFor(deviceID shareddevice.ID) int {
	return int(bit.Mix64(deviceID.Uint64())) & mask
}
