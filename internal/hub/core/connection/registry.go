/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connection

import (
	"sync"
	"sync/atomic"

	"xxx/internal/shared/device"

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
)

// connectionRegistry owns the sharded device-to-connection map and its
// capacity accounting. A slot remains charged until either Remove or a
// replacement wins the old session's active-to-closed transition.
type connectionRegistry struct {
	meta atomic.Uint32
	_    [cpu.CacheLineSize - 4]byte

	lifecycle sync.RWMutex
	shardMtx  [nShards]sync.Mutex
	conns     [nShards]*swiss.TableC2[device.ID, nio.Conn]
	maxConns  uint32
}

// newConnectionRegistry allocates a sharded device map with the given connection capacity.
func newConnectionRegistry(maxConns uint32) *connectionRegistry {
	nItemsPerShard := int(maxConns / nShards)
	if maxConns%uint32(nShards) != 0 {
		nItemsPerShard++
	}

	r := &connectionRegistry{
		maxConns: maxConns,
	}
	for i := range nShards {
		r.conns[i] = swiss.NewTableC2WithCap[device.ID, nio.Conn](
			nItemsPerShard,
			hash.Int64,
		)
	}
	return r
}

// Replace installs conn as the current connection for deviceID and returns
// the prior connection, if any. The provided session becomes conn's context.
func (r *connectionRegistry) Replace(
	deviceID device.ID,
	conn nio.Conn,
	session *Session,
) (nio.Conn, error) {
	r.lifecycle.RLock()
	defer r.lifecycle.RUnlock()

	bucket := r.bucketFor(deviceID)
	r.shardMtx[bucket].Lock()
	defer r.shardMtx[bucket].Unlock()

	if r.meta.Load()&closed != 0 {
		return nil, ErrManagerClosed
	}

	old, exists := r.conns[bucket].Lookup(deviceID)
	if !exists {
		if err := r.reserveSlot(); err != nil {
			return nil, err
		}
	}

	conn.SetContext(session)
	r.conns[bucket].Set(deviceID, conn)
	if !exists || old == conn {
		return nil, nil
	}

	oldSession, ok := old.Context().(*Session)
	if ok {
		oldSession.close()
	}

	return old, nil
}

// Remove closes and removes conn if this call wins its session's close
// transition. A replaced connection has already released its slot and is a
// no-op here.
func (r *connectionRegistry) Remove(conn nio.Conn) (*Session, bool) {
	session, ok := conn.Context().(*Session)
	if !ok {
		return nil, false
	}

	bucket := r.bucketFor(session.deviceID)
	r.shardMtx[bucket].Lock()
	defer r.shardMtx[bucket].Unlock()

	if !session.close() {
		return nil, false
	}

	current, exists := r.conns[bucket].Lookup(session.deviceID)
	if exists && current == conn {
		r.conns[bucket].Delete(session.deviceID)
	}
	r.releaseSlot()

	return session, true
}

// Lookup returns the currently registered connection for the device, if any.
// TableC2 synchronizes reads and resizing internally; shardMtx only serializes
// multi-step registry mutations and is not required for this single lookup.
func (r *connectionRegistry) Lookup(deviceID device.ID) (nio.Conn, bool) {
	return r.conns[r.bucketFor(deviceID)].Lookup(deviceID)
}

// MaxConns returns the configured registry capacity.
func (r *connectionRegistry) MaxConns() uint32 {
	return r.maxConns
}

// CurConns returns the actual tracked connection count, including while closed.
func (r *connectionRegistry) CurConns() uint32 {
	return r.meta.Load() &^ closed
}

// Close marks the registry closed under the lifecycle lock so no new replacement can enter.
func (r *connectionRegistry) Close() {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	r.meta.Or(closed)
}

// reserveSlot atomically claims capacity, rejecting admission when full or closed.
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

// releaseSlot returns one previously reserved connection slot.
func (r *connectionRegistry) releaseSlot() {
	r.meta.Add(^uint32(0))
}

// bucketFor hashes the device ID to its registry shard and transition lock.
func (r *connectionRegistry) bucketFor(deviceID device.ID) int {
	return int(bit.Mix64(deviceID.Uint64())) & mask
}
