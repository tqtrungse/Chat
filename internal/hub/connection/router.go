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
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync/atomic"
	"time"

	pbpub "xxx/api/hub/v1/proto/gen/pub"
	"xxx/internal/hub/domain/device"
	"xxx/internal/hub/domain/session"
	"xxx/internal/hub/protocol"

	"xxx/pkg"
	"xxx/pkg/collection/swiss"
	"xxx/pkg/hash"
	"xxx/pkg/log"
	"xxx/pkg/nio"
	workerpool "xxx/pkg/pool/worker"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type Op uint8

const (
	nShards = 8
	mask    = nShards - 1
	closed  = uint32(1) << 31

	addDeviceOp Op = 1
	delDeviceOp Op = 2

	defaultTickerDuration = time.Second
	defaultMaxConns       = 128000
)

type SendData struct {
	PackType pbpub.PacketType
	Msg      proto.Message
	Expand   []byte
}

// Router is a plain connection registry: it tracks live connections per
// shard and drives their lifecycle (create/activate/remove/reap).
type Router struct {
	meta atomic.Uint32
	_    [pkg.CacheLineSize - 4]byte

	signer         Signer
	tickets        session.TicketSealer
	tickerDuration time.Duration
	maxConns       uint32
	logger         *log.Logger
	pool           *workerpool.Pool
	encoder        *protocol.Encoder
	presence       *presenceSync
	conns          [nShards]*swiss.TableC2[device.ID, nio.Conn]
}

func NewRouter(
	rootCtx context.Context,
	cfg Config,
	signer Signer,
	tickets session.TicketSealer,
	dCache DistributedCache,
	cb CircuitBreaker,
	logger *log.Logger,
	pool *workerpool.Pool,
	encoder *protocol.Encoder,
) *Router {
	r := &Router{
		signer:         signer,
		tickets:        tickets,
		tickerDuration: cfg.Router.TickerDuration,
		logger:         logger,
		pool:           pool,
		encoder:        encoder,
		maxConns:       cfg.Router.MaxConns,
	}

	if r.tickerDuration <= 0 {
		r.tickerDuration = defaultTickerDuration
	}
	if r.maxConns == 0 {
		r.maxConns = defaultMaxConns
	}

	r.presence = newPresenceSync(
		rootCtx,
		cfg.PresenceSync,
		dCache,
		cb,
		logger,
		pool,
	)

	nItemsPerShard := math.Ceil(float64(r.maxConns / nShards))
	for i := range nShards {
		r.conns[i] = swiss.NewTableC2WithCap[device.ID, nio.Conn](
			int(nItemsPerShard),
			hash.Int64,
		)
	}
	return r
}

func (r *Router) ActivateConn(
	ctx context.Context,
	token, ticket, sign []byte,
	conn nio.Conn,
	callback func(id device.ID),
) error {
	deviceID := device.ID(binary.LittleEndian.Uint64(token))

	t, err := r.tickets.Open(deviceID.Uint64(), ticket)
	if err != nil {
		return err
	}
	defer func() {
		clear(t.Keys.RecvEnc[:])
		clear(t.Keys.SendEnc[:])
		clear(t.Keys.RecvMac[:])
		clear(t.Keys.SendMac[:])
	}()

	// The signature covers the ticket bytes: it's bound to this exchange
	// (keys + expiry) and can't be replayed against another one.
	if err = r.signer.Verify(t.IdentityPub[:], ticket, sign); err != nil {
		return err
	}

	// Reserve a slot first; give it back if we end up replacing a conn.
	meta := r.meta.Add(1)
	if meta&closed != 0 {
		r.meta.Add(^uint32(0))
		return ErrRouterClosed
	}
	if meta&^closed > r.maxConns {
		r.meta.Add(^uint32(0))
		return ErrConnReachMax
	}

	ss := &session.Data{Keys: t.Keys, DeviceID: deviceID}
	timestamp := time.Now().Unix()
	ss.LastHeartBeat.Store(timestamp)
	ss.State.Store(uint32(session.StateActive))

	conn.SetContext(ss)
	old, existence := r.conns[deviceID&mask].Set(deviceID, conn)
	switch {
	case existence && old == conn:
		r.meta.Add(^uint32(0)) // activated twice on the same conn
	case existence:
		// If we flip the old session to Closed first, RemoveConn(old) becomes a
		// no-op and its slot is inherited by the new conn. If RemoveConn won the
		// race, it already released the slot and ours stays reserved.
		if oss, ok := old.Context().(*session.Data); ok &&
			oss.State.Swap(uint32(session.StateClosed)) != uint32(session.StateClosed) {
			r.meta.Add(^uint32(0))
		}
		r.ForceClose(old)
	}

	// Sync device presence (device -> this hub) to cache.
	return r.pool.Submit(func(_ *workerpool.Context) {
		err := r.presence.Submit(
			ctx,
			deviceOp{
				DeviceID:  deviceID,
				Timestamp: timestamp,
				Op:        addDeviceOp,
			},
			func(_ any, err error) {
				if err == nil {
					callback(deviceID)
					return
				}
				r.logger.Error("failed to add connection to ledger", zap.Error(err))
				r.ForceClose(conn)
			},
		)
		if err != nil {
			r.logger.Error("failed to submit batch", zap.Error(err))
			r.ForceClose(conn)
		}
	})
}

func (r *Router) RemoveConn(ctx context.Context, conn nio.Conn) {
	// If connection has no context, it just connects and immediately closes while doesn't
	// send any package. It is not in map, so call connActivator.Deactivate is unnecessary.
	// We set SO_KEEPALIVE, so when it closed, it will be cleaned up by NIO.
	ss, ok := conn.Context().(*session.Data)
	// never activated
	if !ok {
		return
	}
	// Whoever flips Active->Closed owns the cleanup. If ActivateConn replaced
	// this connection it already flipped it, so we must not touch the map or counter.
	if ss.State.Swap(uint32(session.StateClosed)) == uint32(session.StateClosed) {
		return
	}
	if cur, ok := r.conns[ss.DeviceID&mask].Lookup(ss.DeviceID); ok && cur == conn {
		r.conns[ss.DeviceID&mask].Delete(ss.DeviceID)
	}
	r.meta.Add(^uint32(0))

	// Delete device from cache.
	timestamp := time.Now().Unix()
	err := r.pool.Submit(func(_ *workerpool.Context) {
		err := r.presence.Submit(
			ctx,
			deviceOp{
				DeviceID:  ss.DeviceID,
				Timestamp: timestamp,
				Op:        delDeviceOp,
			},
			func(_ any, err error) {
				// If delete device is failed, don't need to retry.
				// The connection is deleted from hub, just garbage record in cache.
				//
				// If another hub sends message to the deleted device (get garbage record in cache
				// to determine storage hub), it is solved as offline messages.
				//
				// When device reconnect, cache will remove garbage record and create new record.
				if err != nil && !errors.Is(err, errSuperseded) {
					r.logger.Error("failed to delete connection from ledger", zap.Error(err))
				}
			},
		)
		if err != nil {
			r.logger.Error("failed to submit batch", zap.Error(err))
		}
	})
	if err != nil {
		r.logger.Error("failed to worker pool submit", zap.Error(err))
	}
}

func (r *Router) Send(deviceID device.ID, data SendData) error {
	conn, existence := r.conns[deviceID&mask].Lookup(deviceID)
	if !existence {
		return ErrSessionNotFound
	}
	ss, ok := conn.Context().(*session.Data)
	if !ok || ss.State.Load() == uint32(session.StateClosed) {
		return ErrSessionClosed
	}
	return send(
		conn,
		&ss.Keys.SendMac,
		r.encoder,
		data.PackType,
		data.Msg,
		data.Expand,
	)
}

func (r *Router) MaxConns() uint32 {
	return r.maxConns
}

func (r *Router) CurConns() uint32 {
	currConns := r.meta.Load() &^ closed
	if currConns >= r.maxConns {
		return r.maxConns
	}
	return currConns
}

func (r *Router) Close() {
	r.meta.Or(closed)
}

func (r *Router) Tick() time.Duration {
	//for i := range nShards {
	//	r.conns[i].Range(func(deviceID device.ID, conn connection) (goOn bool) {
	//		conCtx := conn.Context()
	//		if conCtx == nil {
	//			return true
	//		}
	//
	//		ss := conCtx.(*session.Data)
	//		if ss.State.Load() == uint32(session.StateUnactive) {
	//			durationSecs := time.Now().Unix() - ss.LastHeartBeat.Load()
	//			if durationSecs >= int64(r.ttlUnactiveSession.Seconds()) {
	//				r.conns[i].Delete(deviceID)
	//				conn.SetContext(nil)
	//				r.meta.Add(^uint32(0))
	//			}
	//		}
	//		return true
	//	})
	//}
	return r.tickerDuration
}

func (r *Router) ForceClose(conn nio.Conn) {
	if err := conn.Close(); err != nil {
		r.logger.Error("failed to force close connection", zap.Error(err))
	} else {
		r.logger.Info("forces close connection")
	}
}
