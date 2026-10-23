/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connection

import (
	"context"
	"sync"
	"time"

	"xxx/internal/hub/core/protocol"
	"xxx/internal/hub/core/transport"
	"xxx/internal/shared/device"

	"xxx/pkg/log"
	"xxx/pkg/nio"
	slicepool "xxx/pkg/pool/slice"

	"go.uber.org/zap"
)

type Manager struct {
	transitions    [nShards]sync.Mutex
	presenceQueue  presenceQueue
	presence       PresencePublisher
	tickerDuration time.Duration
	registry       *connectionRegistry
	sender         *connectionSender
	logger         *log.Logger
}

// NewManager creates the connection registry, sender, and per-device presence queue.
func NewManager(
	cfg Config,
	presence PresencePublisher,
	logger *log.Logger,
	encoder *protocol.Encoder,
) *Manager {
	registry := newConnectionRegistry(cfg.MaxConns)
	return &Manager{
		presence:       presence,
		tickerDuration: cfg.TickerDuration,
		registry:       registry,
		sender:         newConnectionSender(registry, encoder, logger),
		logger:         logger,
	}
}

// ActivateConn consumes channel and cleanup ownership, even on failure. Cleanup
// runs on the owning event loop after activation succeeds or the session is
// cleared. Registration alone does not permit business sends before ACK.
func (m *Manager) ActivateConn(
	ctx context.Context,
	id device.ID,
	ch transport.Channel,
	conn nio.Conn,
	cleanup func(),
	callback func(error),
) error {
	session := newSession(ch, id, time.Now().Unix())
	session.activationCleanup = cleanup

	transition := &m.transitions[m.registry.bucketFor(id)]
	transition.Lock()
	if !m.presenceQueue.canAdd(id) {
		transition.Unlock()
		session.clear()
		return ErrPresenceBusy
	}

	old, err := m.registry.Replace(id, conn, session)
	if err != nil {
		transition.Unlock()
		session.clear()
		return err
	}

	start := m.presenceQueue.append(id, func(done func()) error {
		// A queued registration can become obsolete before reaching Redis.
		current, ok := m.registry.Lookup(id)
		if !ok || current != conn || session.isClosed() {
			done()
			return nil
		}

		resultCallback := func(result error) {
			defer done()
			enqueueErr := conn.EventLoop().Execute(context.WithoutCancel(ctx), func(context.Context) error {
				currConn, existence := m.registry.Lookup(id)
				if !existence ||
					currConn != conn ||
					conn.Context() != session ||
					session.isClosed() {
					return nil
				}

				if result != nil {
					callback(result)
					forceCloseConnection(m.logger, conn)
					return nil
				}

				callback(nil)
				return nil
			})
			if enqueueErr != nil {
				forceCloseConnection(m.logger, conn)
			}
		}

		addErr := m.presence.Add(
			ctx,
			id,
			session.lastHeartBeat.Load(),
			func(_ any, result error) {
				resultCallback(result)
			},
		)
		if addErr != nil {
			resultCallback(addErr)
		}
		return addErr
	})
	transition.Unlock()

	if old != nil {
		forceCloseConnection(m.logger, old)
	}

	if start != nil {
		err = start()
	}

	if err != nil {
		forceCloseConnection(m.logger, conn)
	}

	return err
}

// CompleteActivation must be called on conn's event loop after final deadline
// and ticket checks. ACK is queued before state becomes visible as ACTIVE.
func (m *Manager) CompleteActivation(conn nio.Conn, data SendData) error {
	s, ok := conn.Context().(*Session)
	if !ok || s.state.Load() != uint32(sessionStateRegistering) {
		return ErrSessionClosed
	}

	current, ok := m.registry.Lookup(s.deviceID)
	if !ok || current != conn {
		return ErrSessionClosed
	}

	payload, err := m.sender.encoder.EncodePayload(
		data.MsgKind,
		data.Msg,
		data.Expand,
	)
	if err != nil {
		return err
	}
	defer func() {
		clear(payload[:cap(payload)])
		slicepool.Put(payload)
	}()

	err = writeRecord(conn, s, payload)
	if err != nil {
		forceCloseConnection(m.logger, conn)
		return err
	}

	// A replacement on another loop must not be resurrected after the write.
	if !s.state.CompareAndSwap(uint32(sessionStateRegistering), uint32(sessionStateActive)) {
		return ErrSessionClosed
	}
	s.releaseActivation()
	return nil
}

// RemoveConn releases the session on its owning loop and queues presence removal
// only if this connection still owns the device registration.
func (m *Manager) RemoveConn(ctx context.Context, conn nio.Conn) {
	// OnClose owns channel cleanup, even after replacement marked state closed.
	owned, ok := conn.Context().(*Session)
	if !ok {
		return
	}
	defer owned.clear()

	transition := &m.transitions[m.registry.bucketFor(owned.deviceID)]
	transition.Lock()
	session, removed := m.registry.Remove(conn)
	if !removed {
		transition.Unlock()
		return
	}

	start := m.presenceQueue.append(session.deviceID, func(done func()) error {
		err := m.presence.Del(
			context.WithoutCancel(ctx),
			session.deviceID,
			time.Now().Unix(),
			func(err error) {
				defer done()
				if err != nil {
					m.logger.Error("failed to delete presence", zap.Error(err))
				}
			},
		)
		if err != nil {
			m.logger.Error("failed to enqueue presence removal", zap.Error(err))
			done()
		}
		return err
	})
	transition.Unlock()

	if start != nil {
		_ = start()
	}
}

// AsyncSend queues a message for the current active connection of the device.
// Success means the loop accepted the work, not that the peer received the message.
func (m *Manager) AsyncSend(ctx context.Context, id device.ID, data SendData) error {
	return m.sender.AsyncSend(ctx, id, data)
}

// AsyncSendConn queues a message for the specified active connection, dropping
// the queued send if the connection is closed or replaced before execution.
func (m *Manager) AsyncSendConn(ctx context.Context, conn nio.Conn, data SendData) error {
	return m.sender.AsyncSendConn(ctx, conn, data)
}

// MaxConns returns the configured connection capacity.
func (m *Manager) MaxConns() uint32 {
	return m.registry.MaxConns()
}

// CurConns returns the actual number of registered connections.
func (m *Manager) CurConns() uint32 {
	return m.registry.CurConns()
}

// Tick returns the delay requested before the next server tick.
func (m *Manager) Tick() time.Duration {
	return m.tickerDuration
}

// Close prevents new registrations; existing sessions still require connection cleanup.
func (m *Manager) Close() {
	m.registry.Close()
}

// ForceClose requests connection closure and logs any failure to submit it.
func (m *Manager) ForceClose(conn nio.Conn) {
	forceCloseConnection(m.logger, conn)
}

// forceCloseConnection requests closure through the connection API and logs errors.
func forceCloseConnection(logger *log.Logger, conn nio.Conn) {
	if err := conn.Close(); err != nil {
		logger.Error("failed to close connection", zap.Error(err))
	}
}
