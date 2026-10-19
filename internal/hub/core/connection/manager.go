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
	"time"

	"xxx/internal/hub/core/protocol"
	sharedevice "xxx/internal/shared/device"
	sharedsession "xxx/internal/shared/session"

	"xxx/pkg/log"
	"xxx/pkg/nio"

	"go.uber.org/zap"
)

const defaultTickerDuration = time.Second

// Manager coordinates activation and removal across the registry
// and shared presence cache.
type Manager struct {
	presence       PresencePublisher
	tickerDuration time.Duration
	registry       *connectionRegistry
	sender         *connectionSender
	logger         *log.Logger
}

func NewManager(
	cfg Config,
	presence PresencePublisher,
	logger *log.Logger,
	encoder *protocol.Encoder,
) *Manager {
	if cfg.TickerDuration <= 0 {
		cfg.TickerDuration = defaultTickerDuration
	}

	registry := newConnectionRegistry(cfg.MaxConns)
	return &Manager{
		presence:       presence,
		tickerDuration: cfg.TickerDuration,
		registry:       registry,
		sender:         newConnectionSender(registry, encoder, logger),
		logger:         logger,
	}
}

// ActivateConn registers conn for ticket.DeviceID and asynchronously
// publishes the presence change. The caller retains ownership of ticket and
// keys and must clear them after this method returns.
func (m *Manager) ActivateConn(
	ctx context.Context,
	id sharedevice.ID,
	keys *sharedsession.Keys,
	conn nio.Conn,
	callback func(id sharedevice.ID),
) error {
	timestamp := time.Now().Unix()
	session := newSession(*keys, id, timestamp)
	old, err := m.registry.Replace(id, conn, session)
	if err != nil {
		session.clear()
		return err
	}
	if old != nil {
		forceCloseConnection(m.logger, old)
	}

	err = m.presence.Add(
		ctx,
		id,
		timestamp,
		func(_ any, err error) {
			if err == nil {
				callback(id)
				return
			}
			m.logger.Error("failed to add connection to ledger", zap.Error(err))
			forceCloseConnection(m.logger, conn)
		},
	)
	if err != nil {
		m.logger.Error("failed to do presence", zap.Error(err))
		forceCloseConnection(m.logger, conn)
	}
	return err
}

func (m *Manager) RemoveConn(ctx context.Context, conn nio.Conn) {
	session, removed := m.registry.Remove(conn)
	if !removed {
		return
	}

	deviceID := session.deviceID
	timestamp := time.Now().Unix()
	err := m.presence.Del(
		ctx,
		deviceID,
		timestamp,
		func(err error) {
			if err != nil {
				m.logger.Error("failed to delete connection from ledger", zap.Error(err))
			}
		},
	)
	if err != nil {
		m.logger.Error("failed to remove connection from ledger", zap.Error(err))
	}
}

// AsyncSend encodes and queues a frame for the current connection of deviceID.
func (m *Manager) AsyncSend(secure bool, deviceID sharedevice.ID, data SendData) error {
	return m.sender.AsyncSend(secure, deviceID, data)
}

// AsyncSendConn targets one activation so a delayed callback cannot send to a
// replacement connection for the same device.
func (m *Manager) AsyncSendConn(secure bool, conn nio.Conn, data SendData) error {
	return m.sender.AsyncSendConn(secure, conn, data)
}

func (m *Manager) MaxConns() uint32 {
	return m.registry.MaxConns()
}

func (m *Manager) CurConns() uint32 {
	return m.registry.CurConns()
}

func (m *Manager) Close() {
	m.registry.Close()
}

func (m *Manager) Tick() time.Duration {
	return m.tickerDuration
}

func (m *Manager) ForceClose(conn nio.Conn) {
	forceCloseConnection(m.logger, conn)
}

func forceCloseConnection(logger *log.Logger, conn nio.Conn) {
	if err := conn.Close(); err != nil {
		logger.Error("failed to force close connection", zap.Error(err))
	} else {
		logger.Debug("forces close connection")
	}
}
