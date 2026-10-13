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
	"time"

	sharedevice "xxx/internal/shared/device"
	sharedsession "xxx/internal/shared/session"
	sharedticket "xxx/internal/shared/ticket"

	"xxx/pkg/log"
	"xxx/pkg/nio"
	workerpool "xxx/pkg/pool/worker"

	"go.uber.org/zap"
)

const (
	defaultTickerDuration = time.Second
	tokenSize             = 8
)

// connectionLifecycle coordinates activation and removal across the registry
// and shared presence cache.
type connectionLifecycle struct {
	signer         Signer
	ticketVerifier TicketVerifier
	tickerDuration time.Duration
	registry       *connectionRegistry
	logger         *log.Logger
	pool           *workerpool.Pool
	presence       *presenceSync
}

func newConnectionLifecycle(
	signer Signer,
	ticketVerifier TicketVerifier,
	tickerDuration time.Duration,
	registry *connectionRegistry,
	logger *log.Logger,
	pool *workerpool.Pool,
	presence *presenceSync,
) *connectionLifecycle {
	if tickerDuration <= 0 {
		tickerDuration = defaultTickerDuration
	}
	return &connectionLifecycle{
		signer:         signer,
		ticketVerifier: ticketVerifier,
		tickerDuration: tickerDuration,
		registry:       registry,
		logger:         logger,
		pool:           pool,
		presence:       presence,
	}
}

// ActivateConn registers conn for ticket.DeviceID and asynchronously
// publishes the presence change. The caller retains ownership of ticket and
// keys and must clear them after this method returns.
func (l *connectionLifecycle) ActivateConn(
	ctx context.Context,
	ticket *sharedticket.Ticket,
	keys *sharedsession.Keys,
	conn nio.Conn,
	callback func(id sharedevice.ID),
) error {
	if err := l.ticketVerifier.CheckExpiry(ticket, time.Now()); err != nil {
		return err
	}

	deviceID := sharedevice.ID(ticket.DeviceID)
	timestamp := time.Now().Unix()
	session := newSession(*keys, deviceID, timestamp)
	old, err := l.registry.Replace(deviceID, conn, session)
	if err != nil {
		session.clear()
		return err
	}
	if old != nil {
		forceCloseConnection(l.logger, old)
	}

	return l.pool.Submit(func(_ *workerpool.Context) {
		submitErr := l.presence.Submit(
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
				l.logger.Error("failed to add connection to ledger", zap.Error(err))
				forceCloseConnection(l.logger, conn)
			},
		)
		if submitErr != nil {
			l.logger.Error("failed to submit batch", zap.Error(submitErr))
			forceCloseConnection(l.logger, conn)
		}
	})
}

// PrepareActivation opens the sealed ticket and verifies the device
// signature, but does not register the connection. The caller must clear the
// returned ticket and prove possession of its activation key before
// activating.
func (l *connectionLifecycle) PrepareActivation(token, ticket, sign []byte) (*sharedticket.Ticket, error) {
	if len(token) != tokenSize {
		return nil, ErrActivationInvalid
	}

	deviceID := binary.LittleEndian.Uint64(token)
	t, err := l.ticketVerifier.Open(deviceID, ticket)
	if err != nil {
		return nil, err
	}

	// The signature authenticates the device identity. Freshness is provided
	// separately by the per-connection challenge-response.
	if err = l.signer.Verify(t.IdentityPub[:], ticket, sign); err != nil {
		t.Clear()
		return nil, err
	}
	return t, nil
}

func (l *connectionLifecycle) RemoveConn(ctx context.Context, conn nio.Conn) {
	session, removed := l.registry.Remove(conn)
	if !removed {
		return
	}

	deviceID := session.deviceID
	timestamp := time.Now().Unix()
	err := l.pool.Submit(func(_ *workerpool.Context) {
		err := l.presence.Submit(
			ctx,
			deviceOp{
				DeviceID:  deviceID,
				Timestamp: timestamp,
				Op:        delDeviceOp,
			},
			func(_ any, err error) {
				// The connection is removed locally regardless of cache cleanup.
				if err != nil && !errors.Is(err, errSuperseded) {
					l.logger.Error("failed to delete connection from ledger", zap.Error(err))
				}
			},
		)
		if err != nil {
			l.logger.Error("failed to submit batch", zap.Error(err))
		}
	})
	if err != nil {
		l.logger.Error("failed to worker pool submit", zap.Error(err))
	}
}

func (l *connectionLifecycle) Tick() time.Duration {
	return l.tickerDuration
}

func (l *connectionLifecycle) ForceClose(conn nio.Conn) {
	forceCloseConnection(l.logger, conn)
}

func forceCloseConnection(logger *log.Logger, conn nio.Conn) {
	if err := conn.Close(); err != nil {
		logger.Error("failed to force close connection", zap.Error(err))
	} else {
		logger.Debug("forces close connection")
	}
}
