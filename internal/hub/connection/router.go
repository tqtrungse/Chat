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

	"xxx/internal/hub/protocol"
	shareddevice "xxx/internal/shared/device"
	sharedsession "xxx/internal/shared/session"
	sharedticket "xxx/internal/shared/ticket"

	"xxx/pkg/log"
	"xxx/pkg/nio"
	workerpool "xxx/pkg/pool/worker"
)

// Router is the public connection manager facade. Registry, lifecycle,
// outbound sending, and presence synchronization live in separate components.
type Router struct {
	registry  *connectionRegistry
	lifecycle *connectionLifecycle
	sender    *connectionSender
}

func NewRouter(
	rootCtx context.Context,
	cfg Config,
	signer Signer,
	ticketVerifier TicketVerifier,
	dCache DistributedCache,
	cb CircuitBreaker,
	logger *log.Logger,
	pool *workerpool.Pool,
	encoder *protocol.Encoder,
) *Router {
	registry := newConnectionRegistry(cfg.Router.MaxConns)
	sender := newConnectionSender(registry, encoder, logger)
	presence := newPresenceSync(
		rootCtx,
		cfg.PresenceSync,
		dCache,
		cb,
		logger,
		pool,
	)
	lifecycle := newConnectionLifecycle(
		signer,
		ticketVerifier,
		cfg.Router.TickerDuration,
		registry,
		logger,
		pool,
		presence,
	)
	return &Router{
		registry:  registry,
		lifecycle: lifecycle,
		sender:    sender,
	}
}

// ActivateConn registers conn for ticket.DeviceID. The ticket must come from
// PrepareActivation; keys must be derived for that activation challenge. The
// caller retains ownership of ticket and keys and must clear them afterward.
func (r *Router) ActivateConn(
	ctx context.Context,
	ticket *sharedticket.Ticket,
	keys *sharedsession.Keys,
	conn nio.Conn,
	callback func(id shareddevice.ID),
) error {
	return r.lifecycle.ActivateConn(ctx, ticket, keys, conn, callback)
}

// PrepareActivation opens the sealed ticket and verifies the device
// signature without registering the connection.
func (r *Router) PrepareActivation(token, ticket, sign []byte) (*sharedticket.Ticket, error) {
	return r.lifecycle.PrepareActivation(token, ticket, sign)
}

func (r *Router) RemoveConn(ctx context.Context, conn nio.Conn) {
	r.lifecycle.RemoveConn(ctx, conn)
}

// AsyncSend encodes and queues a frame for the current connection of deviceID.
func (r *Router) AsyncSend(secure bool, deviceID shareddevice.ID, data SendData) error {
	return r.sender.AsyncSend(secure, deviceID, data)
}

// AsyncSendConn targets one activation so a delayed callback cannot send to a
// replacement connection for the same device.
func (r *Router) AsyncSendConn(secure bool, conn nio.Conn, data SendData) error {
	return r.sender.AsyncSendConn(secure, conn, data)
}

func (r *Router) MaxConns() uint32 {
	return r.registry.MaxConns()
}

func (r *Router) CurConns() uint32 {
	return r.registry.CurConns()
}

func (r *Router) Close() {
	r.registry.Close()
}

func (r *Router) Tick() time.Duration {
	return r.lifecycle.Tick()
}

func (r *Router) ForceClose(conn nio.Conn) {
	r.lifecycle.ForceClose(conn)
}
