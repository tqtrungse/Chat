/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package activateconn

import (
	"context"
	"time"

	"xxx/internal/hub/core/connection"
	shareddevice "xxx/internal/shared/device"
	sharedsession "xxx/internal/shared/session"
	sharedticket "xxx/internal/shared/ticket"

	"xxx/pkg/nio"
)

// ConnManager is the subset of connection.Manager the activation flow needs.
type ConnManager interface {
	CurConns() uint32
	MaxConns() uint32
	Tick() time.Duration
	ActivateConn(
		ctx context.Context,
		id shareddevice.ID,
		keys *sharedsession.Keys,
		conn nio.Conn,
		callback func(id shareddevice.ID),
	) error
	AsyncSendConn(secure bool, conn nio.Conn, data connection.SendData) error
	RemoveConn(ctx context.Context, conn nio.Conn)
	ForceClose(conn nio.Conn)
}

// ActivationService is the subset of activation.Service the activation flow needs.
type ActivationService interface {
	Prepare(token, ticket, sign []byte) (*sharedticket.Ticket, error)
	CheckExpiry(ticket *sharedticket.Ticket, now time.Time) error
}
