/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package activateconn

import (
	"context"
	"time"

	"xxx/internal/hub/core/connection"
	"xxx/internal/hub/core/transport"
	"xxx/internal/shared/device"
	"xxx/internal/shared/ticket"

	"xxx/pkg/nio"
)

type ConnManager interface {
	CurConns() uint32
	MaxConns() uint32
	Tick() time.Duration
	// ActivateConn takes ownership of the channel and cleanup callback, even on
	// failure. Cleanup runs on the connection event loop on activation or close.
	ActivateConn(
		ctx context.Context,
		id device.ID,
		ch transport.Channel,
		conn nio.Conn,
		cleanup func(),
		callback func(error),
	) error
	CompleteActivation(conn nio.Conn, data connection.SendData) error
	RemoveConn(ctx context.Context, conn nio.Conn)
	ForceClose(conn nio.Conn)
}

// ActivationService must support concurrent verification with independent inputs.
type ActivationService interface {
	VerifyAuthorization(raw, signature, binding, challenge []byte, hub transport.Identity) (*ticket.Info, error)
	CheckExpiry(meta *ticket.Info, now time.Time) error
}

// AuthorizationExecutor submits CPU work without blocking the owning event loop.
// It must bound queued and running jobs and reject overload or shutdown.
type AuthorizationExecutor interface {
	Submit(job func()) error
}
