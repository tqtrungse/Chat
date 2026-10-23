/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package tcp

import (
	"context"
	"time"

	"xxx/api/hub/v2/proto/gen/pub"
	"xxx/pkg/nio"
)

// ConnectionActivator owns the connection lifecycle used by the TCP adapter.
type ConnectionActivator interface {
	Admit(ctx context.Context, conn nio.Conn) bool
	HandleFrame(ctx context.Context, conn nio.Conn, frame []byte) nio.Action
	Deactivate(ctx context.Context, conn nio.Conn) nio.Action
	Tick() (time.Duration, nio.Action)
}

type MessageSender interface {
	// Send receives an owned decoded message and E2EE expand bytes.
	Send(conn nio.Conn, msg *pub.SendMsgReq, expand []byte) nio.Action
}
