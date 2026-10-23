/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package http

import (
	"context"

	"xxx/api/iam/v2/json"
)

type TicketIssuer interface {
	// Issue authorizes the subject and device and returns a signed ticket with the
	// Hub descriptor, transferring ownership of the response Ticket buffer on success.
	// The caller must clear and return Ticket to slicepool after consuming it.
	Issue(context.Context, string, json.ConnectionTicketReq) (*json.ConnectionTicketResp, error)
}
