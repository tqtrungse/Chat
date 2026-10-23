/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package activation

import (
	"time"

	"xxx/internal/hub/core/transport"
	"xxx/internal/shared/ticket"
)

type TicketVerifier interface {
	// Verify authenticates IAM's signature and validates claims, expiry and device
	// identity against a trusted, previously validated local Hub identity.
	// Implementations must support concurrent calls with independently owned inputs.
	Verify(wire []byte, expected transport.Identity) (*ticket.Info, error)

	// CheckExpiry validates the ticket time window at the supplied time.
	CheckExpiry(info *ticket.Info, now time.Time) error
}

type Signer interface {
	// Verify checks the device signature over the supplied proof transcript.
	Verify(identityPub, payload, signature []byte) error
}
