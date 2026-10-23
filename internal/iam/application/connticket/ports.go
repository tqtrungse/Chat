/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connticket

import (
	"context"

	"xxx/internal/shared/device"
	"xxx/internal/shared/ticket"
)

type DeviceMeta struct {
	ExternalID  string
	State       device.State
	IdentityPub []byte
}

type DeviceMetaReader interface {
	// FindDeviceMeta loads the device owner, state, and identity key from the
	// database context, returning nil, nil when no device is found.
	FindDeviceMeta(dbCtx context.Context, deviceID device.ID) (*DeviceMeta, error)
}

type TicketSigner interface {
	// Sign validates and signs the ticket claims, transferring ownership of the
	// returned buffer to the caller on success. The caller must clear it and
	// return it to slicepool after use. On error, no buffer is returned.
	Sign(info *ticket.Info) ([]byte, error)
}
