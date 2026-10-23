/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package presence

import (
	"context"

	"xxx/internal/hub/domain/message"
	"xxx/internal/shared/device"
)

type DistributedCache interface {
	// ReapHub removes this Hub's shared presence during shutdown. Call it once
	// while the shutdown context is still usable.
	ReapHub(ctx context.Context) error

	// BatchAddDevices registers this Hub as owner and returns pending message dates.
	// deviceIDs must contain unique device IDs.
	BatchAddDevices(ctx context.Context, deviceIDs []device.ID) (map[device.ID][]message.Date, error)

	// BatchDelDevices removes device ownership held by this Hub.
	// deviceIDs must contain unique device IDs.
	BatchDelDevices(ctx context.Context, deviceIDs []device.ID) error

	// ListHubsByDevices looks up the owning hubID for each deviceID.
	ListHubsByDevices(ctx context.Context, deviceIDs []device.ID) ([]uint64, error)

	// IsInfraError classifies failures that may be retried as infrastructure errors.
	IsInfraError(err error) bool
}
