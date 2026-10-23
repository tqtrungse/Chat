/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connection

import (
	"context"

	"xxx/internal/shared/device"
)

// PresencePublisher publishes device presence changes to the shared cache.
// Both methods must be non-blocking: they enqueue and return. onResult is
// called exactly once, from another goroutine, unless the call returns an error.
type PresencePublisher interface {
	// Add announces that this hub now owns the device.
	Add(ctx context.Context, id device.ID, ts int64, onResult func(any, error)) error

	// Del announces that this hub no longer holds the device.
	Del(ctx context.Context, id device.ID, ts int64, onResult func(error)) error
}
