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

package connection

import (
	"context"

	shareddevice "xxx/internal/shared/device"
)

// PresencePublisher publishes device presence changes to the shared cache.
// Both methods must be non-blocking: they enqueue and return. onResult is
// called exactly once, from another goroutine, unless the call returns an error.
type PresencePublisher interface {
	// Add announces that this hub now owns the device.
	Add(ctx context.Context, id shareddevice.ID, ts int64, onResult func(any, error)) error

	// Del announces that this hub no longer holds the device.
	Del(ctx context.Context, id shareddevice.ID, ts int64, onResult func(error)) error
}