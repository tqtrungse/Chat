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

	"xxx/internal/hub/domain/device"
	"xxx/internal/hub/domain/message"
)

type DistributedCache interface {
	// ReapHub have to let set exactly one time when stop and before ctx done.
	ReapHub(ctx context.Context) error

	// BatchAddDevices
	//
	// deviceIDs must contain unique device IDs.
	BatchAddDevices(ctx context.Context, deviceIDs []device.ID) (map[device.ID][]message.Date, error)

	// BatchDelDevices
	//
	// deviceIDs must contain unique device IDs.
	BatchDelDevices(ctx context.Context, deviceIDs []device.ID) error

	// ListHubsByDevices looks up the owning hubID for each deviceID.
	ListHubsByDevices(ctx context.Context, deviceIDs []device.ID) ([]uint64, error)

	IsInfraError(err error) bool
}
