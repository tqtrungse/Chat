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

package exchange_key

import (
	"context"

	"xxx/internal/hub/domain/device"
)

type DeviceMeta struct {
	ExternalID  string
	State       device.State
	IdentityPub []byte
}

type DeviceMetaReader interface {
	FindDeviceMeta(dbCtx context.Context, deviceID device.ID) (*DeviceMeta, error)
}
