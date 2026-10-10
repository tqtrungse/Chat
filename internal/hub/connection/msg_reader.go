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

	"xxx/internal/hub/domain/message"
	shareddevice "xxx/internal/shared/device"
)

type MsgReader interface {
	// ListOfflineMsgs returns one page of offline messages for the given
	// device and day bucket.
	//
	// Pass a nil state to fetch the first page. To fetch the next page, pass the
	// state returned by the previous call, together with the same deviceID, dayBucket.
	// The state is an opaque cursor: callers must not inspect, modify  or persist
	// it beyond the paging session.
	//
	// The returned next state is empty (len(next) == 0) if and only if there are
	// no more pages. Keep requesting pages until that condition is met.
	//
	// A state that was not issued for the same deviceID and dayBucket results in an
	// error.
	ListOfflineMsgs(
		ctx context.Context,
		state []byte,
		deviceID shareddevice.ID,
		dayBucket message.Date,
	) (msgs []message.Offline, nextState []byte, err error)
}
