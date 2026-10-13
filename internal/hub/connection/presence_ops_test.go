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
	"errors"
	"testing"

	shareddevice "xxx/internal/shared/device"

	"xxx/pkg/batcher"

	"github.com/stretchr/testify/require"
)

func TestDedupeDeviceOps_KeepsNewestValidOperationPerDevice(t *testing.T) {
	var callbacks [8]int
	reqs := []batcher.Req[deviceOp]{
		{Data: deviceOp{DeviceID: 1, Timestamp: 2, Op: addDeviceOp}, OnResult: func(any, error) { callbacks[0]++ }},
		{Data: deviceOp{DeviceID: 1, Timestamp: 3, Op: delDeviceOp}, OnResult: func(any, error) { callbacks[1]++ }},
		{Data: deviceOp{DeviceID: 2, Timestamp: 5, Op: delDeviceOp}, OnResult: func(any, error) { callbacks[2]++ }},
		{Data: deviceOp{DeviceID: 2, Timestamp: 5, Op: addDeviceOp}, OnResult: func(any, error) { callbacks[3]++ }},
		{Data: deviceOp{DeviceID: 3, Timestamp: 9, Op: addDeviceOp}, OnResult: func(any, error) { callbacks[4]++ }},
		{Data: deviceOp{DeviceID: 3, Timestamp: 8, Op: delDeviceOp}, OnResult: func(any, error) { callbacks[5]++ }},
		{Data: deviceOp{DeviceID: 4, Timestamp: 10, Op: addDeviceOp}, OnResult: func(any, error) { callbacks[6]++ }},
		{Data: deviceOp{DeviceID: 4, Timestamp: 11, Op: Op(255)}, OnResult: func(any, error) { callbacks[7]++ }},
	}

	isLatest, adds, dels := dedupeDeviceOps(reqs)

	require.Equal(t, []bool{false, true, false, true, true, false, false, false}, isLatest)
	require.ElementsMatch(t, []shareddevice.ID{2, 3}, adds)
	require.ElementsMatch(t, []shareddevice.ID{1}, dels)

	resultErr := errors.New("cache write failed")
	sendResult(reqs, isLatest, addDeviceOp, resultErr)
	require.Equal(t, [8]int{0, 0, 0, 1, 1, 0, 0, 0}, callbacks)
	sendResult(reqs, isLatest, delDeviceOp, resultErr)
	require.Equal(t, [8]int{0, 1, 0, 1, 1, 0, 0, 0}, callbacks)
}

func TestSendResult_OnlyResolvesLatestRequestsForRequestedOperation(t *testing.T) {
	var callbacks [3]int
	reqs := []batcher.Req[deviceOp]{
		{Data: deviceOp{DeviceID: 1, Op: addDeviceOp}, OnResult: func(_ any, err error) { require.NoError(t, err); callbacks[0]++ }},
		{Data: deviceOp{DeviceID: 2, Op: addDeviceOp}, OnResult: func(_ any, err error) { require.ErrorIs(t, err, errSuperseded); callbacks[1]++ }},
		{Data: deviceOp{DeviceID: 2, Op: delDeviceOp}, OnResult: func(_ any, err error) { require.NoError(t, err); callbacks[2]++ }},
	}
	isLatest := []bool{true, false, true}

	sendResult(reqs, isLatest, addDeviceOp, nil)
	sendResult(reqs, isLatest, delDeviceOp, nil)

	require.Equal(t, [3]int{1, 0, 1}, callbacks)
}
