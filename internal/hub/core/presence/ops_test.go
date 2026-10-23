/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package presence

import (
	"errors"
	"testing"

	"xxx/internal/shared/device"

	"xxx/pkg/batcher"

	"github.com/stretchr/testify/require"
)

func TestDedupeDeviceOps_KeepsNewestValidOperationPerDevice(t *testing.T) {
	var callbacks [8]int
	reqs := []batcher.Req[deviceOp]{
		batcher.NewReq(deviceOp{DeviceID: 1, Timestamp: 2, Op: addDeviceOp}, func(any, error) { callbacks[0]++ }),
		batcher.NewReq(deviceOp{DeviceID: 1, Timestamp: 3, Op: delDeviceOp}, func(any, error) { callbacks[1]++ }),
		batcher.NewReq(deviceOp{DeviceID: 2, Timestamp: 5, Op: delDeviceOp}, func(any, error) { callbacks[2]++ }),
		batcher.NewReq(deviceOp{DeviceID: 2, Timestamp: 5, Op: addDeviceOp}, func(any, error) { callbacks[3]++ }),
		batcher.NewReq(deviceOp{DeviceID: 3, Timestamp: 9, Op: addDeviceOp}, func(any, error) { callbacks[4]++ }),
		batcher.NewReq(deviceOp{DeviceID: 3, Timestamp: 8, Op: delDeviceOp}, func(any, error) { callbacks[5]++ }),
		batcher.NewReq(deviceOp{DeviceID: 4, Timestamp: 10, Op: addDeviceOp}, func(any, error) { callbacks[6]++ }),
		batcher.NewReq(deviceOp{DeviceID: 4, Timestamp: 11, Op: op(255)}, func(any, error) { callbacks[7]++ }),
	}

	isLatest, adds, dels := dedupeDeviceOps(reqs)
	require.Equal(t, []bool{false, true, false, true, true, false, false, false}, isLatest)
	require.ElementsMatch(t, []device.ID{2, 3}, adds)
	require.ElementsMatch(t, []device.ID{1}, dels)

	resultErr := errors.New("cache write failed")
	sendResult(reqs, isLatest, addDeviceOp, resultErr)
	require.Equal(t, [8]int{0, 0, 0, 1, 1, 0, 0, 0}, callbacks)

	sendResult(reqs, isLatest, delDeviceOp, resultErr)
	require.Equal(t, [8]int{0, 1, 0, 1, 1, 0, 0, 0}, callbacks)
}

func TestSendResult_OnlyResolvesLatestRequestsForRequestedOperation(t *testing.T) {
	var callbacks [3]int
	reqs := []batcher.Req[deviceOp]{
		batcher.NewReq(deviceOp{DeviceID: 1, Op: addDeviceOp}, func(_ any, err error) { require.NoError(t, err); callbacks[0]++ }),
		batcher.NewReq(deviceOp{DeviceID: 2, Op: addDeviceOp}, func(_ any, err error) { require.ErrorIs(t, err, errSuperseded); callbacks[1]++ }),
		batcher.NewReq(deviceOp{DeviceID: 2, Op: delDeviceOp}, func(_ any, err error) { require.NoError(t, err); callbacks[2]++ }),
	}
	isLatest := []bool{true, false, true}
	sendResult(reqs, isLatest, addDeviceOp, nil)
	sendResult(reqs, isLatest, delDeviceOp, nil)
	require.Equal(t, [3]int{1, 0, 1}, callbacks)
}
