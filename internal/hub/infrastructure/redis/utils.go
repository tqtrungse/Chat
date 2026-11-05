/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package redis

import (
	"strconv"
	"strings"
)

func arrAnyToU64s(values []any) []uint64 {
	var (
		err    error
		result = make([]uint64, len(values))
	)

	for i, value := range values {
		if value == nil {
			result[i] = 0
			continue
		}

		result[i], err = strconv.ParseUint(value.(string), 10, 64)
		if err != nil {
			result[i] = 0
		}
	}
	return result
}

func parseHeartbeatKey(key string) (uint64, bool) {
	if !strings.HasPrefix(key, hubHeartbeatPrefix) {
		return 0, false
	}

	id, err := strconv.ParseUint(key[len(hubHeartbeatPrefix):], 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

//// ChunkDeviceIDsByBucket splits deviceIDs into chunks for
//// BatchAddDevices. Every chunk holds devices from a single bucket
//// only, using the same bucketCount as your Config.DeviceBucketCount.
//// No chunk holds more than chunkSize devices. BatchAddDevices does
//// not chunk its input itself. A Lua call blocks the single-threaded
//// event loop of Redis for its full duration, so pass a chunkSize
//// that you are comfortable blocking Redis for, at one time, for
//// example a few hundred.
////
//// The order matters: bucket first, then size. Chunking by size alone
//// can span many buckets in one slice. This would make BatchAddDevices
//// open far more Lua calls than it needs for the same input, because
//// it issues one call per distinct bucket that it finds in whatever
//// you pass it.
////
//// The order of the returned chunks across buckets is not defined; it
//// follows map iteration order. This is fine, because inserts across
//// buckets do not depend on order.
//func ChunkDeviceIDsByBucket(deviceIDs []entity.DeviceID, bucketCount uint16, chunkSize int) [][]entity.DeviceID {
//	if len(deviceIDs) == 0 || chunkSize <= 0 {
//		return nil
//	}
//
//	byBucket := make(map[uint32][]entity.DeviceID)
//	for _, id := range deviceIDs {
//		bucket := deviceBucketFor(id, bucketCount)
//		byBucket[bucket] = append(byBucket[bucket], id)
//	}
//
//	chunks := make([][]entity.DeviceID, 0, len(deviceIDs)/chunkSize+len(byBucket))
//	for _, ids := range byBucket {
//		for start := 0; start < len(ids); start += chunkSize {
//			end := start + chunkSize
//			if end > len(ids) {
//				end = len(ids)
//			}
//			chunks = append(chunks, ids[start:end])
//		}
//	}
//
//	return chunks
//}
