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

//// ChunkDeviceIDsByBucket splits deviceIDs into chunks ready for
//// BatchAddDevices: every chunk holds devices from a single bucket only (using
//// the same bucketCount as your Config.DeviceBucketCount), and no chunk holds
//// more than chunkSize devices. BatchAddDevices does not chunk its input
//// itself — a Lua call blocks Redis's single-threaded event loop for its full
//// duration — so pass a chunkSize you're comfortable blocking Redis for at a
//// time (e.g. a few hundred).
////
//// Bucket-first, then size, matters: chunking by size alone can span many
//// buckets in one slice, which would make BatchAddDevices open far more Lua
//// calls than necessary for the same input, since it issues one call per
//// distinct bucket it finds in whatever you pass it.
////
//// The returned order of chunks across buckets is unspecified (map iteration
//// order); this is fine since inserts across buckets don't depend on order.
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
