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

import "github.com/redis/go-redis/v9"

var (
	// deleteHubScript removes a hub from the hubs set and announces it.
	// Reused for hub de-registration, both graceful stop and crash reap.
	//
	// KEYS[1] = hubs set
	//
	// KEYS[2] = del_hub_event
	//
	// ARGV[1] = hubID (string)
	//
	// ARGV[2] = payload
	deleteHubScript = redis.NewScript(`
		local deleted = redis.call(
			"SREM",
			KEYS[1],
			ARGV[1]
		)
		
		if deleted == 1 then
			redis.call(
				"PUBLISH",
				KEYS[2],
				ARGV[2]
			)
		end
		
		return deleted
	`)

	// batchAddDevicesScript atomically registers devices to a hub and returns
	// the dates on which each device received messages while it was offline.
	//
	// It keeps the following state consistent:
	//   - devices:{bucket}: device -> owning hub
	//   - hub:devices:<hubID>: set of devices owned by the hub
	//
	// If a device previously belonged to another hub, it is removed from the
	// previous hub's device set.
	//
	// KEYS[1]                  = hub:devices:<hubID>
	// KEYS[2..N+1]             = devices:{bucket}
	//
	// KEYS[N+2..N+1+numDevices] = pending_buckets:<deviceID>, one per device,
	//                             in the SAME order devices appear across ARGV
	//
	// ARGV[1]                  = hubID
	// ARGV[2]                  = numBuckets (N)
	//
	// ARGV[3..]                = alternating in bucket order:
	//                            count, deviceID..., count, deviceID..., ...
	//
	// Returns:
	//   { changed, pending }
	//   pending[i] = {YYYYMMDD, YYYYMMDD, ...}
	batchAddDevicesScript = redis.NewScript(`
		local hubID = ARGV[1]
		local numBuckets = tonumber(ARGV[2])
		local argIdx = 3
		local changed = 0
		local deviceIdx = 0
		local pending = {}
	
		for b = 1, numBuckets do
		   local deviceKey = KEYS[1 + b]
		   local count = tonumber(ARGV[argIdx])
		   argIdx = argIdx + 1
	
		   for i = 1, count do
			  local deviceID = ARGV[argIdx]
			  argIdx = argIdx + 1
			  deviceIdx = deviceIdx + 1
	
			  local old = redis.call("HGET", deviceKey, deviceID)
			  if old ~= hubID then
				 if old then
					redis.call("SREM", "hub:devices:" .. old, deviceID)
				 end
				 redis.call("HSET", deviceKey, deviceID, hubID)
				 changed = changed + 1
			  end
	
			  redis.call("SADD", KEYS[1], deviceID)
	
			  -- Fetch the dates on which this device received messages while offline.
			  local pendingKey = KEYS[1 + numBuckets + deviceIdx]
			  local dates = redis.call("SMEMBERS", pendingKey)
			  pending[deviceIdx] = dates
		   end
		end
	
		return {changed, pending}
	`)

	// batchDelDeviceScript deletes devices from their owning
	// "devices:{bucket}" hash and the caller's "hub:devices:<hubID>" set,
	// but only for devices still owned by the given hubID (a device may
	// have already been re-claimed by a different hub via
	// batchAddDevicesScript in the meantime, in which case it's left
	// alone). Announces each successful deletion individually, in the same
	// 16-byte wire format the single-device version used, so
	// onDelDeviceEvent needs no changes.
	//
	// The PUBLISH payload for each device is precomputed in Go and passed
	// straight through as an opaque ARGV blob rather than built here in
	// Lua: deviceID is a full uint64, and Lua 5.1's numbers are
	// double-precision floats, which start losing integer precision above
	// 2^53 — packing it here would silently corrupt large device IDs.
	//
	// KEYS[1]      = eventDelDevice channel
	//
	// KEYS[2]      = hub:devices:<hubID>
	//
	// KEYS[3..]    = devices:{bucket} for each distinct bucket present in ARGV
	//
	// ARGV[1]      = hubID (expected owner, string)
	//
	// ARGV[2]      = numBuckets (N)
	//
	// ARGV[3..]    = alternating in the correct order KEYS[3..N+2]:
	//                [count bucket 1, deviceID, payload, deviceID, payload, ...,
	//                 count bucket 2, deviceID, payload, ...]
	//                deviceID is the decimal-string hash field; payload is
	//                the precomputed 16-byte (deviceID+hubID) PUBLISH
	//                payload for that device.
	//
	// Returns the number of devices actually deleted.
	batchDelDeviceScript = redis.NewScript(`
		local hubID = ARGV[1]
		local numBuckets = tonumber(ARGV[2])
		local argIdx = 3
		local removed = 0

		for b = 1, numBuckets do
			local deviceKey = KEYS[2 + b]
			local count = tonumber(ARGV[argIdx])
			argIdx = argIdx + 1

			for i = 1, count do
				local deviceID = ARGV[argIdx]
				local payload = ARGV[argIdx + 1]
				argIdx = argIdx + 2

				local owner = redis.call("HGET", deviceKey, deviceID)
				if owner == hubID then
					redis.call("HDEL", deviceKey, deviceID)
					redis.call("SREM", KEYS[2], deviceID)
					redis.call(
						"PUBLISH",
						KEYS[1],
						payload
					)
					removed = removed + 1
				end
			end
		end

		return removed
	`)

	// reapDevicesInBucketScript conditionally deletes a batch of devices
	// from one "devices:{bucket}" hash, but only for those still owned by
	// the given hubID (a device may have already been re-claimed by a live
	// hub via addDeviceScript/addDevicesScript in the meantime, in which
	// case it's left alone). The caller (reapHub) first SPOPs a batch from
	// the crashed hub's device set in Go, groups the results by bucket, then
	// runs this once per bucket group. Returns the number removed.
	//
	// KEYS[1] = devices:{bucket}
	//
	// KEYS[2] = hub:devices:<hubID>
	//
	// ARGV[1] = hubID (string)
	//
	// ARGV[2..] = deviceIDs (all hashing into this bucket, already popped
	//             from the hub's device set)
	reapDevicesInBucketScript = redis.NewScript(`
		local hubID = ARGV[1]
		local removed = 0

		for i = 2, #ARGV do
			local deviceID = ARGV[i]
			local owner = redis.call("HGET", KEYS[1], deviceID)
			if owner == hubID then
				redis.call("HDEL", KEYS[1], deviceID)
				removed = removed + 1
			end
			redis.call("SREM", KEYS[2], deviceID)
		end

		return removed
	`)
)
