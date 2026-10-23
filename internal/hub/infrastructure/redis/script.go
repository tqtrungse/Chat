/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package redis

import "github.com/redis/go-redis/v9"

var (
	// deleteHubScript removes a hub from the hubs set and announces the
	// removal. The code reuses this script for hub de-registration,
	// both for a graceful stop and for a crash reap.
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

	// batchAddDevicesScript registers devices to a hub, as one atomic
	// step. It also returns the dates on which each device received
	// messages while the device was offline.
	//
	// The script keeps the following state consistent:
	//   - devices:{bucket}: device -> owning hub
	//   - hub:devices:<hubID>: set of devices owned by the hub
	//
	// If a device belonged to another hub before, the script removes
	// the device from that hub's device set.
	//
	// KEYS[1]                  = hub:devices:<hubID>
	// KEYS[2..N+1]             = devices:{bucket}
	//
	// KEYS[N+2..N+1+numDevices] = pending_buckets:<deviceID>, one per
	//                             device, in the SAME order the devices
	//                             appear across ARGV
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

	// batchDelDeviceScript deletes devices from the owning
	// "devices:{bucket}" hash and from the caller's
	// "hub:devices:<hubID>" set. The script deletes only the devices
	// still owned by the given hubID. A different hub may have already
	// re-claimed a device through batchAddDevicesScript in the
	// meantime; in that case, the script leaves the device alone. The
	// script announces each successful deletion on its own, in the same
	// 16-byte wire format that the single-device version used. For this
	// reason, onDelDeviceEvent needs no change.
	//
	// Go precomputes the PUBLISH payload for each device. The payload
	// passes through as an opaque ARGV blob; the script does not build
	// it in Lua. deviceID is a full uint64. Lua 5.1 numbers are
	// double-precision floats, and they start to lose integer precision
	// above 2^53. Packing deviceID in Lua would silently corrupt large
	// device IDs.
	//
	// KEYS[1]      = eventDelDevice channel
	//
	// KEYS[2]      = hub:devices:<hubID>
	//
	// KEYS[3..]    = devices:{bucket} for each distinct bucket present
	//                in ARGV
	//
	// ARGV[1]      = hubID (expected owner, string)
	//
	// ARGV[2]      = numBuckets (N)
	//
	// ARGV[3..]    = alternating in the correct order KEYS[3..N+2]:
	//                [count bucket 1, deviceID, payload, deviceID, payload, ...,
	//                 count bucket 2, deviceID, payload, ...]
	//                deviceID is the decimal-string hash field. payload
	//                is the precomputed 16-byte (deviceID+hubID) PUBLISH
	//                payload for that device.
	//
	// Returns the number of devices that the script actually deleted.
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

	// reapDevicesInBucketScript deletes a batch of devices from one
	// "devices:{bucket}" hash, but only the devices still owned by the
	// given hubID. A live hub may have already re-claimed a device
	// through batchAddDevicesScript in the meantime; in that case, the
	// script leaves the mapping alone. The caller, reapHub, reads a
	// batch from the hub's device set with SRANDMEMBER in Go, groups
	// the results by bucket, then runs this script once per bucket
	// group.
	//
	// The script removes each device from the hub's set with SREM,
	// regardless of its current owner. SRANDMEMBER does not remove
	// members on selection, so devices stay available for a later reap
	// if processing fails before this script runs. Returns the number
	// of device mappings that the script actually deleted from the
	// hash.
	//
	// KEYS[1] = devices:{bucket}
	//
	// KEYS[2] = hub:devices:<hubID>
	//
	// ARGV[1] = hubID (string)
	//
	// ARGV[2..] = deviceIDs. All device IDs hash into this bucket. The
	//             caller selects them from the hub's device set without
	//             removing them.
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
