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
	"time"

	"xxx/pkg"
)

type Config struct {
	// Addr is the address formated as host:port
	Addr string

	// Password is an optional password. Must match the password specified in the
	// `requirepass` server configuration option (if connecting to a Redis 5.0 instance, or lower),
	// or the User Password when connecting to a Redis 6.0 instance, or greater,
	// that is using the Redis ACL system.
	Password string

	// PoolSize is the base number of socket connections.
	// Default is 10 connections per every available CPU as reported by runtime.GOMAXPROCS.
	// If there is not enough connections in the pool, new connections will be allocated in excess of PoolSize,
	// you can limit it through MaxActiveConns
	//
	// Default: 10 * runtime.GOMAXPROCS(0)
	PoolSize uint16

	// MaxIdleConns is the maximum number of idle connections.
	// The idle connections are not closed by default.
	//
	// Default: 0
	MaxIdleConns uint16

	// MaxActiveConns is the maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	// If the pool is full, the next call to Get() will block until a connection is released.
	//
	// Default: 0
	MaxActiveConns uint16

	// DB is the database to be selected after connecting to the server.
	DB uint16

	// HeartbeatInterval is how often this hub refreshes its liveness lease
	// in Redis.
	//
	// Defaults: 3s
	HeartbeatInterval time.Duration

	// HeartbeatTTL is how long a hub's liveness lease survives without a
	// refresh before other hubs consider it crashed.
	// Should be a few multiples of HeartbeatInterval so that a couple
	// of missed beats don't cause a false-positive crash detection.
	//
	// Default: 10s
	HeartbeatTTL time.Duration

	// ReapBatchSize caps how many devices are cleaned up per Lua call when
	// reaping a crashed hub, so a hub owning many devices doesn't block
	// Redis for too long in one shot.
	//
	// Default: 500
	ReapBatchSize uint16

	// DeviceBucketCount is how many bucket hashes ("devices:0".."devices:{N-1}")
	// the device registry is split across. Pick N so that
	// expectedMaxDevices / N stays comfortably under Redis's
	// hash-max-listpack-entries (default 128) — e.g. target ~100 fields per
	// bucket on average, since random distribution means some buckets will
	// sit above the average.
	//
	// IMPORTANT: treat this like a partition count — it's effectively fixed
	// once there's real data in Redis. Changing it changes which bucket
	// every deviceID maps to (bucket = deviceID % N), so existing entries
	// won't be found under the new bucket keys until a migration re-shards
	// them. If you must change it later, write a one-off job that reads
	// every "devices:*" bucket under the old N and re-writes each entry
	// into its bucket under the new N.
	//
	// Default: 1024
	DeviceBucketCount uint16

	// VerifyExpiredNotifications verifies that Redis keyspace notifications for
	// expired keys are enabled. It is required for handling hub heartbeat expiration
	// events.
	VerifyExpiredNotifications bool
}

func (c *Config) Load(loader pkg.ConfigLoader) {
	if loader == nil {
		return
	}
	c.Addr = loader.GetString("DISTRIBUTED_CACHE_ADDRESS")
	c.Password = loader.GetString("DISTRIBUTED_CACHE_PASSWORD")
	c.PoolSize = loader.GetUint16("DISTRIBUTED_CACHE_POOL_SIZE")
	c.MaxIdleConns = loader.GetUint16("DISTRIBUTED_CACHE_MAX_IDLE_CONNS")
	c.MaxActiveConns = loader.GetUint16("DISTRIBUTED_CACHE_MAX_ACTIVE_CONNS")
	c.DB = loader.GetUint16("DISTRIBUTED_CACHE_DB")
	c.HeartbeatInterval = loader.GetDuration("DISTRIBUTED_CACHE_HEARTBEAT_INTERVAL")
	c.HeartbeatTTL = loader.GetDuration("DISTRIBUTED_CACHE_HEARTBEAT_TTL")
	c.ReapBatchSize = loader.GetUint16("DISTRIBUTED_CACHE_REAP_BATCH_SIZE")
	c.DeviceBucketCount = loader.GetUint16("DISTRIBUTED_CACHE_DEVICE_BUCKET_COUNT")
	c.VerifyExpiredNotifications = loader.GetBool("DISTRIBUTED_CACHE_VERIFY_EVENT_EXPIRED")
}
