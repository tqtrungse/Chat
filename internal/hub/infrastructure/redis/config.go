/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package redis

import (
	"crypto/tls"
	"errors"
	"runtime"
	"strings"
	"time"

	"xxx/internal/hub/core/presence"

	"xxx/pkg/config"
)

const (
	defaultDialTimeout       = 5 * time.Second
	defaultDialIdle          = 30 * time.Second
	defaultDialInterval      = 5 * time.Second
	defaultDialKeepCount     = 3
	defaultReadTimeout       = 5 * time.Second
	defaultWriteTimeout      = 5 * time.Second
	defaultDeviceBucketCount = uint16(4096)
	defaultReapBatchSize     = uint16(presence.MaxBatchSize)
	defaultHeartbeatInterval = 3 * time.Second
	defaultHeartbeatTTL      = 10 * time.Second
	defaultPipelinePoolSize  = 10
)

type Config struct {
	// Addr is the address of the server. The format is host:port.
	Addr string

	// Password is an optional password.
	//
	// If you set a password, it must match the `requirepass` server
	// configuration option, for a connection to a Redis 5.0 instance or
	// lower. For a connection to a Redis 6.0 instance or higher that
	// uses the Redis ACL system, it must match the User Password.
	Password string

	// DialTimeout is the timeout for the creation of a new connection.
	//
	// Default: 5 seconds
	DialTimeout time.Duration

	// DialIdle is the time that a connection must stay idle before the
	// connection sends the first keep-alive probe.
	//
	// Default: 30s
	DialIdle time.Duration

	// DialInterval is the time between keep-alive probes.
	//
	// Default: 5s
	DialInterval time.Duration

	// DialKeepCount is the maximum number of keep-alive probes that can
	// stay unanswered before the connection drops.
	//
	// Default: 3
	DialKeepCount int

	// TLSConfig is the TLS configuration to use. When you set
	// TLSConfig, the client negotiates TLS.
	TLSConfig *tls.Config

	// ReadTimeout is the timeout for socket reads. If a read reaches
	// this timeout, the command fails with a timeout error instead of
	// blocking. Supported values:
	//
	//	- `-1`: no timeout. The read blocks forever.
	//	- `-2`: disables all calls to SetReadDeadline.
	//
	// Default: 5s
	ReadTimeout time.Duration

	// WriteTimeout is the timeout for socket writes. If a write reaches
	// this timeout, the command fails with a timeout error instead of
	// blocking. Supported values:
	//
	//	- `-1`: no timeout. The write blocks forever.
	//	- `-2`: disables all calls to SetWriteDeadline.
	//
	// Default: 5s
	WriteTimeout time.Duration

	// PoolSize is the base number of socket connections.
	//
	// If the pool does not have enough connections, the client
	// allocates new connections beyond PoolSize. You can limit this
	// through MaxActiveConns.
	//
	// Default: 10 * runtime.GOMAXPROCS(0)
	PoolSize uint16

	// MaxIdleConns is the maximum number of idle connections. By
	// default, the client does not close idle connections.
	//
	// Default: 0
	MaxIdleConns uint16

	// MaxActiveConns is the maximum number of connections that the pool
	// allocates at one time. When MaxActiveConns is zero, the pool has
	// no limit on the number of connections. When the pool is full, the
	// next call to Get() blocks until a connection becomes free.
	//
	// Default: 0
	MaxActiveConns uint16

	// DB is the database that the client selects after it connects to
	// the server.
	DB uint16

	// HeartbeatInterval is how often this hub refreshes its liveness
	// lease in Redis.
	//
	// Default: 3s
	HeartbeatInterval time.Duration

	// HeartbeatTTL is how long a hub's liveness lease survives without
	// a refresh, before other hubs treat the hub as crashed. Set
	// HeartbeatTTL to a few multiples of HeartbeatInterval. This way, a
	// few missed beats do not trigger a false crash detection.
	//
	// Default: 10s
	HeartbeatTTL time.Duration

	// ReapBatchSize caps how many devices the code cleans up per Lua
	// call, when it reaps a crashed hub. This way, a hub that owns many
	// devices does not block Redis for too long in one call.
	//
	// Default: 500
	//
	// The code clamps values above presence.MaxBatchSize to that limit.
	ReapBatchSize uint16

	// DeviceBucketCount is the number of bucket hashes
	// ("devices:0".."devices:{N-1}") that split the device registry.
	// Pick N so that expectedMaxDevices / N stays well under the
	// configured Redis hash-max-listpack-entries limit (1024 in
	// infrastructure/redis/local/redis.conf). For example, 1 million
	// devices across the default 4096 buckets give about 244 fields per
	// bucket on average. Allow headroom, because some buckets hold more
	// than the average.
	//
	// IMPORTANT: treat DeviceBucketCount like a partition count. Once
	// Redis holds real data, you must not change this value freely. A
	// change to N changes the bucket that a deviceID maps to, through
	// bucket = Mix64(deviceID) % N. Existing entries then sit under the
	// wrong bucket keys, until a migration re-shards them. If you must
	// change N later, write a one-off job. The job must read every
	// "devices:*" bucket under the old N, then write each entry into
	// its bucket under the new N.
	//
	// Default: 4096
	DeviceBucketCount uint16

	// VerifyExpiredNotifications turns on a check that Redis keyspace
	// notifications for expired keys are enabled. The cache needs this
	// check to handle hub heartbeat expiration events.
	VerifyExpiredNotifications bool

	// PipelinePoolSize is the pool size for the separate pipeline
	// connection pool. The client always creates this dedicated
	// pipeline pool now. Setting PipelinePoolSize alone still sizes
	// that pool. Its buffers default to the larger of the regular
	// buffer size and DefaultPipelineBufferSize (128 KiB), unless you
	// set PipelineReadBufferSize or PipelineWriteBufferSize.
	//
	// Pipelining usually needs fewer connections than regular
	// operations, because batching reduces connection contention. A
	// smaller pool saves memory and keeps throughput high.
	//
	// NewClient always creates the dedicated pipeline pool, in the same
	// way as the pubsub pool. This way, pipelines never compete with
	// regular commands for connections in the main pool. The pipeline
	// pool never pre-dials: NewClient forces MinIdleConns to 0 on it.
	// So PipelinePoolSize caps burst capacity; it is not a standing
	// footprint, and an unused pipeline pool holds zero connections. A
	// burst of concurrent pipelines wider than the cap spills to the
	// main pool right away, through a non-blocking TryGet on the
	// pipeline pool. The client does not wait out a grace period first.
	// So a saturated pipeline pool never adds latency before it falls
	// back to the main pool, and DefaultPipelinePoolTimeout does not
	// gate that spill. Pipeline connections use
	// DefaultPipelineBufferSize buffers, unless you set the pipeline
	// buffer sizes explicitly. The pipeline pool does not inherit
	// MaxActiveConns. If it inherited MaxActiveConns as is, the
	// combined ceiling could reach about 2x MaxActiveConns. Instead,
	// the pipeline pool adds at most PipelinePoolSize connections on
	// top of the main pool's MaxActiveConns, so the effective ceiling
	// is MaxActiveConns plus PipelinePoolSize, a small and bounded
	// addition. The main pool that a burst spills to still enforces
	// MaxActiveConns.
	//
	// Set PipelinePoolSize to a negative value to opt out of the
	// dedicated pool entirely. Pipelines then run on the main pool, in
	// the same way as before this pool existed.
	//
	// Default: 10
	PipelinePoolSize int
}

// LoadConfig reads the Redis settings from loader and applies defaults. A nil loader
// is an empty source. LoadConfig returns an error if a required field is empty.
func LoadConfig(loader config.Loader) (Config, error) {
	var c Config
	if loader != nil {
		c.Addr = strings.TrimSpace(loader.GetString("DISTRIBUTED_CACHE_ADDRESS"))
		c.Password = strings.TrimSpace(loader.GetString("DISTRIBUTED_CACHE_PASSWORD"))
		c.DialTimeout = loader.GetDuration("DISTRIBUTED_CACHE_DIAL_TIMEOUT")
		c.DialIdle = loader.GetDuration("DISTRIBUTED_CACHE_DIAL_IDLE")
		c.DialInterval = loader.GetDuration("DISTRIBUTED_CACHE_DIAL_INTERVAL")
		c.DialKeepCount = loader.GetInt("DISTRIBUTED_CACHE_DIAL_KEEP_COUNT")
		c.PoolSize = loader.GetUint16("DISTRIBUTED_CACHE_POOL_SIZE")
		c.MaxIdleConns = loader.GetUint16("DISTRIBUTED_CACHE_MAX_IDLE_CONNS")
		c.MaxActiveConns = loader.GetUint16("DISTRIBUTED_CACHE_MAX_ACTIVE_CONNS")
		c.DB = loader.GetUint16("DISTRIBUTED_CACHE_DB")
		c.HeartbeatInterval = loader.GetDuration("DISTRIBUTED_CACHE_HEARTBEAT_INTERVAL")
		c.HeartbeatTTL = loader.GetDuration("DISTRIBUTED_CACHE_HEARTBEAT_TTL")
		c.ReapBatchSize = loader.GetUint16("DISTRIBUTED_CACHE_REAP_BATCH_SIZE")
		c.DeviceBucketCount = loader.GetUint16("DISTRIBUTED_CACHE_DEVICE_BUCKET_COUNT")
		c.VerifyExpiredNotifications = loader.GetBool("DISTRIBUTED_CACHE_VERIFY_EVENT_EXPIRED")
		c.PipelinePoolSize = loader.GetInt("DISTRIBUTED_CACHE_PIPELINE_POOL_SIZE")
		// GetDuration reads a value without a unit as nanoseconds.
		// Thus the values -1 and -2 keep their meaning.
		c.ReadTimeout = loader.GetDuration("DISTRIBUTED_CACHE_READ_TIMEOUT")
		c.WriteTimeout = loader.GetDuration("DISTRIBUTED_CACHE_WRITE_TIMEOUT")
	}
	c.applyDefault()

	err := c.validate()
	if err != nil {
		return Config{}, err
	}
	return c, nil
}

// validate returns an error if a required field is empty.
func (c *Config) validate() error {
	if len(c.Addr) == 0 {
		return errors.New("DISTRIBUTED_CACHE_ADDRESS is required")
	}
	return nil
}

func (c *Config) applyDefault() {
	if c.DialTimeout <= 0 {
		c.DialTimeout = defaultDialTimeout
	}
	if c.DialIdle <= 0 {
		c.DialIdle = defaultDialIdle
	}
	if c.DialInterval <= 0 {
		c.DialInterval = defaultDialInterval
	}
	if c.DialKeepCount <= 0 {
		c.DialKeepCount = defaultDialKeepCount
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
	if c.PoolSize == 0 {
		c.PoolSize = uint16(10 * runtime.GOMAXPROCS(0))
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = defaultHeartbeatInterval
	}
	if c.HeartbeatTTL <= 0 {
		c.HeartbeatTTL = defaultHeartbeatTTL
	}
	if c.ReapBatchSize == 0 {
		c.ReapBatchSize = defaultReapBatchSize
	}
	// A negative PipelinePoolSize disables the separate pipeline pool. Thus
	// only zero gets the default.
	if c.PipelinePoolSize == 0 {
		c.PipelinePoolSize = defaultPipelinePoolSize
	}
	if c.DeviceBucketCount == 0 {
		c.DeviceBucketCount = defaultDeviceBucketCount
	}
	c.ReapBatchSize = min(c.ReapBatchSize, uint16(presence.MaxBatchSize))
}
