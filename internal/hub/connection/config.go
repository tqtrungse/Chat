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
	"time"

	"xxx/pkg"
)

// Config composes the config for every component in this package. Each
// component only ever sees its own nested struct (RouterConfig,
// PresenceSyncConfig) — Config itself just exists so callers wire up one
// value instead of several.
type Config struct {
	Router       RouterConfig
	PresenceSync PresenceSyncConfig
}

// RouterConfig configures Router: the connection registry.
type RouterConfig struct {
	// TickerDuration defines interval time to the next polling.
	//
	// Default: 1s
	TickerDuration time.Duration

	// MaxConns defines the maximum number of keeping connections in the hub.
	//
	// Default: 128000
	MaxConns uint32
}

// PresenceSyncConfig configures presenceSync: batching of device add/delete
// ops, and retry/backoff behavior for the underlying cache operations.
type PresenceSyncConfig struct {
	// BatchSize is the maximum number of add/delete device requests accumulated before a partition
	// flushes immediately.
	//
	// Default: 100
	BatchSize int

	// Linger is the maximum time a non-empty partition batch waits for more
	// add/delete device requests. A non-positive value disables the linger timer;
	// such a batch is flushed only when BatchSize is reached or the batcher's context is canceled.
	//
	// Default: 50ms
	Linger time.Duration

	// RetryAttempts
	//
	// Default: 3
	RetryAttempts uint32

	// BackoffJitterBase
	//
	// Default: 1s
	BackoffJitterBase time.Duration

	// BackoffJitterCap
	//
	// Default: 30s
	BackoffJitterCap time.Duration
}

func (c *Config) Load(loader pkg.ConfigLoader) {
	if loader == nil {
		return
	}
	c.Router.TickerDuration = loader.GetDuration("ROUTER_TICKER_DURATION")
	c.Router.MaxConns = loader.GetUint32("ROUTER_MAX_CONNS")
	c.PresenceSync.BatchSize = loader.GetInt("ROUTER_CACHE_BATCH_SIZE")
	c.PresenceSync.Linger = loader.GetDuration("ROUTER_CACHE_LINGER")
	c.PresenceSync.RetryAttempts = loader.GetUint32("ROUTER_CACHE_RETRY_ATTEMPT")
	c.PresenceSync.BackoffJitterBase = loader.GetDuration("ROUTER_SYNC_BACKOFF_JITTER_BASE")
	c.PresenceSync.BackoffJitterCap = loader.GetDuration("ROUTER_SYNC_BACKOFF_JITTER_CAP")
}
