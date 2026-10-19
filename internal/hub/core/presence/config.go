/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package presence

import (
	"time"

	"xxx/pkg/config"
)

type Config struct {
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

func (c *Config) Load(loader config.Loader) {
	if loader == nil {
		return
	}
	c.BatchSize = loader.GetInt("PRESENCE_CACHE_BATCH_SIZE")
	c.Linger = loader.GetDuration("PRESENCE_CACHE_LINGER")
	c.RetryAttempts = loader.GetUint32("PRESENCE_CACHE_RETRY_ATTEMPT")
	c.BackoffJitterBase = loader.GetDuration("PRESENCE_SYNC_BACKOFF_JITTER_BASE")
	c.BackoffJitterCap = loader.GetDuration("PRESENCE_SYNC_BACKOFF_JITTER_CAP")
}