/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package presence

import (
	"time"

	"xxx/pkg/config"
)

const (
	// MaxBatchSize bounds the device count submitted in one cache batch.
	MaxBatchSize             = 500
	defaultBatchSize         = 100
	defaultLinger            = 50 * time.Millisecond
	defaultRetryAttempts     = 3
	defaultBackoffJitterBase = time.Second
	defaultBackoffJitterCap  = 30 * time.Second
)

type Config struct {
	// BatchSize is the maximum number of add/delete device requests accumulated before a partition
	// flushes immediately.
	//
	// Default: 100
	//
	// Values above MaxBatchSize are clamped to MaxBatchSize.
	BatchSize int

	// Linger is the maximum time a non-empty partition batch waits for more
	// add/delete device requests. Load replaces a non-positive value with the default.
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

// Load reads batching and retry settings when a loader is provided and applies defaults.
func (c *Config) Load(loader config.Loader) {
	if loader != nil {
		c.BatchSize = loader.GetInt("PRESENCE_CACHE_BATCH_SIZE")
		c.Linger = loader.GetDuration("PRESENCE_CACHE_LINGER")
		c.RetryAttempts = loader.GetUint32("PRESENCE_CACHE_RETRY_ATTEMPT")
		c.BackoffJitterBase = loader.GetDuration("PRESENCE_SYNC_BACKOFF_JITTER_BASE")
		c.BackoffJitterCap = loader.GetDuration("PRESENCE_SYNC_BACKOFF_JITTER_CAP")
	}
	c.BatchSize = normalizeBatchSize(c.BatchSize)
	if c.Linger <= 0 {
		c.Linger = defaultLinger
	}
	if c.RetryAttempts == 0 {
		c.RetryAttempts = defaultRetryAttempts
	}
	if c.BackoffJitterBase <= 0 {
		c.BackoffJitterBase = defaultBackoffJitterBase
	}
	if c.BackoffJitterCap <= 0 {
		c.BackoffJitterCap = defaultBackoffJitterCap
	}
}

// normalizeBatchSize applies the default and upper bound for a cache batch.
func normalizeBatchSize(size int) int {
	if size <= 0 {
		return defaultBatchSize
	}
	return min(size, MaxBatchSize)
}
