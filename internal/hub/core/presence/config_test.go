/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package presence

import (
	"strconv"
	"testing"
	"time"

	"xxx/pkg/config"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestConfigLoadDefaults(t *testing.T) {
	for _, loader := range []config.Loader{nil, viper.New()} {
		var cfg Config
		cfg.Load(loader)
		require.Equal(
			t,
			Config{
				BatchSize:         100,
				Linger:            50 * time.Millisecond,
				RetryAttempts:     3,
				BackoffJitterBase: time.Second,
				BackoffJitterCap:  30 * time.Second,
			},
			cfg,
		)
	}
}

func TestConfigLoadPreservesExplicitValues(t *testing.T) {
	l := viper.New()
	l.Set("PRESENCE_CACHE_BATCH_SIZE", 5)
	l.Set("PRESENCE_CACHE_LINGER", "2ms")
	l.Set("PRESENCE_CACHE_RETRY_ATTEMPT", 6)
	l.Set("PRESENCE_SYNC_BACKOFF_JITTER_BASE", "2s")
	l.Set("PRESENCE_SYNC_BACKOFF_JITTER_CAP", "4s")

	var cfg Config
	cfg.Load(l)

	require.Equal(
		t,
		Config{
			BatchSize:         5,
			Linger:            2 * time.Millisecond,
			RetryAttempts:     6,
			BackoffJitterBase: 2 * time.Second,
			BackoffJitterCap:  4 * time.Second,
		},
		cfg,
	)
}

func TestConfigLoadCapsBatchSize(t *testing.T) {
	for _, size := range []int{MaxBatchSize, MaxBatchSize + 1, 10_000} {
		t.Run(
			strconv.Itoa(size),
			func(t *testing.T) {
				cfg := Config{BatchSize: size}
				cfg.Load(nil)
				require.Equal(t, MaxBatchSize, cfg.BatchSize)

				loader := viper.New()
				loader.Set("PRESENCE_CACHE_BATCH_SIZE", size)
				cfg.Load(loader)
				require.Equal(t, MaxBatchSize, cfg.BatchSize)
			},
		)
	}
}
