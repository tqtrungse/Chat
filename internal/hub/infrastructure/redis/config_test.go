/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package redis

import (
	"runtime"
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
		require.Equal(t,
			Config{
				PoolSize:          uint16(10 * runtime.GOMAXPROCS(0)),
				HeartbeatInterval: 3 * time.Second,
				HeartbeatTTL:      10 * time.Second,
				ReapBatchSize:     500,
				DeviceBucketCount: 4096,
			},
			cfg,
		)
	}
}

func TestConfigLoadPreservesExplicitValues(t *testing.T) {
	l := viper.New()
	l.Set("DISTRIBUTED_CACHE_POOL_SIZE", 4)
	l.Set("DISTRIBUTED_CACHE_HEARTBEAT_INTERVAL", "1s")
	l.Set("DISTRIBUTED_CACHE_HEARTBEAT_TTL", "6s")
	l.Set("DISTRIBUTED_CACHE_REAP_BATCH_SIZE", 12)
	l.Set("DISTRIBUTED_CACHE_DEVICE_BUCKET_COUNT", 64)

	var cfg Config
	cfg.Load(l)
	require.EqualValues(t, 4, cfg.PoolSize)
	require.Equal(t, time.Second, cfg.HeartbeatInterval)
	require.Equal(t, 6*time.Second, cfg.HeartbeatTTL)
	require.EqualValues(t, 12, cfg.ReapBatchSize)
	require.EqualValues(t, 64, cfg.DeviceBucketCount)
}

func TestConfigCapsReapBatchSize(t *testing.T) {
	for _, size := range []uint16{500, 501, 65535} {
		cfg := Config{ReapBatchSize: size}
		cfg.Load(nil)
		require.EqualValues(t, 500, cfg.ReapBatchSize)
	}
}
