/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
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

func addrLoader() *viper.Viper {
	l := viper.New()
	l.Set("DISTRIBUTED_CACHE_ADDRESS", "127.0.0.1:6379")
	return l
}

func TestConfigLoadDefaults(t *testing.T) {
	want := Config{
		Addr:              "127.0.0.1:6379",
		DialTimeout:       5 * time.Second,
		DialIdle:          30 * time.Second,
		DialInterval:      5 * time.Second,
		DialKeepCount:     3,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		PoolSize:          uint16(10 * runtime.GOMAXPROCS(0)),
		HeartbeatInterval: 3 * time.Second,
		HeartbeatTTL:      10 * time.Second,
		ReapBatchSize:     500,
		DeviceBucketCount: 4096,
		PipelinePoolSize:  10,
	}

	t.Run(
		"unset keys",
		func(t *testing.T) {
			cfg, err := LoadConfig(addrLoader())
			require.NoError(t, err)
			require.Equal(t, want, cfg)
		},
	)
}

func TestConfigLoadRequiresAddr(t *testing.T) {
	for _, tc := range []struct {
		name   string
		loader config.Loader
	}{
		{
			name:   "nil loader",
			loader: nil,
		},
		{
			name:   "empty loader",
			loader: viper.New(),
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				_, err := LoadConfig(tc.loader)
				require.EqualError(t, err, "DISTRIBUTED_CACHE_ADDRESS is required")
			},
		)
	}
}

func TestConfigLoadPreservesExplicitValues(t *testing.T) {
	l := addrLoader()
	l.Set("DISTRIBUTED_CACHE_POOL_SIZE", 4)
	l.Set("DISTRIBUTED_CACHE_HEARTBEAT_INTERVAL", "1s")
	l.Set("DISTRIBUTED_CACHE_HEARTBEAT_TTL", "6s")
	l.Set("DISTRIBUTED_CACHE_REAP_BATCH_SIZE", 12)
	l.Set("DISTRIBUTED_CACHE_DEVICE_BUCKET_COUNT", 64)

	cfg, err := LoadConfig(l)
	require.NoError(t, err)
	require.EqualValues(t, 4, cfg.PoolSize)
	require.Equal(t, time.Second, cfg.HeartbeatInterval)
	require.Equal(t, 6*time.Second, cfg.HeartbeatTTL)
	require.EqualValues(t, 12, cfg.ReapBatchSize)
	require.EqualValues(t, 64, cfg.DeviceBucketCount)
}

func TestConfigLoadPipelinePoolSize(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  int
	}{
		{
			name:  "unset",
			value: 0,
			want:  10,
		},
		{
			name:  "zero",
			value: 0,
			want:  10,
		},
		{
			name:  "custom",
			value: 4,
			want:  4,
		},
		{
			name:  "negative opts out",
			value: -1,
			want:  -1,
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				l := addrLoader()
				if tc.value != nil {
					l.Set("DISTRIBUTED_CACHE_PIPELINE_POOL_SIZE", tc.value)
				}
				cfg, err := LoadConfig(l)
				require.NoError(t, err)
				require.Equal(t, tc.want, cfg.PipelinePoolSize)
			},
		)
	}
}

func TestConfigApplyDefaultKeepsNegativePipelinePoolSize(t *testing.T) {
	cfg := Config{PipelinePoolSize: -1}
	cfg.applyDefault()
	require.Equal(t, -1, cfg.PipelinePoolSize)
}

func TestConfigLoadSocketTimeouts(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		want  time.Duration
	}{
		{
			name:  "duration",
			value: "2s",
			want:  2 * time.Second,
		},
		{
			name:  "no timeout",
			value: "-1",
			want:  -1,
		},
		{
			name:  "no deadline",
			value: "-2",
			want:  -2,
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				l := addrLoader()
				l.Set("DISTRIBUTED_CACHE_READ_TIMEOUT", tc.value)
				l.Set("DISTRIBUTED_CACHE_WRITE_TIMEOUT", tc.value)
				cfg, err := LoadConfig(l)
				require.NoError(t, err)
				require.Equal(t, tc.want, cfg.ReadTimeout)
				require.Equal(t, tc.want, cfg.WriteTimeout)
			},
		)
	}
}

func TestConfigCapsReapBatchSize(t *testing.T) {
	for _, size := range []uint16{500, 501, 65535} {
		l := addrLoader()
		l.Set("DISTRIBUTED_CACHE_REAP_BATCH_SIZE", size)
		cfg, err := LoadConfig(l)
		require.NoError(t, err)
		require.EqualValues(t, 500, cfg.ReapBatchSize)
	}
}
