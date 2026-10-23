/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package sql

import (
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
		require.Equal(t, Config{
			DialTimeout:  3 * time.Second,
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 5 * time.Second,
		}, cfg)
	}
}

func TestPoolConfigLoadDefaults(t *testing.T) {
	for _, loader := range []config.Loader{nil, viper.New()} {
		var cfg PoolConfig
		cfg.Load(loader)
		require.Equal(t, PoolConfig{}, cfg)
	}
}

func TestConfigLoadPreservesExplicitValues(t *testing.T) {
	l := viper.New()
	l.Set("SQL_DATABASE_MAX_OPEN_CONNS", 8)
	l.Set("SQL_DATABASE_MAX_IDLE_CONNS", 0)

	var cfg Config
	cfg.Load(l)
	require.Equal(t, 8, cfg.Pool.MaxOpenConns)
	require.Zero(t, cfg.Pool.MaxIdleConns)
}

func TestConfigLoadTimeoutOverrides(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
		want  time.Duration
	}{
		{name: "duration", value: 2 * time.Second, want: 2 * time.Second},
		{name: "duration_string", value: "750ms", want: 750 * time.Millisecond},
		{name: "zero", value: 0},
		{name: "zero_string", value: "0s"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loader := viper.New()
			loader.Set("SQL_DATABASE_DIAL_TIMEOUT", tc.value)
			loader.Set("SQL_DATABASE_READ_TIMEOUT", tc.value)
			loader.Set("SQL_DATABASE_WRITE_TIMEOUT", tc.value)
			var cfg Config
			cfg.Load(loader)
			require.Equal(t, tc.want, cfg.DialTimeout)
			require.Equal(t, tc.want, cfg.ReadTimeout)
			require.Equal(t, tc.want, cfg.WriteTimeout)
		})
	}
}

func TestConfigLoadPartialTimeoutOverride(t *testing.T) {
	loader := viper.New()
	loader.Set("SQL_DATABASE_READ_TIMEOUT", "30s")
	var cfg Config
	cfg.Load(loader)
	require.Equal(t, 3*time.Second, cfg.DialTimeout)
	require.Equal(t, 30*time.Second, cfg.ReadTimeout)
	require.Equal(t, 5*time.Second, cfg.WriteTimeout)
}

func TestConfigLoadTimeoutsFromEnvironment(t *testing.T) {
	t.Setenv("SQL_DATABASE_DIAL_TIMEOUT", "0")
	t.Setenv("SQL_DATABASE_READ_TIMEOUT", "20s")
	t.Setenv("SQL_DATABASE_WRITE_TIMEOUT", "0s")
	loader := viper.New()
	loader.AutomaticEnv()
	var cfg Config
	cfg.Load(loader)
	require.Zero(t, cfg.DialTimeout)
	require.Equal(t, 20*time.Second, cfg.ReadTimeout)
	require.Zero(t, cfg.WriteTimeout)
}
