/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package sql

import (
	"testing"
	"time"

	"xxx/pkg/config"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func requiredLoader() *viper.Viper {
	l := viper.New()
	l.Set("SQL_DATABASE_PRIMARY_ADDRESS", "primary:3306")
	l.Set("SQL_DATABASE_USER", "user")
	l.Set("SQL_DATABASE_NAME", "db")
	return l
}

func TestConfigLoadDefaults(t *testing.T) {
	want := Config{
		User:          "user",
		DBName:        "db",
		PrimaryAddr:   "primary:3306",
		DialTimeout:   3 * time.Second,
		ReadTimeout:   10 * time.Second,
		WriteTimeout:  5 * time.Second,
		SlowThreshold: 200 * time.Millisecond,
	}

	t.Run(
		"unset keys",
		func(t *testing.T) {
			cfg, err := LoadConfig(requiredLoader())
			require.NoError(t, err)
			require.Equal(t, want, cfg)
		},
	)
}

func TestConfigLoadRequiredFields(t *testing.T) {
	for _, key := range []string{
		"SQL_DATABASE_PRIMARY_ADDRESS",
		"SQL_DATABASE_USER",
		"SQL_DATABASE_NAME",
	} {
		t.Run(
			key,
			func(t *testing.T) {
				l := requiredLoader()
				l.Set(key, " ")
				_, err := LoadConfig(l)
				require.EqualError(t, err, key+" is required")
			},
		)
	}

	t.Run(
		"nil loader",
		func(t *testing.T) {
			_, err := LoadConfig(nil)
			require.EqualError(t, err, "SQL_DATABASE_PRIMARY_ADDRESS is required")
		},
	)
}

func TestPoolConfigLoadDefaults(t *testing.T) {
	for _, loader := range []config.Loader{nil, viper.New()} {
		cfg := loadPoolConfig(loader)
		require.Equal(t, PoolConfig{}, cfg)
	}
}

func TestConfigLoadPreservesExplicitValues(t *testing.T) {
	l := requiredLoader()
	l.Set("SQL_DATABASE_MAX_OPEN_CONNS", 8)
	l.Set("SQL_DATABASE_MAX_IDLE_CONNS", 0)

	cfg, err := LoadConfig(l)
	require.NoError(t, err)
	require.Equal(t, 8, cfg.Pool.MaxOpenConns)
	require.Zero(t, cfg.Pool.MaxIdleConns)
}

func TestConfigLoadTimeoutOverrides(t *testing.T) {
	defaults := Config{
		DialTimeout:   3 * time.Second,
		ReadTimeout:   10 * time.Second,
		WriteTimeout:  5 * time.Second,
		SlowThreshold: 200 * time.Millisecond,
	}
	same := func(d time.Duration) func(time.Duration) time.Duration {
		return func(time.Duration) time.Duration { return d }
	}

	for _, tc := range []struct {
		name  string
		value any
		want  func(def time.Duration) time.Duration
	}{
		{
			name:  "duration",
			value: 2 * time.Second,
			want:  same(2 * time.Second),
		},
		{
			name:  "duration_string",
			value: "750ms",
			want:  same(750 * time.Millisecond),
		},
		{
			name:  "zero",
			value: 0,
			want:  func(def time.Duration) time.Duration { return def },
		},
		{
			name:  "zero_string",
			value: "0s",
			want:  func(def time.Duration) time.Duration { return def },
		},
		{
			name:  "negative_string",
			value: "-1s",
			want:  same(-time.Second),
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				loader := requiredLoader()
				loader.Set("SQL_DATABASE_DIAL_TIMEOUT", tc.value)
				loader.Set("SQL_DATABASE_READ_TIMEOUT", tc.value)
				loader.Set("SQL_DATABASE_WRITE_TIMEOUT", tc.value)
				loader.Set("SQL_DATABASE_SLOW_THRESHOLD", tc.value)

				cfg, err := LoadConfig(loader)
				require.NoError(t, err)
				require.Equal(t, tc.want(defaults.DialTimeout), cfg.DialTimeout)
				require.Equal(t, tc.want(defaults.ReadTimeout), cfg.ReadTimeout)
				require.Equal(t, tc.want(defaults.WriteTimeout), cfg.WriteTimeout)
				require.Equal(t, tc.want(defaults.SlowThreshold), cfg.SlowThreshold)
			},
		)
	}
}

func TestConfigLoadPartialTimeoutOverride(t *testing.T) {
	loader := requiredLoader()
	loader.Set("SQL_DATABASE_READ_TIMEOUT", "30s")

	cfg, err := LoadConfig(loader)
	require.NoError(t, err)
	require.Equal(t, 3*time.Second, cfg.DialTimeout)
	require.Equal(t, 30*time.Second, cfg.ReadTimeout)
	require.Equal(t, 5*time.Second, cfg.WriteTimeout)
}

func TestConfigLoadTimeoutsFromEnvironment(t *testing.T) {
	t.Setenv("SQL_DATABASE_DIAL_TIMEOUT", "0")
	t.Setenv("SQL_DATABASE_READ_TIMEOUT", "20s")
	t.Setenv("SQL_DATABASE_WRITE_TIMEOUT", "-1s")
	loader := requiredLoader()
	loader.AutomaticEnv()

	cfg, err := LoadConfig(loader)
	require.NoError(t, err)
	require.Equal(t, 3*time.Second, cfg.DialTimeout)
	require.Equal(t, 20*time.Second, cfg.ReadTimeout)
	require.Equal(t, -time.Second, cfg.WriteTimeout)
}

func TestConfigApplyDefaultKeepsNegativeLimits(t *testing.T) {
	cfg := Config{
		ReadTimeout:   -1,
		SlowThreshold: -1,
	}
	cfg.applyDefault()
	require.Equal(t, 3*time.Second, cfg.DialTimeout)
	require.EqualValues(t, -1, cfg.ReadTimeout)
	require.Equal(t, 5*time.Second, cfg.WriteTimeout)
	require.EqualValues(t, -1, cfg.SlowThreshold)
}
