/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package scylla

import (
	"testing"
	"time"

	"xxx/pkg/config"

	"github.com/gocql/gocql"
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
				NumConns:               2,
				NumRetries:             3,
				ConnectTimeout:         11 * time.Second,
				Timeout:                11 * time.Second,
				Consistency:            gocql.Quorum,
				SerialConsistency:      gocql.Serial,
				ReconnectMaxRetries:    3,
				ReconnectionJitterBase: 2 * time.Second,
				ReconnectionJitterCap:  time.Minute,
			},
			cfg,
		)
	}
}

func TestConfigLoadPreservesExplicitValues(t *testing.T) {
	l := viper.New()
	l.Set("SCYLLA_DATABASE_NUM_CONNS", 4)
	l.Set("SCYLLA_DATABASE_NUM_RETRIES", 0)
	l.Set("SCYLLA_DATABASE_CONSISTENCY", 0)
	l.Set("SCYLLA_DATABASE_CONNECT_TIMEOUT", "8s")
	l.Set("SCYLLA_DATABASE_TIMEOUT", "9s")
	l.Set("SCYLLA_DATABASE_RECONNECTION_MAX_RETRIES", 7)
	l.Set("SCYLLA_DATABASE_RECONNECTION_JITTER_BASE", "3s")
	l.Set("SCYLLA_DATABASE_RECONNECTION_JITTER_CAP", "45s")

	var cfg Config
	cfg.Load(l)
	require.Zero(t, cfg.NumRetries, "zero retries explicitly disables retries")
	require.Equal(t, gocql.Any, cfg.Consistency)
	require.Equal(t, 4, cfg.NumConns)
	require.Equal(t, 8*time.Second, cfg.ConnectTimeout)
	require.Equal(t, 9*time.Second, cfg.Timeout)
	require.EqualValues(t, 7, cfg.ReconnectMaxRetries)
	require.Equal(t, 3*time.Second, cfg.ReconnectionJitterBase)
	require.Equal(t, 45*time.Second, cfg.ReconnectionJitterCap)
}

func TestConfigLoadZeroReconnectionRetriesUsesDefault(t *testing.T) {
	l := viper.New()
	l.Set("SCYLLA_DATABASE_RECONNECTION_MAX_RETRIES", 0)

	var cfg Config
	cfg.Load(l)
	require.EqualValues(t, 3, cfg.ReconnectMaxRetries)
}
