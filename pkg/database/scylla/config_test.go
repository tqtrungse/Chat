/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
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

func hostsLoader() *viper.Viper {
	l := viper.New()
	l.Set("SCYLLA_DATABASE_HOSTS", "127.0.0.1")
	return l
}

func TestConfigLoadDefaults(t *testing.T) {
	want := Config{
		Hosts:                  []string{"127.0.0.1"},
		NumConns:               2,
		NumRetries:             3,
		ConnectTimeout:         11 * time.Second,
		Timeout:                11 * time.Second,
		Consistency:            gocql.Quorum,
		SerialConsistency:      gocql.Serial,
		ReconnectMaxRetries:    3,
		ReconnectionJitterBase: 2 * time.Second,
		ReconnectionJitterCap:  time.Minute,
	}

	t.Run(
		"unset keys",
		func(t *testing.T) {
			cfg, err := LoadConfig(hostsLoader())
			require.NoError(t, err)
			require.Equal(t, want, cfg)
		},
	)
}

func TestConfigLoadRequiresHosts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		loader config.Loader
	}{
		{name: "nil loader", loader: nil},
		{name: "empty loader", loader: viper.New()},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				_, err := LoadConfig(tc.loader)
				require.ErrorIs(t, err, ErrNilHosts)
			},
		)
	}
}

func TestConfigLoadTrimsStrings(t *testing.T) {
	l := hostsLoader()
	l.Set("SCYLLA_DATABASE_USER", " user ")
	l.Set("SCYLLA_DATABASE_PASSWORD", " secret ")
	l.Set("SCYLLA_DATABASE_KEYSPACE", " ks ")
	l.Set("SCYLLA_DATABASE_LOCAL_DC", " dc1 ")
	l.Set("SCYLLA_DATABASE_CA_PATH", " ca.pem ")

	cfg, err := LoadConfig(l)
	require.NoError(t, err)
	require.Equal(t, "user", cfg.User)
	require.Equal(t, " secret ", cfg.Passwd, "a password keeps its spaces")
	require.Equal(t, "ks", cfg.Keyspace)
	require.Equal(t, "dc1", cfg.LocalDC)
	require.Equal(t, "ca.pem", cfg.CAPath)
}

func TestConfigLoadPreservesExplicitValues(t *testing.T) {
	l := hostsLoader()
	l.Set("SCYLLA_DATABASE_NUM_CONNS", 4)
	l.Set("SCYLLA_DATABASE_NUM_RETRIES", 0)
	l.Set("SCYLLA_DATABASE_CONSISTENCY", 0)
	l.Set("SCYLLA_DATABASE_CONNECT_TIMEOUT", "8s")
	l.Set("SCYLLA_DATABASE_TIMEOUT", "9s")
	l.Set("SCYLLA_DATABASE_RECONNECTION_MAX_RETRIES", 7)
	l.Set("SCYLLA_DATABASE_RECONNECTION_JITTER_BASE", "3s")
	l.Set("SCYLLA_DATABASE_RECONNECTION_JITTER_CAP", "45s")

	cfg, err := LoadConfig(l)
	require.NoError(t, err)
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
	l := hostsLoader()
	l.Set("SCYLLA_DATABASE_RECONNECTION_MAX_RETRIES", 0)

	cfg, err := LoadConfig(l)
	require.NoError(t, err)
	require.EqualValues(t, 3, cfg.ReconnectMaxRetries)
}
