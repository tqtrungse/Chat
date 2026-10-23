/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package activateconn

import (
	"runtime"
	"testing"
	"time"

	"xxx/pkg/config"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestConfigLoadKeepsRuntimeDependenciesUnresolved(t *testing.T) {
	loader := viper.New()
	loader.Set("CONNECTION_MAX_CONNS", 80)
	loader.Set("TCP_SERVER_MAX_FRAME_BYTES", 512)
	loader.Set("HUB_NOISE_HANDSHAKE_TIMEOUT", "2s")

	var cfg Config
	cfg.Load(loader)
	require.Equal(
		t,
		Config{
			VerifyWorkers:  max(runtime.GOMAXPROCS(0)/2, 1),
			MaxVerifyJobs:  max(runtime.GOMAXPROCS(0)/2, 1) * 4,
			PreAuthTimeout: 10 * time.Second,
		},
		cfg,
	)

	loader.Set("ACTIVATE_PREAUTH_TIMEOUT", "7s")
	loader.Set("ACTIVATE_MAX_PREAUTH_CONNS", 3)

	cfg.Load(loader)
	require.Equal(
		t,
		Config{
			VerifyWorkers:   max(runtime.GOMAXPROCS(0)/2, 1),
			MaxVerifyJobs:   max(runtime.GOMAXPROCS(0)/2, 1) * 4,
			PreAuthTimeout:  7 * time.Second,
			MaxPreAuthConns: 3,
		},
		cfg,
	)
}

func TestConfigLoadDefaults(t *testing.T) {
	loaders := []config.Loader{nil, viper.New()}
	for _, loader := range loaders {
		var cfg Config
		cfg.Load(loader)
		require.Equal(
			t,
			Config{
				VerifyWorkers:  max(runtime.GOMAXPROCS(0)/2, 1),
				MaxVerifyJobs:  max(runtime.GOMAXPROCS(0)/2, 1) * 4,
				PreAuthTimeout: 10 * time.Second,
			},
			cfg,
		)
	}
}

func TestConfigLoadPreservesExplicitValues(t *testing.T) {
	l := viper.New()
	l.Set("ACTIVATE_PREAUTH_TIMEOUT", "7s")

	var cfg Config
	cfg.Load(l)
	require.Zero(t, cfg.MaxPreAuthConns, "only bootstrap derives the pre-auth limit")
	require.Zero(t, cfg.HandshakeTimeout)
	require.Zero(t, cfg.MaxFrameBytes)
	require.Equal(t, 7*time.Second, cfg.PreAuthTimeout)

	l.Set("ACTIVATE_MAX_PREAUTH_CONNS", 3)

	cfg.Load(l)
	require.EqualValues(t, 3, cfg.MaxPreAuthConns)
}

func TestConfigAuthorizationLimits(t *testing.T) {
	l := viper.New()
	l.Set("ACTIVATE_VERIFY_WORKERS", 2)
	l.Set("ACTIVATE_MAX_VERIFY_JOBS", 7)

	var cfg Config
	cfg.Load(l)
	require.Equal(t, 2, cfg.VerifyWorkers)
	require.Equal(t, 7, cfg.MaxVerifyJobs)
}
