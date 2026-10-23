/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package bootstrap

import (
	"testing"
	"time"

	"xxx/internal/hub/adapter/tcp"
	"xxx/internal/hub/application/activateconn"
	"xxx/internal/hub/core/connection"
	"xxx/internal/hub/infrastructure/noise"

	"github.com/stretchr/testify/require"
)

func activationInputs() (
	activateconn.Config,
	noise.Config,
	tcp.Config,
	connection.Config,
) {
	var activation activateconn.Config
	activation.Load(nil)

	var tcpConfig tcp.Config
	tcpConfig.Load(nil)

	var manager connection.Config
	manager.Load(nil)

	return activation, noise.Config{HandshakeTimeout: 5 * time.Second}, tcpConfig, manager
}

func TestResolveActivationConfig(t *testing.T) {
	for _, tc := range []struct {
		name    string
		max     uint32
		limit   uint32
		frame   int
		wantCap uint32
	}{
		{"automatic", 128000, 0, 4096, 12800},
		{"less than ten connections", 9, 0, 512, 1},
		{"one connection", 1, 0, 512, 1},
		{"ten connections", 10, 0, 512, 1},
		{"round down", 29, 0, 4096, 2},
		{"explicit limit", 100, 7, 65535, 7},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				cfg, cfgNoise, cfgTCP, cfgManager := activationInputs()
				cfg.MaxPreAuthConns = tc.limit
				cfg.HandshakeTimeout = time.Hour
				cfg.MaxFrameBytes = 1
				before := cfg
				cfgNoise.HandshakeTimeout = 2 * time.Second
				cfgTCP.MaxFrameBytes = tc.frame
				cfgManager.MaxConns = tc.max
				resolved, err := resolveActivationConfig(cfg, cfgNoise, cfgTCP, cfgManager)
				require.NoError(t, err)
				require.Equal(t, before, cfg, "resolution must not modify the input")
				require.Equal(
					t,
					activateconn.Config{
						VerifyWorkers:    cfg.VerifyWorkers,
						MaxVerifyJobs:    cfg.MaxVerifyJobs,
						PreAuthTimeout:   10 * time.Second,
						HandshakeTimeout: 2 * time.Second,
						MaxFrameBytes:    tc.frame,
						MaxPreAuthConns:  tc.wantCap,
					},
					resolved,
				)
			},
		)
	}
}

func TestResolveActivationConfigRejectsIncompleteOrInvalidInputs(t *testing.T) {
	for _, kind := range []string{
		"pre-auth timeout",
		"handshake timeout",
		"negative handshake timeout",
		"frame missing",
		"frame too small",
		"frame too large",
		"connection capacity",
	} {
		t.Run(
			kind,
			func(t *testing.T) {
				var (
					key                               string
					cfg, cfgNoise, cfgTCP, cfgManager = activationInputs()
				)

				switch kind {
				case "pre-auth timeout":
					cfg.PreAuthTimeout = 0
					key = "ACTIVATE_PREAUTH_TIMEOUT"

				case "handshake timeout",
					"negative handshake timeout":
					cfgNoise.HandshakeTimeout = 0
					if kind == "negative handshake timeout" {
						cfgNoise.HandshakeTimeout = -time.Second
					}
					key = "HUB_NOISE_HANDSHAKE_TIMEOUT"

				case "frame missing",
					"frame too small",
					"frame too large":
					cfgTCP.MaxFrameBytes = map[string]int{
						"frame missing":   0,
						"frame too small": 511,
						"frame too large": 65536,
					}[kind]
					key = "TCP_SERVER_MAX_FRAME_BYTES"

				case "connection capacity":
					cfgManager.MaxConns = 0
					key = "CONNECTION_MAX_CONNS"
				}

				_, err := resolveActivationConfig(cfg, cfgNoise, cfgTCP, cfgManager)
				require.ErrorContains(t, err, key)
			},
		)
	}
}
