/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package tcp

import (
	"testing"
	"time"

	"xxx/pkg/config"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestConfigLoadDefaults(t *testing.T) {
	want := Config{
		Addr:             "127.0.0.1:9000",
		TCPKeepAlive:     30 * time.Second,
		ReadBufferCap:    4096,
		WriteBufferCap:   4096,
		MaxFrameBytes:    4096,
		MaxFramesPerTick: 64,
		MaxBytesPerTick:  256 * 1024,
	}

	t.Run(
		"unset keys",
		func(t *testing.T) {
			l := viper.New()
			l.Set("TCP_SERVER_ADDRESS", " 127.0.0.1:9000 ")
			cfg, err := LoadConfig(l)
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
		{name: "nil loader", loader: nil},
		{name: "empty loader", loader: viper.New()},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				_, err := LoadConfig(tc.loader)
				require.EqualError(t, err, "TCP_SERVER_ADDRESS is required")
			},
		)
	}
}

func TestConfigLoadPreservesExplicitValues(t *testing.T) {
	l := viper.New()
	l.Set("TCP_SERVER_ADDRESS", "127.0.0.1:9000")
	l.Set("TCP_SERVER_MAX_FRAME_BYTES", 512)
	l.Set("TCP_SERVER_MAX_FRAMES_PER_TICK", 4)
	l.Set("TCP_SERVER_MAX_BYTES_PER_TICK", 2048)

	cfg, err := LoadConfig(l)
	require.NoError(t, err)
	require.Equal(t, 512, cfg.MaxFrameBytes)
	require.Equal(t, 4, cfg.MaxFramesPerTick)
	require.Equal(t, 2048, cfg.MaxBytesPerTick)
}
