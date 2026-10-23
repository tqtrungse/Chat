/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
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
	for _, loader := range []config.Loader{nil, viper.New()} {
		var cfg Config
		cfg.Load(loader)
		require.Equal(
			t,
			Config{
				TCPKeepAlive:     30 * time.Second,
				ReadBufferCap:    4096,
				WriteBufferCap:   4096,
				MaxFrameBytes:    4096,
				MaxFramesPerTick: 64,
				MaxBytesPerTick:  256 * 1024,
			},
			cfg,
		)
	}
}

func TestConfigLoadPreservesExplicitValues(t *testing.T) {
	l := viper.New()
	l.Set("TCP_SERVER_MAX_FRAME_BYTES", 512)
	l.Set("TCP_SERVER_MAX_FRAMES_PER_TICK", 4)
	l.Set("TCP_SERVER_MAX_BYTES_PER_TICK", 2048)

	var cfg Config
	cfg.Load(l)
	require.Equal(t, 512, cfg.MaxFrameBytes)
	require.Equal(t, 4, cfg.MaxFramesPerTick)
	require.Equal(t, 2048, cfg.MaxBytesPerTick)
}
