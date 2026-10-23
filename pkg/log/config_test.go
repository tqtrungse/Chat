/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package log

import (
	"testing"
	"time"

	"xxx/pkg/config"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestFileConfigLoadDefaults(t *testing.T) {
	for _, loader := range []config.Loader{nil, viper.New()} {
		var cfg FileConfig
		cfg.Load(loader)
		require.Equal(
			t,
			FileConfig{
				MaxSize:       100,
				BufferSize:    256 * 1024,
				FlushInterval: 30 * time.Second,
			},
			cfg,
		)
	}
}

func TestConfigLoadPreservesExplicitValues(t *testing.T) {
	l := viper.New()
	l.Set("LOGGER_MAX_SIZE", 20)
	l.Set("LOGGER_BUFFER_SIZE", 1024)
	l.Set("LOGGER_FLUSH_INTERVAL", "2s")

	var cfg FileConfig
	cfg.Load(l)
	require.Equal(t, 20, cfg.MaxSize)
	require.Equal(t, 1024, cfg.BufferSize)
	require.Equal(t, 2*time.Second, cfg.FlushInterval)
}
