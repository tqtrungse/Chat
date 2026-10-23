/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package http

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
				ReadHeaderTimeout: 5 * time.Second,
				ReadTimeout:       10 * time.Second,
				WriteTimeout:      30 * time.Second,
				IdleTimeout:       15 * time.Second,
				MaxHeaderBytes:    16384,
			},
			cfg,
		)
	}
}

func TestConfigLoadPreservesExplicitValues(t *testing.T) {
	l := viper.New()
	l.Set("HTTP_READ_HEADER_TIMEOUT", "0s")
	l.Set("HTTP_READ_TIMEOUT", "0s")
	l.Set("HTTP_WRITE_TIMEOUT", "0s")
	l.Set("HTTP_MAX_HEADER_BYTES", 32768)

	var cfg Config
	cfg.Load(l)
	require.Zero(t, cfg.ReadHeaderTimeout)
	require.Zero(t, cfg.ReadTimeout)
	require.Zero(t, cfg.WriteTimeout)
	require.Equal(t, 32768, cfg.MaxHeaderBytes)
}
