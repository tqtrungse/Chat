/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connection

import (
	"testing"
	"time"

	"xxx/pkg/config"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestConfigLoadUsesConnectionNamespace(t *testing.T) {
	loader := viper.New()
	loader.Set("ROUTER_MAX_CONNS", 42)
	loader.Set("ROUTER_TICKER_DURATION", "9s")

	var cfg Config
	cfg.Load(loader)
	require.Equal(t, Config{MaxConns: 128000, TickerDuration: time.Second}, cfg)

	loader.Set("CONNECTION_MAX_CONNS", 80)
	loader.Set("CONNECTION_TICKER_DURATION", "2s")

	cfg.Load(loader)
	require.Equal(t, Config{MaxConns: 80, TickerDuration: 2 * time.Second}, cfg)
}

func TestConfigLoadDefaults(t *testing.T) {
	for _, loader := range []config.Loader{nil, viper.New()} {
		var cfg Config
		cfg.Load(loader)
		require.Equal(
			t,
			Config{
				TickerDuration: time.Second,
				MaxConns:       128000,
			},
			cfg,
		)
	}
}

func TestConfigLoadPreservesExplicitValues(t *testing.T) {
	l := viper.New()
	l.Set("CONNECTION_MAX_CONNS", 80)
	l.Set("CONNECTION_TICKER_DURATION", "2s")

	var cfg Config
	cfg.Load(l)
	require.EqualValues(t, 80, cfg.MaxConns)
	require.Equal(t, 2*time.Second, cfg.TickerDuration)
}
