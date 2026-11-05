/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package log

import (
	"testing"
	"time"

	"xxx/pkg/config"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestConfigLoadDefaults(t *testing.T) {
	for _, tc := range []struct {
		name   string
		loader config.Loader
	}{
		{name: "nil loader", loader: nil},
		{name: "unset keys", loader: viper.New()},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				cfg := LoadConfig(tc.loader)
				require.Equal(t, DefaultConfig(), cfg)
			},
		)
	}
}

func TestConfigLoadPreservesExplicitValues(t *testing.T) {
	l := viper.New()
	l.Set("LOGGER_QUEUE_SIZE", 32)
	l.Set("LOGGER_FLUSH_INTERVAL", "1s")
	l.Set("LOGGER_DISABLE_SAMPLING", true)

	cfg := LoadConfig(l)
	require.Equal(t, 32, cfg.QueueSize)
	require.Equal(t, time.Second, cfg.FlushInterval)
	require.True(t, cfg.DisableSampling)
	require.Equal(t, defaultBufferSize, cfg.BufferSize)
}
