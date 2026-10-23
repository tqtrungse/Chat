/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package nats

import (
	"testing"
	"time"

	"xxx/pkg/config"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestConfigLoadDefaults(t *testing.T) {
	for _, loader := range []config.Loader{nil, viper.New()} {
		cfg, err := LoadConfig(loader)
		require.NoError(t, err)
		require.Equal(
			t,
			Config{
				MaxHops:        5,
				PublishTimeout: 30 * time.Second,
				InboxWorkers:   128,
				DeliveryBackOff: []time.Duration{
					100 * time.Millisecond,
					500 * time.Millisecond,
					time.Second,
					2 * time.Second,
					5 * time.Second,
				},
			},
			cfg,
		)
	}
}

func TestConfigLoadPreservesExplicitValues(t *testing.T) {
	l := viper.New()
	l.Set("BROKER_MAX_HOPS", 9)
	l.Set("BROKER_PUBLISH_TIMEOUT", "4s")
	l.Set("BROKER_INBOX_WORKERS", 2)
	l.Set("BROKER_DELIVERY_BACKOFF", "1ms,2ms")

	cfg, err := LoadConfig(l)
	require.NoError(t, err)
	require.Equal(
		t,
		Config{
			MaxHops:         9,
			PublishTimeout:  4 * time.Second,
			InboxWorkers:    2,
			DeliveryBackOff: []time.Duration{time.Millisecond, 2 * time.Millisecond},
		},
		cfg,
	)
}

func TestConfigDefaultBackoffHasIndependentStorage(t *testing.T) {
	first, err := LoadConfig(nil)
	require.NoError(t, err)

	second, err := LoadConfig(nil)
	require.NoError(t, err)

	first.DeliveryBackOff[0] = time.Hour
	require.Equal(t, 100*time.Millisecond, second.DeliveryBackOff[0])
}

func TestConfigLoadRejectsInvalidBackoff(t *testing.T) {
	for _, value := range []string{"oops", "0s", "-1s", "1ms,bad"} {
		t.Run(
			value,
			func(t *testing.T) {
				l := viper.New()
				l.Set("BROKER_DELIVERY_BACKOFF", value)
				_, err := LoadConfig(l)
				require.ErrorContains(t, err, "BROKER_DELIVERY_BACKOFF")
			},
		)
	}
}

func TestConfigApplyDefaultFillsCodeBuiltConfig(t *testing.T) {
	cfg := Config{MaxHops: 9}
	cfg.applyDefault()
	require.EqualValues(t, 9, cfg.MaxHops)
	require.Equal(t, 30*time.Second, cfg.PublishTimeout)
	require.Equal(t, 128, cfg.InboxWorkers)
	require.Len(t, cfg.DeliveryBackOff, 5)
}
