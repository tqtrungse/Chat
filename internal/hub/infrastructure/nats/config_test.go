/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
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
		var cfg Config
		require.NoError(t, cfg.Load(loader))
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

	var cfg Config
	require.NoError(t, cfg.Load(l))
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
	var first, second Config
	require.NoError(t, first.Load(nil))
	require.NoError(t, second.Load(nil))
	first.DeliveryBackOff[0] = time.Hour
	require.Equal(t, 100*time.Millisecond, second.DeliveryBackOff[0])
}
