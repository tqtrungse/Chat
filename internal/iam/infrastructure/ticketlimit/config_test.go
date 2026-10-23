/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package ticketlimit

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
				SubjectInterval:      time.Second,
				SubjectBurst:         20,
				DeviceInterval:       5 * time.Second,
				DeviceBurst:          5,
				MaxEntries:           65536,
				MaxSubjectEntries:    16384,
				MaxDeviceEntries:     49152,
				MaxDevicesPerSubject: 5,
			},
			cfg,
		)
	}
}

func TestTicketRateLimitConfigLoad(t *testing.T) {
	l := viper.New()
	l.Set("HTTP_TICKET_SUBJECT_INTERVAL", "2s")
	l.Set("HTTP_TICKET_SUBJECT_BURST", 10)
	l.Set("HTTP_TICKET_DEVICE_INTERVAL", "10s")
	l.Set("HTTP_TICKET_DEVICE_BURST", 3)
	l.Set("HTTP_TICKET_RATE_MAX_ENTRIES", 100)

	var cfg Config
	cfg.Load(l)
	require.Equal(
		t,
		Config{
			SubjectInterval:      2 * time.Second,
			SubjectBurst:         10,
			DeviceInterval:       10 * time.Second,
			DeviceBurst:          3,
			MaxEntries:           100,
			MaxSubjectEntries:    25,
			MaxDeviceEntries:     75,
			MaxDevicesPerSubject: 5,
		},
		cfg,
	)
}

func TestTicketRateLimitExplicitCapacities(t *testing.T) {
	l := viper.New()
	l.Set("HTTP_TICKET_RATE_MAX_ENTRIES", 100)
	l.Set("HTTP_TICKET_MAX_SUBJECT_ENTRIES", 10)
	l.Set("HTTP_TICKET_MAX_DEVICE_ENTRIES", 50)
	l.Set("HTTP_TICKET_MAX_DEVICES_PER_SUBJECT", 16)

	var cfg Config
	cfg.Load(l)
	require.Equal(t, 10, cfg.MaxSubjectEntries)
	require.Equal(t, 50, cfg.MaxDeviceEntries)
	require.Equal(t, 16, cfg.MaxDevicesPerSubject)

	limiter := New(cfg)
	require.Equal(t, 10, limiter.subjects.maxEntries)
	require.Equal(t, 50, limiter.devices.maxEntries)
	require.Equal(t, 16, limiter.maxDevicesPerSubject)
}
