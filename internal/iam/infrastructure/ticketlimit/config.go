/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package ticketlimit

import (
	"time"

	"xxx/pkg/config"
)

const (
	defaultSubjectInterval      = time.Second
	defaultSubjectBurst         = 20
	defaultDeviceInterval       = 5 * time.Second
	defaultDeviceBurst          = 5
	defaultMaxEntries           = 65536
	defaultMaxDevicesPerSubject = 5
)

// Config controls per-subject/device quotas and bounded bucket storage.
// MaxDevicesPerSubject limits tracked rate buckets, not enrolled devices.
type Config struct {
	SubjectInterval      time.Duration
	SubjectBurst         int
	DeviceInterval       time.Duration
	DeviceBurst          int
	MaxEntries           int
	MaxSubjectEntries    int
	MaxDeviceEntries     int
	MaxDevicesPerSubject int
}

// Load preserves the deployed HTTP_TICKET_* configuration names and applies defaults.
func (c *Config) Load(loader config.Loader) {
	if loader != nil {
		c.SubjectInterval = loader.GetDuration("HTTP_TICKET_SUBJECT_INTERVAL")
		c.SubjectBurst = loader.GetInt("HTTP_TICKET_SUBJECT_BURST")
		c.DeviceInterval = loader.GetDuration("HTTP_TICKET_DEVICE_INTERVAL")
		c.DeviceBurst = loader.GetInt("HTTP_TICKET_DEVICE_BURST")
		c.MaxEntries = loader.GetInt("HTTP_TICKET_RATE_MAX_ENTRIES")
		c.MaxSubjectEntries = loader.GetInt("HTTP_TICKET_MAX_SUBJECT_ENTRIES")
		c.MaxDeviceEntries = loader.GetInt("HTTP_TICKET_MAX_DEVICE_ENTRIES")
		c.MaxDevicesPerSubject = loader.GetInt("HTTP_TICKET_MAX_DEVICES_PER_SUBJECT")
	}
	c.normalize()
}

// normalize applies quota defaults and derives missing table capacities from
// MaxEntries, preserving explicit subject and device capacity overrides.
func (c *Config) normalize() {
	if c.SubjectInterval <= 0 {
		c.SubjectInterval = defaultSubjectInterval
	}
	if c.SubjectBurst <= 0 {
		c.SubjectBurst = defaultSubjectBurst
	}
	if c.DeviceInterval <= 0 {
		c.DeviceInterval = defaultDeviceInterval
	}
	if c.DeviceBurst <= 0 {
		c.DeviceBurst = defaultDeviceBurst
	}
	if c.MaxEntries < 2 {
		c.MaxEntries = defaultMaxEntries
	}
	// The legacy total setting supplies a fallback 1/4 subject, 3/4 device split.
	// Explicit table settings override their respective share independently.
	if c.MaxSubjectEntries <= 0 {
		c.MaxSubjectEntries = max(1, c.MaxEntries/4)
	}
	if c.MaxDeviceEntries <= 0 {
		c.MaxDeviceEntries = c.MaxEntries - max(1, c.MaxEntries/4)
	}
	if c.MaxDevicesPerSubject <= 0 {
		c.MaxDevicesPerSubject = defaultMaxDevicesPerSubject
	}
}
