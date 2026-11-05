/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
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
	// SubjectInterval is the time to refill one token in the bucket of a
	// subject. After a subject uses all of its burst, it can send one request
	// in each SubjectInterval. If the value is zero or negative, the default
	// applies.
	//
	// Default: 1s
	SubjectInterval time.Duration

	// SubjectBurst is the maximum number of tokens in the bucket of a subject.
	// A subject can send this number of requests quickly before the limit
	// stops it. Each request uses one subject token. This includes a request
	// that fails and a request that the device limit stops. Thus a subject
	// cannot get more requests when it changes its device IDs. If the value is
	// zero or negative, the default applies.
	//
	// Default: 20
	SubjectBurst int

	// DeviceInterval is the time to refill one token in the bucket of one
	// subject and device pair. If the value is zero or negative, the default
	// applies.
	//
	// Default: 5s
	DeviceInterval time.Duration

	// DeviceBurst is the maximum number of tokens in the bucket of one subject
	// and device pair. If the value is zero or negative, the default applies.
	//
	// Default: 5
	DeviceBurst int

	// MaxEntries is the total number of buckets for the subject table and the
	// device table together. It is the legacy setting. If MaxSubjectEntries or
	// MaxDeviceEntries is zero or negative, the limiter gives 1/4 of
	// MaxEntries to subjects and 3/4 of MaxEntries to devices. If the value is
	// less than 2, the default applies.
	//
	// Default: 65536
	MaxEntries int

	// MaxSubjectEntries is the maximum number of subject buckets in memory. If
	// the table is full, the limiter rejects a new subject, and the server
	// returns 503. The limiter does not remove an active bucket to make space.
	// It removes a bucket only when all of its tokens are back. It does this
	// at the next access to the table. If the value is zero or negative, the
	// value is MaxEntries/4, with a minimum of 1.
	//
	// Default: 16384 (MaxEntries/4)
	MaxSubjectEntries int

	// MaxDeviceEntries is the maximum number of device buckets in memory for
	// all subjects. If the table is full, the limiter rejects a new device,
	// and the server returns 503. The limiter removes buckets with the same
	// rule as MaxSubjectEntries. If the value is zero or negative, the value
	// is MaxEntries minus the subject share of MaxEntries.
	//
	// Default: 49152 (MaxEntries - MaxEntries/4)
	MaxDeviceEntries int

	// MaxDevicesPerSubject is the maximum number of device buckets that one
	// subject can have in memory at the same time. It does not limit the
	// number of enrolled devices. If a subject is at this limit, the limiter
	// rejects a new device of that subject, and the server returns 429. The
	// buckets that exist keep their quotas. If the value is zero or negative,
	// the default applies.
	//
	// Default: 5
	MaxDevicesPerSubject int
}

// LoadConfig preserves the deployed HTTP_TICKET_* configuration names and applies defaults.
func LoadConfig(loader config.Loader) Config {
	var c Config
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
	c.applyDefault()
	return c
}

// applyDefault applies quota defaults and derives missing table capacities from
// MaxEntries, preserving explicit subject and device capacity overrides.
func (c *Config) applyDefault() {
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
