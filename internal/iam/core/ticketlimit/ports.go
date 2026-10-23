/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

// Package ticketlimit defines the transport-independent connection-ticket admission contract.
package ticketlimit

// Reason identifies admission success or the quota/capacity that rejected it.
type Reason uint8

const (
	Allowed Reason = iota
	SubjectLimited
	DeviceLimited
	SubjectDeviceCapacity
	SubjectCapacity
	DeviceCapacity
)

// Decision describes admission and the suggested retry delay in whole seconds.
// Its zero value allows the request.
type Decision struct {
	Reason     Reason
	RetryAfter int
}

// Overloaded reports exhaustion of shared bucket storage rather than caller quota.
func (d Decision) Overloaded() bool {
	return d.Reason == SubjectCapacity || d.Reason == DeviceCapacity
}

// Limiter protects ticket issuance work. Implementations must support concurrent calls.
type Limiter interface {
	// AllowSubject consumes an aggregate subject attempt, even if later device
	// validation or admission fails. Call it before AllowDevice.
	AllowSubject(subject string) Decision
	// AllowDevice consumes a per-subject, per-device attempt after subject
	// admission. device must be nonzero; rejection does not refund the subject.
	AllowDevice(subject string, device uint64) Decision
}
