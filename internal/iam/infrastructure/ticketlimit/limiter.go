/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package ticketlimit

import (
	"math"
	randv2 "math/rand/v2"
	"sync"
	"time"

	core "xxx/internal/iam/core/ticketlimit"
)

var _ core.Limiter = (*Limiter)(nil)

// Limiter protects DB/signing work after authentication. Subject
// tokens intentionally cover every attempt, including device-limited requests,
// so one account cannot bypass its aggregate budget by rotating device IDs.
//
// Properties:
//   - Buckets are never evicted while active (tat > now): key churn cannot
//     reset anyone's quota. Full tables reject new keys instead.
//   - Fully replenished buckets (tat <= now) are reclaimed on the next access
//     to their table, releasing slots and per-subject device counters.
//   - Subject and device tables have separate capacities, so device churn
//     cannot occupy subject slots.
//   - Each subject may own at most maxDevicesPerSubject live device buckets.
//   - Constructor normalization protects callers that skip Config.Load.
type Limiter struct {
	mu                   sync.Mutex
	now                  func() time.Time
	base                 time.Time // origin of the int64 timeline
	last                 int64     // highest timeline value observed
	subjects             *ticketRateTable
	devices              *ticketRateTable
	devicesPerSubject    map[string]int // live device buckets per subject
	maxDevicesPerSubject int
}

// New creates an in-memory limiter with independent subject and device tables.
func New(cfg Config) *Limiter {
	return newWithClock(cfg, time.Now)
}

// newWithClock normalizes configuration and creates separate subject and device
// tables using an injectable clock for deterministic time-based tests.
func newWithClock(cfg Config, now func() time.Time) *Limiter {
	cfg.normalize()
	return &Limiter{
		now:  now,
		base: now(),
		subjects: newTicketRateTable(
			cfg.SubjectInterval,
			cfg.SubjectBurst,
			cfg.MaxSubjectEntries,
		),
		devices: newTicketRateTable(
			cfg.DeviceInterval,
			cfg.DeviceBurst,
			cfg.MaxDeviceEntries,
		),
		devicesPerSubject:    make(map[string]int),
		maxDevicesPerSubject: cfg.MaxDevicesPerSubject,
	}
}

// AllowSubject checks and consumes the aggregate subject quota.
func (l *Limiter) AllowSubject(subject string) core.Decision {
	return l.allow(ticketRateKey{subject: subject})
}

// AllowDevice checks and consumes the quota for a subject-device pair.
func (l *Limiter) AllowDevice(subject string, device uint64) core.Decision {
	return l.allow(ticketRateKey{subject: subject, device: device})
}

// allow serializes admission for one subject or subject-device key, reclaiming
// fully replenished buckets before checking storage limits and GCRA quota.
// Only accepted requests update the bucket TAT and heap position.
func (l *Limiter) allow(key ticketRateKey) core.Decision {
	l.mu.Lock()
	defer l.mu.Unlock()

	var (
		// Read the clock after locking so every decision sees a consistent now.
		now      = l.tick()
		table    = l.subjects
		limited  = core.SubjectLimited
		capacity = core.SubjectCapacity
	)

	if key.device != 0 {
		table = l.devices
		limited = core.DeviceLimited
		capacity = core.DeviceCapacity
	}
	l.prune(table, now)

	bucket, exists := table.entries[key]
	if !exists {
		// Check the user have the required number of bucket devices being monitored
		if key.device != 0 && l.devicesPerSubject[key.subject] >= l.maxDevicesPerSubject {
			return core.Decision{
				Reason:     core.SubjectDeviceCapacity,
				RetryAfter: capacityRetryAfter(),
			}
		}

		// Check table is full
		if len(table.entries) >= table.maxEntries {
			return core.Decision{
				Reason:     capacity,
				RetryAfter: capacityRetryAfter(),
			}
		}
	}

	var tat int64
	if exists {
		tat = bucket.tat
	}

	newTAT, wait := table.take(tat, now)
	if wait > 0 {
		return core.Decision{
			Reason:     limited,
			RetryAfter: ceilSeconds(wait),
		}
	}

	if exists {
		bucket.tat = newTAT // Strictly increases, so only a sift-down is needed.
		table.down(bucket.index)
		return core.Decision{}
	}

	// A fresh bucket always admits its first request (burst >= 1).
	bucket = &ticketRateBucket{
		key: key,
		tat: newTAT,
	}
	table.entries[key] = bucket
	table.push(bucket)
	if key.device != 0 {
		l.devicesPerSubject[key.subject]++
	}
	return core.Decision{}
}

// tick returns the current time on the int64 timeline. It never goes
// backwards: if the clock is stepped back, time stands still until it catches
// up, which is stricter than the configured limit but never looser.
func (l *Limiter) tick() int64 {
	n := int64(l.now().Sub(l.base)) // Uses the monotonic reading when present.
	if n < l.last {
		return l.last
	}
	l.last = n
	return n
}

// prune reclaims every bucket that is full again (tat <= now). The heap root
// is the smallest tat, so this stops at the first live bucket. Each pop is
// paid for by the insert that created the bucket: amortized O(log N).
func (l *Limiter) prune(table *ticketRateTable, now int64) {
	for len(table.heap) > 0 && table.heap[0].tat <= now {
		bucket := table.popMin()
		delete(table.entries, bucket.key)
		if bucket.key.device != 0 {
			l.devicesPerSubject[bucket.key.subject]--
			if l.devicesPerSubject[bucket.key.subject] <= 0 {
				delete(l.devicesPerSubject, bucket.key.subject)
			}
		}
	}
}

// ceilSeconds rounds a nanosecond retry delay up to whole seconds, clamping
// the result to the inclusive range 1..MaxInt32.
func ceilSeconds(wait int64) int {
	const sec = int64(time.Second)
	return int(min(max((wait+sec-1)/sec, 1), math.MaxInt32))
}

// capacityRetryAfter returns a random delay of 5..10 seconds to spread retries
// when bucket storage or the per-subject device-bucket limit is exhausted.
func capacityRetryAfter() int {
	return 5 + randv2.IntN(6) // 5..10 seconds, to spread retries under saturation.
}
