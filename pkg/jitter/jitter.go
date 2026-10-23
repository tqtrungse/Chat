/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package jitter

import (
	"math/bits"
	"math/rand/v2"
	"time"
)

// Delay implements the "Decorrelated Delay" algorithm described in
// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
type Delay struct {
	// minimum delay (starting value and floor for every backoff)
	base time.Duration

	// maximum delay, the returned value never exceeds this
	cap time.Duration

	// delay returned by the previous Next() call, used as the basis for computing the next upper bound
	prev time.Duration
}

// New creates a new Delay with 0 <= base <= cap.
// A zero base produces zero delay on every call, including after Reset.
func New(base time.Duration, cap time.Duration) (*Delay, error) {
	if base < 0 {
		return nil, ErrBaseNegative
	}
	if base > cap {
		return nil, ErrBaseOverCap
	}
	return &Delay{
		base: base,
		cap:  cap,
		prev: base,
	}, nil
}

// Next computes and returns the next delay using the decorrelated jitter
// formula: sleep = min(cap, random_between(base, prev * 3)). The random
// interval includes both endpoints; the cap is applied after sampling.
func (b *Delay) Next() time.Duration {
	var (
		upperHi uint64
		upperLo uint64
		sample  uint64
	)

	// A non-negative duration fits in 63 bits, so prev*3 needs at most
	// 65 bits. Keep the full upper bound to preserve the AWS distribution.
	upperHi, upperLo = bits.Mul64(uint64(b.prev), 3)

	if upperHi == 0 {
		width := upperLo - uint64(b.base) + 1
		if width == 0 {
			// The inclusive interval [0, MaxUint64] has 2^64 values.
			sample = rand.Uint64()
		} else {
			sample = uint64(b.base) + rand.Uint64N(width)
		}
	} else {
		// Sample uniformly from 65 bits and reject values outside the
		// interval. Here upperHi is 1; any accepted high-bit value exceeds
		// every valid duration cap and can be clamped before conversion.
		for {
			hi, lo := rand.Uint64N(2), rand.Uint64()
			if hi == 1 && lo > upperLo || hi == 0 && lo < uint64(b.base) {
				continue
			}
			if hi == 1 {
				b.prev = b.cap
				return b.prev
			}
			sample = lo
			break
		}
	}

	// Clamp while still unsigned, before converting to time.Duration.
	b.prev = time.Duration(min(sample, uint64(b.cap)))
	return b.prev
}

// Reset resets prev back to base, used to restart the backoff sequence
// from scratch (e.g. after a successful retry).
func (b *Delay) Reset() {
	b.prev = b.base
}
