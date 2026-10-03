/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package backoff

import (
	"math/rand/v2"
	"time"
)

// Jitter implements based on the "Decorrelated Jitter" algorithm described in
// https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
type Jitter struct {
	// minimum delay (starting value and floor for every backoff)
	base time.Duration

	// maximum delay, the returned value never exceeds this
	cap time.Duration

	// delay returned by the previous Next() call, used as the basis for computing the next upper bound
	prev time.Duration
}

// NewJitter creates a new Jitter with the given base and cap.
func NewJitter(base, cap time.Duration) *Jitter {
	return &Jitter{
		base: base,
		cap:  cap,
		prev: base,
	}
}

// Next computes and returns the next delay using the decorrelated jitter
// formula: sleep = random_between(base, prev * 3), clamped to [base, cap].
func (b *Jitter) Next() time.Duration {
	upper := b.prev * 3
	// Guard against overflow (prev*3 wrapping negative) and against the
	// upper bound exceeding cap.
	if upper <= 0 || upper > b.cap {
		upper = b.cap
	}
	if upper < b.base {
		upper = b.base
	}

	span := int64(upper - b.base)
	d := b.base
	if span > 0 {
		d += rand.N(time.Duration(span) + 1)
	}

	b.prev = d
	return d
}

// Reset resets prev back to base, used to restart the backoff sequence
// from scratch (e.g. after a successful retry).
func (b *Jitter) Reset() {
	b.prev = b.base
}
