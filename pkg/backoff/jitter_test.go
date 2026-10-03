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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJitter_WithinBaseAndCap(t *testing.T) {
	base, capDur := 10*time.Millisecond, 200*time.Millisecond
	j := NewJitter(base, capDur)

	for range 50 {
		got := j.Next()
		require.GreaterOrEqual(t, got, base)
		require.LessOrEqual(t, got, capDur)
	}
}

// TestJitter_FirstCallBoundedByBaseTimesThree checks the core formula
// sleep = random_between(base, prev*3) for the very first draw, where
// prev == base. The cap is set far away so it never clips the range.
func TestJitter_FirstCallBoundedByBaseTimesThree(t *testing.T) {
	base, capDur := 10*time.Millisecond, time.Hour
	j := NewJitter(base, capDur)

	got := j.Next()
	require.GreaterOrEqual(t, got, base)
	require.LessOrEqual(t, got, base*3)
}

func TestJitter_NotMonotonic(t *testing.T) {
	// Decorrelated jitter is allowed to shrink back toward base even after
	// growing — unlike a plain doubling backoff. Run enough draws that,
	// with overwhelming probability, at least one decrease occurs; this
	// guards against an accidental regression to "always grows" logic.
	base, capDur := time.Millisecond, time.Second
	j := NewJitter(base, capDur)

	prev := j.Next()
	sawDecrease := false
	for range 200 {
		got := j.Next()
		if got < prev {
			sawDecrease = true
			break
		}
		prev = got
	}
	require.True(t, sawDecrease)
}

func TestJitter_Reset(t *testing.T) {
	base, capDur := 5*time.Millisecond, 500*time.Millisecond
	j := NewJitter(base, capDur)

	for range 5 {
		j.Next()
	}

	j.Reset()
	require.Equal(t, base, j.prev)

	// First call after reset is bounded the same way as a fresh Jitter.
	got := j.Next()
	require.GreaterOrEqual(t, got, base)
	require.LessOrEqual(t, got, base*3)
}

// TestJitter_NoPanicOnZeroSpan guards against a regression: when
// base == cap, the random range collapses to a single point.
// rand.N panics for n <= 0 if that's not special-cased.
func TestJitter_NoPanicOnZeroSpan(t *testing.T) {
	d := time.Millisecond
	j := NewJitter(d, d)
	for range 5 {
		got := j.Next()
		require.Equal(t, d, got)
	}
}

// TestJitter_NoPanicOnZeroBase documents that base == 0 is safe: prev*3
// stays 0, the overflow guard clamps upper to cap, and span > 0 lets
// rand.N run normally.
func TestJitter_NoPanicOnZeroBase(t *testing.T) {
	capDur := 100 * time.Millisecond
	j := NewJitter(0, capDur)
	for range 5 {
		got := j.Next()
		require.GreaterOrEqual(t, got, time.Duration(0))
		require.LessOrEqual(t, got, capDur)
	}
}

// TestJitter_BaseGreaterThanCap documents current (pre-existing, not
// introduced by this change) behavior when misconfigured with base > cap:
// the "upper must be >= base" guard wins, so Next() pins at base forever
// instead of respecting cap. Flagging this via a test rather than silently
// fixing it, since NewJitter doesn't validate base <= cap.
func TestJitter_BaseGreaterThanCap(t *testing.T) {
	base, capDur := 30*time.Second, 10*time.Second
	j := NewJitter(base, capDur)

	for range 5 {
		got := j.Next()
		require.Equal(t, base, got)
	}
}
