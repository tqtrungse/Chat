/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package jitter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJitter_WithinBaseAndCap(t *testing.T) {
	base := 10 * time.Millisecond
	capDur := 200 * time.Millisecond
	j, err := New(base, capDur)
	require.NoError(t, err)

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
	j, err := New(base, capDur)
	require.NoError(t, err)

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
	j, err := New(base, capDur)
	require.NoError(t, err)

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
	j, err := New(base, capDur)
	require.NoError(t, err)

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

// When base == cap, every sample clamps back to that same delay.
func TestJitter_EqualBaseAndCap(t *testing.T) {
	d := time.Millisecond
	j, err := New(d, d)
	require.NoError(t, err)
	for range 5 {
		got := j.Next()
		require.Equal(t, d, got)
	}
}

// Zero is an absorbing state in the AWS recurrence: random(0, 0*3) == 0.
func TestJitter_ZeroBase(t *testing.T) {
	for _, capDur := range []time.Duration{0, time.Second, time.Duration(1<<63 - 1)} {
		j, err := New(0, capDur)
		require.NoError(t, err)
		for range 5 {
			require.Zero(t, j.Next())
		}
		j.Reset()
		require.Zero(t, j.Next())
	}
}

func TestNewJitter_InvalidBounds(t *testing.T) {
	for _, tc := range []struct {
		name      string
		base, cap time.Duration
		want      error
	}{
		{
			name: "negative_base",
			base: -time.Second,
			cap:  time.Second,
			want: ErrBaseNegative,
		},
		{
			name: "base_over_cap",
			base: 30 * time.Second,
			cap:  10 * time.Second,
			want: ErrBaseOverCap,
		},
		{
			name: "negative_cap",
			cap:  -time.Second,
			want: ErrBaseOverCap,
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				j, err := New(tc.base, tc.cap)
				require.ErrorIs(t, err, tc.want)
				require.Nil(t, j)
			},
		)
	}
}

// Sampling [1s, 3s] before clamping to 2s gives a 1/2 chance of hitting cap.
// Truncating the sampling interval to [1s, 2s] almost never returns exactly cap.
func TestJitter_ClampsAfterSampling(t *testing.T) {
	j, err := New(time.Second, 2*time.Second)
	require.NoError(t, err)

	capped := false
	belowCap := false
	for range 100 {
		j.Reset()
		got := j.Next()
		require.GreaterOrEqual(t, got, time.Second)
		require.LessOrEqual(t, got, 2*time.Second)
		capped = capped || got == 2*time.Second
		belowCap = belowCap || got < 2*time.Second
	}
	// Each missing outcome has probability 2^-100 under the intended formula.
	require.True(t, capped, "results above cap must be clamped to cap")
	require.True(t, belowCap, "sampling must retain results below cap")
}

func TestJitter_LargeDurations(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	for _, base := range []time.Duration{
		1,
		maxDuration/3 + 1,         // prev*3 exceeds MaxInt64.
		6_148_914_691_236_517_205, // prev*3 == MaxUint64.
		7_000_000_000_000_000_000, // prev*3 requires 65 bits.
		maxDuration,
	} {
		t.Run(
			base.String(),
			func(t *testing.T) {
				j, err := New(base, maxDuration)
				require.NoError(t, err)
				for range 100 {
					got := j.Next()
					require.GreaterOrEqual(t, got, base)
					require.LessOrEqual(t, got, maxDuration)
				}
			},
		)
	}
}

func TestJitter_PositiveMultiplicationOverflowDoesNotPinAtBase(t *testing.T) {
	const base = time.Duration(7_000_000_000_000_000_000)
	const capDur = time.Duration(8_000_000_000_000_000_000)

	j, err := New(base, capDur)
	require.NoError(t, err)

	capped := false
	for range 100 {
		j.Reset()
		got := j.Next()
		require.GreaterOrEqual(t, got, base)
		require.LessOrEqual(t, got, capDur)
		capped = capped || got == capDur
	}
	// AWS samples [7e18, 21e18], so 13/14 of first draws clamp to 8e18.
	// Signed multiplication used to wrap positive and pin every draw at base.
	require.True(t, capped)
}
