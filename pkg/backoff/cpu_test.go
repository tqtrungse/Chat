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

func TestCpu_Reset(t *testing.T) {
	b := Cpu{step: 5}

	b.Reset()
	require.Zero(t, b.step)
	require.False(t, b.IsDone())
}

func TestCpu_Reset_ZeroValueIsAlreadyReset(t *testing.T) {
	var b Cpu
	require.Zero(t, b.step)
}

func TestCpu_IsDone(t *testing.T) {
	tests := []struct {
		name string
		step int
		want bool
	}{
		{"zero step", 0, false},
		{"just below yieldLimit", yieldLimit - 1, false},
		{"exactly yieldLimit", yieldLimit, false},
		{"one above yieldLimit", yieldLimit + 1, true},
		{"well above yieldLimit", yieldLimit + 100, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := Cpu{step: tt.step}
			got := b.IsDone()
			require.Equal(t, tt.want, got)
		})
	}
}

// Spin must keep incrementing step by 1 on every call while step <= spinLimit,
// then stop advancing once step has moved past spinLimit.
func TestCpu_Spin_IncrementsUntilSpinLimit(t *testing.T) {
	var b Cpu

	for i := 1; i <= spinLimit+1; i++ {
		b.Spin()
		require.Equal(t, i, b.step)
	}

	// step is now spinLimit+1, which is > spinLimit, so it must not grow further.
	require.Equal(t, spinLimit+1, b.step)
}

func TestCpu_Spin_StopsGrowingPastSpinLimit(t *testing.T) {
	b := Cpu{step: spinLimit + 1}

	for range 20 {
		b.Spin()
		require.Equal(t, spinLimit+1, b.step)
	}
}

// Spin should never panic and should return promptly for a small cycle count.
func TestCpu_Spin_DoesNotBlock(t *testing.T) {
	var b Cpu

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range spinLimit + 5 {
			b.Spin()
		}
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		require.Fail(t, "Spin() loop did not complete within 1s; procYield may be hanging")
	}
}

// Snooze must increment step by exactly 1 on every call, regardless of
// which branch (procYield / Gosched / Sleep) was taken.
func TestCpu_Snooze_AlwaysIncrementsStep(t *testing.T) {
	var b Cpu

	// Walk far enough to exercise all three branches: the spin branch
	// (step <= spinLimit), the Gosched branch (spinLimit < step <= sleepAfter)
	// and the bounded-sleep branch (step > sleepAfter).
	const calls = sleepAfter + 5

	for i := 1; i <= calls; i++ {
		b.Snooze()
		require.Equal(t, i, b.step)
	}
}

// Per the comment in Snooze, step keeps growing past yieldLimit so Snooze can
// reach the sleep tier, but IsDone must still trip at the same yieldLimit
// threshold used elsewhere (e.g. Mpsc.Pop's decision to stop spinning).
func TestCpu_Snooze_IsDoneStillTripsAtYieldLimit(t *testing.T) {
	var b Cpu

	for b.step <= yieldLimit {
		require.False(t, b.IsDone())
		b.Snooze()
	}
	require.True(t, b.IsDone())
}

// Snooze must eventually reach the bounded real-sleep tier and must not sleep
// longer than sleepMax per call, so the whole loop finishes within a small
// bound instead of hanging.
func TestCpu_Snooze_SleepTierIsBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real-time Snooze test in -short mode")
	}

	b := Cpu{step: sleepAfter + 1} // already in the sleep tier

	const extraCalls = 6
	// Worst case: every call sleeps the max duration.
	maxTotal := sleepMax * time.Duration(extraCalls)

	start := time.Now()
	for range extraCalls {
		b.Snooze()
	}
	elapsed := time.Since(start)

	// Generous slack on top of the theoretical worst case to absorb
	// scheduler jitter, without allowing an unbounded/hanging sleep.
	slack := elapsed - maxTotal
	require.Less(t, slack, 50*time.Millisecond)
}

// Snooze should never panic when it runs on either the spinning or the
// blocking side of the multiOSThreads switch.
func TestCpu_Snooze_DoesNotPanicAcrossAllTiers(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			require.Fail(t, "Snooze() panicked: %v", r)
		}
	}()

	var b Cpu
	for range sleepAfter + 3 {
		b.Snooze()
	}
}
