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

package cb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"xxx/internal/hub/connection"

	"github.com/sony/gobreaker/v2"
	"github.com/stretchr/testify/require"
)

func isInfra(target error) func(error) bool {
	return func(err error) bool {
		return errors.Is(err, target)
	}
}

func newTestBreaker(
	cfg Config,
	isInfraErr func(error) bool,
	onChange func(name string, from, to gobreaker.State),
) connection.CircuitBreaker {
	if onChange == nil {
		onChange = func(string, gobreaker.State, gobreaker.State) {}
	}
	return New(cfg, isInfraErr, onChange)
}

var (
	errBusiness  = errors.New("device not found") // stand-in for a non-infra error
	errCacheDown = errors.New("cache: connection refused")
)

func TestExecute_SuccessPassesCtxThrough(t *testing.T) {
	cb := newTestBreaker(Config{ConsecutiveFailures: 3}, func(error) bool { return true }, nil)

	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "value")

	var gotCtx context.Context
	err := cb.Execute(ctx, func(c context.Context) error {
		gotCtx = c
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, gotCtx)
	require.Equal(t, "value", gotCtx.Value(ctxKey{}))
	require.Equal(t, gobreaker.StateClosed, cb.State())
}

// This is the property the whole IsExcluded wiring exists for: an error the
// classifier doesn't recognize as infrastructure-related must never count
// toward tripping, no matter how many times it happens.
func TestExecute_NonInfraErrorsAreExcludedNeverTrip(t *testing.T) {
	cb := newTestBreaker(
		Config{ConsecutiveFailures: 2},
		func(error) bool { return false }, // nothing is ever classified as infra
		nil,
	)

	for range 10 { // far past ConsecutiveFailures=2
		err := cb.Execute(context.Background(), func(context.Context) error {
			return errBusiness
		})
		require.Equal(t, errBusiness, err)
	}
	require.Equal(t, gobreaker.StateClosed, cb.State())
}

func TestExecute_InfraErrorsTripAfterConsecutiveFailures(t *testing.T) {
	cb := newTestBreaker(
		Config{ConsecutiveFailures: 3},
		isInfra(errCacheDown),
		nil,
	)

	for range 3 {
		err := cb.Execute(context.Background(), func(context.Context) error {
			return errCacheDown
		})
		require.Equal(t, errCacheDown, err)
	}
	require.Equal(t, gobreaker.StateOpen, cb.State())
}

func TestExecute_OpenStateFailsFastWithoutCallingFn(t *testing.T) {
	cb := newTestBreaker(
		Config{ConsecutiveFailures: 1},
		isInfra(errCacheDown),
		nil,
	)
	_ = cb.Execute(context.Background(), func(context.Context) error { return errCacheDown }) // trip it
	require.Equal(t, gobreaker.StateOpen, cb.State())

	called := false
	err := cb.Execute(context.Background(), func(context.Context) error {
		called = true
		return nil
	})
	require.Equal(t, connection.ErrCbOpen, err)
	require.False(t, called)
}

func TestExecute_HalfOpenProbeSucceedsCloses(t *testing.T) {
	cb := newTestBreaker(
		// MaxRequests left at 0 -> gobreaker defaults it to 1, so a single
		// successful probe is enough to close.
		Config{ConsecutiveFailures: 1, Timeout: 20 * time.Millisecond},
		isInfra(errCacheDown),
		nil,
	)
	_ = cb.Execute(context.Background(), func(context.Context) error { return errCacheDown })
	require.Equal(t, gobreaker.StateOpen, cb.State())

	time.Sleep(30 * time.Millisecond) // let Timeout elapse

	err := cb.Execute(context.Background(), func(context.Context) error { return nil })
	require.NoError(t, err)
	require.Equal(t, gobreaker.StateClosed, cb.State())
}

func TestExecute_HalfOpenProbeFailsReopens(t *testing.T) {
	cb := newTestBreaker(
		Config{ConsecutiveFailures: 1, Timeout: 20 * time.Millisecond},
		isInfra(errCacheDown),
		nil,
	)
	_ = cb.Execute(context.Background(), func(context.Context) error { return errCacheDown })
	time.Sleep(30 * time.Millisecond)

	err := cb.Execute(context.Background(), func(context.Context) error { return errCacheDown })
	require.Equal(t, errCacheDown, err)
	require.Equal(t, gobreaker.StateOpen, cb.State())
}

// The concurrency gate is the entire point of half-open: only MaxRequests
// probes may be in flight at once, and gobreaker enforces this by
// incrementing Requests synchronously in beforeRequest before fn ever runs
// — so a second, genuinely concurrent probe must be rejected without its fn
// being called at all, not just eventually outvoted.
func TestExecute_HalfOpenRejectsConcurrentProbeBeyondMaxRequests(t *testing.T) {
	cb := newTestBreaker(
		Config{ConsecutiveFailures: 1, Timeout: 20 * time.Millisecond, MaxRequests: 1},
		isInfra(errCacheDown),
		nil,
	)
	_ = cb.Execute(context.Background(), func(context.Context) error { return errCacheDown })
	time.Sleep(30 * time.Millisecond)

	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)

	go func() {
		firstDone <- cb.Execute(context.Background(), func(context.Context) error {
			close(started)
			<-release
			return nil
		})
	}()

	<-started // first probe is confirmed admitted and in flight

	var secondCalled bool
	secondErr := cb.Execute(context.Background(), func(context.Context) error {
		secondCalled = true
		return nil
	})
	close(release)

	require.NoError(t, <-firstDone)
	require.Equal(t, connection.ErrCbOpen, secondErr)
	require.False(t, secondCalled)
	require.Equal(t, gobreaker.StateClosed, cb.State())
}

func TestExecute_PanicIsRecoveredAndRepanickedWithOriginalValue(t *testing.T) {
	cb := newTestBreaker(Config{ConsecutiveFailures: 5}, func(error) bool { return true }, nil)

	type sentinel struct{ msg string }
	want := sentinel{msg: "boom"}

	var got any
	func() {
		defer func() { got = recover() }()
		_ = cb.Execute(context.Background(), func(context.Context) error {
			panic(want)
		})
	}()

	gotVal, ok := got.(sentinel)
	require.True(t, ok)
	require.Equal(t, want, gotVal)
}

// This is the specific behavior panicError exists for: with a classifier
// that excludes everything (see TestExecute_NonInfraErrorsAreExcludedNeverTrip
// for the contrasting case with an ordinary error), a panic must still count
// as a failure and trip the breaker.
func TestExecute_PanicAlwaysCountsAsFailureEvenIfClassifierWouldExcludeIt(t *testing.T) {
	cb := newTestBreaker(
		Config{ConsecutiveFailures: 2},
		func(error) bool { return false },
		nil,
	)

	for range 2 {
		func() {
			defer func() { recover() }()
			_ = cb.Execute(context.Background(), func(context.Context) error {
				panic("boom")
			})
		}()
	}

	require.Equal(t, gobreaker.StateOpen, cb.State())
}

func TestExecute_OnStateChangeReportsTransitions(t *testing.T) {
	type transition struct{ from, to gobreaker.State }
	var mu sync.Mutex
	var got []transition

	cb := newTestBreaker(
		Config{ConsecutiveFailures: 1, Timeout: 20 * time.Millisecond},
		isInfra(errCacheDown),
		func(_ string, from, to gobreaker.State) {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, transition{from, to})
		},
	)

	_ = cb.Execute(context.Background(), func(context.Context) error { return errCacheDown }) // Closed -> Open
	time.Sleep(30 * time.Millisecond)
	_ = cb.Execute(context.Background(), func(context.Context) error { return nil }) // Open -> HalfOpen -> Closed, both within this one call

	mu.Lock()
	defer mu.Unlock()
	want := []transition{
		{gobreaker.StateClosed, gobreaker.StateOpen},
		{gobreaker.StateOpen, gobreaker.StateHalfOpen},
		{gobreaker.StateHalfOpen, gobreaker.StateClosed},
	}
	require.Equal(t, len(want), len(got))
	for i := range want {
		require.Equal(t, want[i], got[i])
	}
}
