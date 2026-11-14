/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package breaker

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"testing"
	"time"

	"github.com/sony/gobreaker/v2"
	"github.com/stretchr/testify/require"
)

var (
	// errBusiness is an example of an error that is not an infrastructure
	// error.
	errBusiness  = errors.New("device not found")
	errCacheDown = errors.New("cache: connection refused")
)

func isInfra(target error) func(error) bool {
	return func(err error) bool {
		return errors.Is(err, target)
	}
}

func newTestBreaker(
	cfg Config,
	isInfraErr func(error) bool,
	onChange func(name string, from, to State),
) Circuit {
	if onChange == nil {
		onChange = func(string, State, State) {}
	}
	return New(cfg, isInfraErr, onChange)
}

func TestState_String(t *testing.T) {
	require.Equal(t, "closed", StateClosed.String())
	require.Equal(t, "half-open", StateHalfOpen.String())
	require.Equal(t, "open", StateOpen.String())
	require.Equal(t, "unknown", State(99).String())
}

func TestExecute_SuccessPassesCtxThrough(t *testing.T) {
	cb := newTestBreaker(
		Config{ConsecutiveFailures: 3},
		func(error) bool { return true },
		nil,
	)

	type ctxKey struct{}
	ctx := context.WithValue(t.Context(), ctxKey{}, "value")

	var gotCtx context.Context
	err := cb.Execute(ctx, func(c context.Context) error {
		gotCtx = c
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, gotCtx)
	require.Equal(t, "value", gotCtx.Value(ctxKey{}))
	require.Equal(t, StateClosed, cb.State())
}

// A ctx that is done before the call must stop Execute. Execute must not run
// fn, and the circuit breaker must not count the error as a failure.
func TestExecute_DoneCtxSkipsFnAndIsNotCounted(t *testing.T) {
	cb := newTestBreaker(
		Config{ConsecutiveFailures: 1},
		func(error) bool { return true },
		nil,
	)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	called := false
	err := cb.Execute(ctx, func(context.Context) error {
		called = true
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, called)
	require.Equal(t, StateClosed, cb.State())
}

// IsExcluded exists for this property. If isInfrastructureError does not
// identify an error, that error must never open the circuit. The number of
// times that the error occurs makes no difference.
func TestExecute_NonInfraErrorsAreExcludedNeverTrip(t *testing.T) {
	cb := newTestBreaker(
		Config{ConsecutiveFailures: 2},
		func(error) bool { return false }, // no error is an infrastructure error
		nil,
	)

	for range 10 { // The loop runs more times than ConsecutiveFailures=2.
		err := cb.Execute(
			t.Context(),
			func(context.Context) error {
				return errBusiness
			},
		)
		require.Equal(t, errBusiness, err)
	}
	require.Equal(t, StateClosed, cb.State())
}

func TestExecute_InfraErrorsTripAfterConsecutiveFailures(t *testing.T) {
	cb := newTestBreaker(
		Config{ConsecutiveFailures: 3},
		isInfra(errCacheDown),
		nil,
	)

	for range 3 {
		err := cb.Execute(
			t.Context(),
			func(context.Context) error {
				return errCacheDown
			},
		)
		require.Equal(t, errCacheDown, err)
	}
	require.Equal(t, StateOpen, cb.State())
}

func TestExecute_OpenStateFailsFastWithoutCallingFn(t *testing.T) {
	cb := newTestBreaker(
		Config{ConsecutiveFailures: 1},
		isInfra(errCacheDown),
		nil,
	)
	_ = cb.Execute(
		t.Context(),
		func(context.Context) error { return errCacheDown },
	) // open the circuit
	require.Equal(t, StateOpen, cb.State())

	called := false
	err := cb.Execute(
		t.Context(),
		func(context.Context) error {
			called = true
			return nil
		},
	)
	require.Equal(t, ErrOpen, err)
	require.False(t, called)
}

func TestExecute_PreservesCallbackBreakerErrors(t *testing.T) {
	for _, tt := range []struct {
		name     string
		sentinel error
		returned error
	}{
		{
			name:     "open state",
			sentinel: gobreaker.ErrOpenState,
			returned: gobreaker.ErrOpenState,
		},
		{
			name:     "wrapped open state",
			sentinel: gobreaker.ErrOpenState,
			returned: fmt.Errorf("inner dependency: %w", gobreaker.ErrOpenState),
		},
		{
			name:     "too many requests",
			sentinel: gobreaker.ErrTooManyRequests,
			returned: gobreaker.ErrTooManyRequests,
		},
		{
			name:     "wrapped too many requests",
			sentinel: gobreaker.ErrTooManyRequests,
			returned: fmt.Errorf("inner dependency: %w", gobreaker.ErrTooManyRequests),
		},
	} {
		t.Run(
			tt.name,
			func(t *testing.T) {
				cb := newTestBreaker(
					Config{ConsecutiveFailures: 1},
					func(error) bool { return false },
					nil,
				)

				calls := 0
				err := cb.Execute(
					t.Context(),
					func(context.Context) error {
						calls++
						return tt.returned
					},
				)

				require.Equal(t, 1, calls)
				require.Same(t, tt.returned, err)
				require.ErrorIs(t, err, tt.sentinel)
				require.NotErrorIs(t, err, ErrOpen)
				require.Equal(t, StateClosed, cb.State())
			},
		)
	}
}

func TestExecute_HalfOpenProbeSucceedsCloses(t *testing.T) {
	cb := newTestBreaker(
		// MaxRequests stays at 0, and gobreaker changes the value to 1.
		// One successful request is then enough to close the circuit.
		Config{
			ConsecutiveFailures: 1,
			Timeout:             20 * time.Millisecond,
		},
		isInfra(errCacheDown),
		nil,
	)
	_ = cb.Execute(
		t.Context(),
		func(context.Context) error { return errCacheDown },
	)
	require.Equal(t, StateOpen, cb.State())

	time.Sleep(30 * time.Millisecond) // wait for the end of Timeout

	err := cb.Execute(
		t.Context(),
		func(context.Context) error { return nil },
	)
	require.NoError(t, err)
	require.Equal(t, StateClosed, cb.State())
}

func TestExecute_HalfOpenProbeFailsReopens(t *testing.T) {
	cb := newTestBreaker(
		Config{
			ConsecutiveFailures: 1,
			Timeout:             20 * time.Millisecond,
		},
		isInfra(errCacheDown),
		nil,
	)
	_ = cb.Execute(
		t.Context(),
		func(context.Context) error { return errCacheDown },
	)
	time.Sleep(30 * time.Millisecond)

	err := cb.Execute(
		t.Context(),
		func(context.Context) error { return errCacheDown },
	)
	require.Equal(t, errCacheDown, err)
	require.Equal(t, StateOpen, cb.State())
}

// A limit on the number of requests is the purpose of the half-open state.
// Only MaxRequests requests can be active at the same time. gobreaker
// increases the Requests count in beforeRequest, and does this before fn
// runs. Therefore the circuit breaker must reject a second request that
// starts at the same time. It must not run the fn of that request.
func TestExecute_HalfOpenRejectsConcurrentProbeBeyondMaxRequests(t *testing.T) {
	cb := newTestBreaker(
		Config{
			ConsecutiveFailures: 1,
			Timeout:             20 * time.Millisecond,
			MaxRequests:         1,
		},
		isInfra(errCacheDown),
		nil,
	)
	_ = cb.Execute(
		t.Context(),
		func(context.Context) error { return errCacheDown },
	)
	time.Sleep(30 * time.Millisecond)

	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)

	go func() {
		firstDone <- cb.Execute(
			t.Context(),
			func(context.Context) error {
				close(started)
				<-release
				return nil
			},
		)
	}()

	<-started // the first request is now active

	var secondCalled bool
	secondErr := cb.Execute(
		t.Context(),
		func(context.Context) error {
			secondCalled = true
			return nil
		},
	)
	close(release)

	require.NoError(t, <-firstDone)
	require.Equal(t, ErrOpen, secondErr)
	require.False(t, secondCalled)
	require.Equal(t, StateClosed, cb.State())
}

func TestExecute_PanicReachesCallerWithOriginalValue(t *testing.T) {
	cb := newTestBreaker(
		Config{ConsecutiveFailures: 5},
		func(error) bool { return true },
		nil,
	)

	type sentinel struct{ msg string }
	want := sentinel{msg: "boom"}

	var got any
	func() {
		defer func() { got = recover() }()
		_ = cb.Execute(
			t.Context(),
			func(context.Context) error {
				panic(want)
			},
		)
	}()

	gotVal, ok := got.(sentinel)
	require.True(t, ok)
	require.Equal(t, want, gotVal)
}

// errPanic exists for this behavior. The isInfrastructureError function
// excludes each error in this test, but a panic must still count as a
// failure and open the circuit. The test
// TestExecute_NonInfraErrorsAreExcludedNeverTrip shows the different result
// for an ordinary error.
func TestExecute_PanicAlwaysCountsAsFailureEvenIfClassifierWouldExcludeIt(t *testing.T) {
	cb := newTestBreaker(
		Config{ConsecutiveFailures: 2},
		func(error) bool { return false },
		nil,
	)

	for range 2 {
		func() {
			defer func() { _ = recover() }()

			_ = cb.Execute(
				t.Context(),
				func(context.Context) error {
					panic("boom")
				},
			)
		}()
	}

	require.Equal(t, StateOpen, cb.State())
}

// panicInFn is a named function, so that the test can find its frame in a
// stack trace.
//
//go:noinline
func panicInFn() {
	panic("boom")
}

// The caller must see the stack frames of fn when it recovers the panic. A
// recover inside Execute removes these frames.
func TestExecute_PanicKeepsOriginalStack(t *testing.T) {
	cb := newTestBreaker(
		Config{ConsecutiveFailures: 5},
		func(error) bool { return true },
		nil,
	)

	var stack string
	func() {
		defer func() {
			if recover() != nil {
				stack = string(debug.Stack())
			}
		}()
		_ = cb.Execute(
			t.Context(),
			func(context.Context) error {
				panicInFn()
				return nil
			},
		)
	}()

	require.Contains(t, stack, "panicInFn")
}

// A call to runtime.Goexit in fn must not keep the half-open request slot.
// The circuit breaker must count the call as a failure and go to the open
// state. A later probe must then be able to close the circuit.
func TestExecute_GoexitInHalfOpenDoesNotBlockCircuit(t *testing.T) {
	cb := newTestBreaker(
		Config{
			ConsecutiveFailures: 1,
			Timeout:             20 * time.Millisecond,
			MaxRequests:         1,
		},
		isInfra(errCacheDown),
		nil,
	)
	_ = cb.Execute(
		t.Context(),
		func(context.Context) error { return errCacheDown },
	)
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, StateHalfOpen, cb.State())

	exited := make(chan struct{})
	go func() {
		defer close(exited)
		_ = cb.Execute(
			t.Context(),
			func(context.Context) error {
				runtime.Goexit()
				return nil
			},
		)
	}()
	<-exited

	require.Equal(t, StateOpen, cb.State())

	time.Sleep(30 * time.Millisecond)
	err := cb.Execute(
		t.Context(),
		func(context.Context) error { return nil },
	)
	require.NoError(t, err)
	require.Equal(t, StateClosed, cb.State())
}

// The default isInfrastructureError must not count context.Canceled. The
// caller canceled the call, and the remote service has no fault. Each other
// error must count as a failure.
func TestNew_NilClassifierExcludesOnlyContextCanceled(t *testing.T) {
	cb := New(Config{ConsecutiveFailures: 2}, nil, nil)

	for range 10 { // The loop runs more times than ConsecutiveFailures=2.
		err := cb.Execute(
			t.Context(),
			func(context.Context) error {
				return fmt.Errorf("redis: %w", context.Canceled)
			},
		)
		require.ErrorIs(t, err, context.Canceled)
	}
	require.Equal(t, StateClosed, cb.State())

	for range 2 {
		_ = cb.Execute(
			t.Context(),
			func(context.Context) error { return errBusiness },
		)
	}
	require.Equal(t, StateOpen, cb.State())
}

// In the half-open state, the circuit closes only after MaxRequests
// successful calls, one after the other.
func TestExecute_HalfOpenNeedsMaxRequestsSuccessesToClose(t *testing.T) {
	cb := newTestBreaker(
		Config{
			ConsecutiveFailures: 1,
			Timeout:             20 * time.Millisecond,
			MaxRequests:         2,
		},
		isInfra(errCacheDown),
		nil,
	)
	_ = cb.Execute(
		t.Context(),
		func(context.Context) error { return errCacheDown },
	)
	time.Sleep(30 * time.Millisecond)

	err := cb.Execute(t.Context(), func(context.Context) error { return nil })
	require.NoError(t, err)
	require.Equal(t, StateHalfOpen, cb.State())

	err = cb.Execute(t.Context(), func(context.Context) error { return nil })
	require.NoError(t, err)
	require.Equal(t, StateClosed, cb.State())
}

// A nil onStateChange is valid. The circuit breaker must not call it when the
// state changes. newTestBreaker replaces nil, so this test calls New.
func TestNew_NilOnStateChangeDoesNotPanic(t *testing.T) {
	cb := New(Config{ConsecutiveFailures: 1}, isInfra(errCacheDown), nil)

	err := cb.Execute(
		t.Context(),
		func(context.Context) error { return errCacheDown }, // Closed -> Open
	)
	require.Equal(t, errCacheDown, err)
	require.Equal(t, StateOpen, cb.State())
}

func TestExecute_OnStateChangeReportsTransitions(t *testing.T) {
	type transition struct {
		from State
		to   State
	}

	var mu sync.Mutex
	var got []transition

	cb := newTestBreaker(
		Config{
			ConsecutiveFailures: 1,
			Timeout:             20 * time.Millisecond,
		},
		isInfra(errCacheDown),
		func(_ string, from, to State) {
			mu.Lock()
			defer mu.Unlock()
			got = append(got, transition{from, to})
		},
	)

	_ = cb.Execute(
		t.Context(),
		func(context.Context) error { return errCacheDown }, // Closed -> Open
	)
	time.Sleep(30 * time.Millisecond)
	_ = cb.Execute(
		t.Context(),
		// Open -> HalfOpen -> Closed. Both changes occur in this one call.
		func(context.Context) error { return nil },
	)

	mu.Lock()
	defer mu.Unlock()

	want := []transition{
		{
			StateClosed,
			StateOpen,
		},
		{
			StateOpen,
			StateHalfOpen,
		},
		{
			StateHalfOpen,
			StateClosed,
		},
	}
	require.Equal(t, len(want), len(got))
	for i := range want {
		require.Equal(t, want[i], got[i])
	}
}
