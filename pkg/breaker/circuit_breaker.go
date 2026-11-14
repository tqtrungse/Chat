/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package breaker

import (
	"context"
	"errors"

	"github.com/sony/gobreaker/v2"
)

const defaultConsecutiveFailures = 5

// circuit is the Circuit implementation. It wraps a gobreaker circuit breaker.
//
// The two-step form of gobreaker lets Execute run fn itself, without a
// recover. Therefore a panic from fn keeps its original stack.
type circuit struct {
	core *gobreaker.TwoStepCircuitBreaker[struct{}]
}

// New makes a circuit breaker with the settings in cfg.
//
// The isInfrastructureError function tells the circuit breaker which errors
// show a fault in the remote service. The circuit breaker counts only these
// errors as failures. It sends each other error to the caller, and the
// circuit stays closed. If isInfrastructureError is nil, the circuit breaker
// counts each error as a failure, except context.Canceled. A canceled ctx
// shows a fault in the caller, not in the remote service.
//
// The circuit breaker calls isInfrastructureError while it holds an internal
// lock. Therefore isInfrastructureError must not call Execute or State, and
// it must not block.
//
// The onStateChange function can be nil. If it is not nil, the circuit
// breaker calls it after each change of state. The circuit breaker holds an
// internal lock during the call. Therefore onStateChange must not call
// Execute or State, and it must not block. The arguments name, from, and to
// give all the data that a log message needs.
func New(
	cfg Config,
	isInfrastructureError func(error) bool,
	onStateChange func(name string, from, to State),
) Circuit {
	if cfg.ConsecutiveFailures == 0 {
		cfg.ConsecutiveFailures = defaultConsecutiveFailures
	}
	if isInfrastructureError == nil {
		isInfrastructureError = func(err error) bool {
			return !errors.Is(err, context.Canceled)
		}
	}

	settings := gobreaker.Settings{
		Name:        cfg.Name,
		MaxRequests: cfg.MaxRequests,
		Interval:    cfg.Interval,
		Timeout:     cfg.Timeout,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.ConsecutiveFailures >= cfg.ConsecutiveFailures
		},
		IsExcluded: func(err error) bool {
			if err == nil {
				return false
			}
			// A panic in fn is always a real failure. Execute
			// reports it as errPanic. The circuit breaker must
			// count this error, because isInfrastructureError
			// cannot identify it.
			if errors.Is(err, errPanic) {
				return false
			}
			return !isInfrastructureError(err)
		},
	}
	if onStateChange != nil {
		settings.OnStateChange = func(name string, from, to gobreaker.State) {
			onStateChange(name, convertState(from), convertState(to))
		}
	}

	return &circuit{
		core: gobreaker.NewTwoStepCircuitBreaker[struct{}](settings),
	}
}

// Execute implements Circuit.Execute. The Circuit interface gives the full
// contract.
func (c *circuit) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	// A ctx that is already done must not reach fn. The circuit breaker
	// does not count this error as a failure, because the fault is in the
	// caller and not in the remote service.
	err := ctx.Err()
	if err != nil {
		return err
	}
	// Allow returns only ErrOpenState or ErrTooManyRequests. An error from
	// fn never comes through this path. Thus, an error of a different
	// circuit breaker inside fn goes to the caller without a change.
	done, err := c.core.Allow()
	if err != nil {
		return ErrOpen
	}

	finished := false
	defer func() {
		// If fn did not return, it panicked or called runtime.Goexit.
		// Do not recover here. A recover removes the stack frames of fn.
		if !finished {
			done(errPanic)
		}
	}()
	err = fn(ctx)
	finished = true
	done(err)
	return err
}

// State implements Circuit.State.
func (c *circuit) State() State {
	return convertState(c.core.State())
}

func convertState(s gobreaker.State) State {
	switch s {
	case gobreaker.StateOpen:
		return StateOpen
	case gobreaker.StateHalfOpen:
		return StateHalfOpen
	case gobreaker.StateClosed:
		return StateClosed
	}
	// gobreaker has no other state. The closed state is the safe default.
	return StateClosed
}
