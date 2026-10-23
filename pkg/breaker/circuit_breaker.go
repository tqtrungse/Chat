/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package breaker

import (
	"context"
	"errors"

	"github.com/sony/gobreaker/v2"
)

const defaultConsecutiveFailures = 5

// State is the state of a circuit.
type State int

// These constants are the states of a circuit.
const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

type circuit struct {
	core *gobreaker.CircuitBreaker[struct{}]
}

// New makes a circuit breaker with the settings in cfg.
//
// The isInfrastructureError function tells the circuit breaker which errors
// show a fault in the remote service. The circuit breaker counts only these
// errors as failures. It sends each other error to the caller, and the
// circuit stays closed. If isInfrastructureError is nil, the circuit breaker
// counts each error as a failure.
//
// The onStateChange function can be nil. If it is not nil, the circuit
// breaker calls it after each change of state.
func New(
	cfg Config,
	isInfrastructureError func(error) bool,
	onStateChange func(name string, from, to gobreaker.State),
) Circuit {
	if cfg.ConsecutiveFailures == 0 {
		cfg.ConsecutiveFailures = defaultConsecutiveFailures
	}
	if isInfrastructureError == nil {
		isInfrastructureError = func(error) bool { return true }
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
			// recovers the panic, and puts the value in a
			// panicError. The circuit breaker must count this error,
			// also when isInfrastructureError cannot identify the
			// message.
			_, ok := errors.AsType[*panicError](err)
			if ok {
				return false
			}
			return !isInfrastructureError(err)
		},
		OnStateChange: onStateChange,
	}

	return &circuit{
		core: gobreaker.NewCircuitBreaker[struct{}](settings),
	}
}

// Execute runs fn when the circuit is in the closed state or in the half-open
// state. Execute then sends the result of fn to the circuit breaker.
//
// Execute returns ErrOpen when the circuit stops the call. In this condition,
// Execute does not run fn.
//
// Execute recovers a panic from fn before gobreaker receives the panic, and
// changes the value to a panicError. The circuit breaker then always counts
// the panic as a failure. Execute makes the same panic again with the
// original value. Therefore, the caller receives the panic without a change.
func (c *circuit) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	// The fn function can return the error of a different circuit breaker.
	// The called flag shows if this circuit breaker stopped the call, or if
	// fn ran and gave the error.
	called := false
	_, err := c.core.Execute(func() (result struct{}, err error) {
		called = true
		defer func() {
			if r := recover(); r != nil {
				err = &panicError{value: r}
			}
		}()
		return struct{}{}, fn(ctx)
	})

	pe, ok := errors.AsType[*panicError](err)
	if ok {
		panic(pe.value)
	}

	if !called && (errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests)) {
		return ErrOpen
	}
	return err
}

// State gives the state of the circuit breaker at this time.
func (c *circuit) State() State {
	switch c.core.State() {
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
