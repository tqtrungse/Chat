/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package breaker

import (
	"context"
	"errors"
	"fmt"

	"github.com/sony/gobreaker/v2"
)

const defaultConsecutiveFailures = 5

// State is a type that represents a state of circuit.
type State int

// These constants are states of circuit.
const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

// ErrOpen is returned when the circuit is open, or half-open with a probe
// already in flight, so the attempt did not run.
var ErrOpen = errors.New("circuit breaker open")

// panicError wraps a value recovered from a panic inside a guarded call.
// It exists so the breaker's failure classifier (see New's IsExcluded) can
// recognize it deterministically and never exclude it from accounting —
// unlike gobreaker's own built-in recover, which converts a panic into a
// plain fmt.Errorf("%v", e) that an infra-error classifier has no way to
// tell apart from any other unrecognized error.
type panicError struct{ value any }

func (e *panicError) Error() string { return fmt.Sprintf("panic: %v", e.value) }

type circuit struct {
	core *gobreaker.CircuitBreaker[struct{}]
}

func New(
	cfg Config,
	isInfrastructureError func(error) bool,
	onStateChange func(name string, from, to gobreaker.State),
) Circuit {
	consecutiveFailures := cfg.ConsecutiveFailures
	if consecutiveFailures == 0 {
		consecutiveFailures = defaultConsecutiveFailures
	}

	settings := gobreaker.Settings{
		Name:        cfg.Name,
		MaxRequests: cfg.MaxRequests,
		Interval:    cfg.Interval,
		Timeout:     cfg.Timeout,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.ConsecutiveFailures >= consecutiveFailures
		},
		IsExcluded: func(err error) bool {
			if err == nil {
				return false
			}
			// A panic we recovered ourselves (see Execute) is always a
			// real failure — never excluded, regardless of what the
			// infra classifier below would make of its message.
			var pe *panicError
			if errors.As(err, &pe) {
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

// Execute runs fn if the circuit allows it, and reports the result back to
// the breaker automatically.
//
// A panic inside fn is recovered here — before gobreaker ever sees it —
// converted into a distinctly-typed error so the classifier above always
// counts it as a failure, and then re-panicked so the caller still
// observes it exactly as if Execute weren't in the call stack.
func (c *circuit) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	_, err := c.core.Execute(func() (result struct{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = &panicError{value: r}
			}
		}()
		return struct{}{}, fn(ctx)
	})

	var pe *panicError
	if errors.As(err, &pe) {
		panic(pe.value)
	}

	if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
		return ErrOpen
	}
	return err
}

func (c *circuit) State() State {
	switch c.core.State() {
	case gobreaker.StateOpen:
		return StateOpen
	case gobreaker.StateHalfOpen:
		return StateHalfOpen
	case gobreaker.StateClosed:
		return StateClosed
	}
	return StateClosed
}
