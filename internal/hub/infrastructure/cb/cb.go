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
	"fmt"

	"xxx/internal/hub/connection"

	"github.com/sony/gobreaker/v2"
)

// panicError wraps a value recovered from a panic inside a guarded call.
// It exists so the breaker's failure classifier (see NewRedisBreaker) can
// recognize it deterministically and never exclude it from accounting —
// unlike gobreaker's own built-in recover, which converts a panic into a
// plain fmt.Errorf("%v", e) that an infra-error classifier has no way to
// tell apart from any other unrecognized error.
type panicError struct{ value any }

func (e *panicError) Error() string { return fmt.Sprintf("panic: %v", e.value) }

type circuitBreaker struct {
	cb *gobreaker.CircuitBreaker[struct{}]
}

func New(
	cfg Config,
	isInfrastructureError func(error) bool,
	onStateChange func(name string, from, to gobreaker.State),
) connection.CircuitBreaker {
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

	return &circuitBreaker{
		cb: gobreaker.NewCircuitBreaker[struct{}](settings),
	}
}

// Execute runs fn if the circuit allows it, and reports the result back to
// the breaker automatically.
//
// A panic inside fn is recovered here — before gobreaker ever sees it —
// converted into a distinctly-typed error so the classifier above always
// counts it as a failure, and then re-panicked so the caller still
// observes it exactly as if Execute weren't in the call stack.
func (c *circuitBreaker) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	_, err := c.cb.Execute(func() (result struct{}, err error) {
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
		return connection.ErrCbOpen
	}
	return err
}

func (c *circuitBreaker) State() connection.CbState {
	switch c.cb.State() {
	case gobreaker.StateOpen:
		return connection.CbStateOpen
	case gobreaker.StateHalfOpen:
		return connection.CbStateHalfOpen
	case gobreaker.StateClosed:
		return connection.CbStateClosed
	}
	return connection.CbStateClosed
}
