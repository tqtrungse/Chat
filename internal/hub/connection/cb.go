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

package connection

import (
	"context"
	"errors"
)

// CbState is a type that represents a state of CircuitBreaker.
type CbState int

// CbErrOpen is returned when the circuit is open, or half-open with a probe
// already in flight, so the attempt did not run.
var CbErrOpen = errors.New("cb: circuit breaker open")

// These constants are states of CircuitBreaker.
const (
	CbStateClosed CbState = iota
	CbStateHalfOpen
	CbStateOpen
)

type CircuitBreaker interface {
	// Execute runs fn if the circuit allows it, and reports the result back to
	// the breaker automatically.
	//
	// Every context passed to Execute must derive from rootCtx.
	Execute(ctx context.Context, fn func(ctx context.Context) error) error

	// State returns the current breaker state — for logging, metrics, or a
	// health check endpoint.
	State() CbState
}
