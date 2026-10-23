/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package breaker

import "context"

// Circuit is a circuit breaker. It protects one remote service.
type Circuit interface {
	// Execute runs fn when the circuit is in the closed state or in the
	// half-open state. Execute then sends the result of fn to the circuit
	// breaker.
	//
	// Execute returns ErrOpen when the circuit stops the call. In this
	// condition, Execute does not run fn.
	//
	// The ctx must apply to this one call only. The circuit breaker counts
	// a deadline error as a failure of the remote service. Therefore, a ctx
	// with a deadline for many calls can open the circuit incorrectly.
	Execute(ctx context.Context, fn func(ctx context.Context) error) error

	// State gives the state of the circuit breaker at this time. Use State
	// for log messages, for metrics, or for a health check.
	State() State
}
