/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
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
	// Execute returns the error of ctx, and does not run fn, when ctx is
	// done before the call. The circuit breaker does not count this error.
	//
	// Execute does not recover a panic from fn. The circuit breaker counts
	// the panic as a failure, and the panic continues to the caller. The
	// caller receives the original value and the original stack.
	//
	// The circuit breaker also counts a failure when fn calls
	// runtime.Goexit. Thus, the half-open state always gets its request
	// slot back.
	//
	// The ctx must apply to this one call only. The default
	// isInfrastructureError counts a deadline error as a failure of the
	// remote service. Therefore, a ctx with a deadline for many calls can
	// open the circuit incorrectly.
	Execute(ctx context.Context, fn func(ctx context.Context) error) error

	// State returns the state of the circuit breaker at this time. Use State
	// for a health check or for a metric that a scrape reads.
	//
	// Do not call State in the onStateChange function of New. This call
	// causes a deadlock.
	State() State
}
