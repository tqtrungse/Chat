/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package breaker

import "time"

type Config struct {
	// Name identifies this circuit breaker in log messages and in metrics.
	Name string

	// MaxRequests is the maximum number of requests that the circuit
	// breaker sends to the remote service in the half-open state.
	//
	// Default: 1
	MaxRequests uint32

	// Interval is the length of time that the circuit breaker keeps its
	// counts in the closed state. At the end of each interval, the circuit
	// breaker sets the counts to 0.
	//
	// If Interval is 0 or less, the circuit breaker keeps the counts for
	// all the closed state.
	Interval time.Duration

	// Timeout is the length of time that the circuit breaker stays in the
	// open state. At the end of the timeout, the circuit breaker goes to
	// the half-open state.
	//
	// Default: 60s
	Timeout time.Duration

	// ConsecutiveFailures is a number of failed calls to the remote
	// service, one after the other, with no successful call between them.
	// A successful call sets this count to 0.
	//
	// When the count becomes equal to ConsecutiveFailures, the circuit
	// breaker goes from the closed state to the open state. It then stops
	// all calls to the remote service, and protects the system.
	//
	// Only an infrastructure error counts as a failure. New shows how the
	// circuit breaker identifies this type of error.
	//
	// Default: 5
	ConsecutiveFailures uint32
}
