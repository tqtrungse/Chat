/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package breaker

// State is the state of a circuit.
type State int

// These constants are the states of a circuit.
const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

// String returns the name of the state: "closed", "half-open", or "open". For
// a value that is not a state, String returns "unknown". Use String in log
// messages and in metric labels.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	}
	return "unknown"
}
