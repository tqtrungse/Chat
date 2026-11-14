/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package breaker

import "errors"

// ErrOpen is the error that Execute returns when the circuit stops the call.
// The circuit is in the open state, or it is in the half-open state and the
// maximum number of requests is already active. Execute does not run the
// function.
var ErrOpen = errors.New("circuit breaker open")

// errPanic is the error that Execute reports to gobreaker when fn does not
// return. This occurs when fn panics or calls runtime.Goexit. The caller
// never receives errPanic.
//
// The private value lets IsExcluded in New identify this condition, and
// always count it as a failure. The isInfrastructureError function cannot
// identify it.
var errPanic = errors.New("breaker: fn did not return")
