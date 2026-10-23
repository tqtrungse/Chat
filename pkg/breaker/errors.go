/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package breaker

import (
	"errors"
	"fmt"
)

// ErrOpen is the error that Execute returns when the circuit stops the call.
// The circuit is in the open state, or it is in the half-open state and the
// maximum number of requests is already active. Execute does not run the
// function.
var ErrOpen = errors.New("circuit breaker open")

// panicError holds the value from a panic in the function that Execute
// runs.
//
// The private type lets IsExcluded in New identify a panic correctly, and
// always count the panic as a failure. gobreaker also recovers a panic, but
// gobreaker changes the value to a plain error from fmt.Errorf("%v", e). An
// error of that type looks the same as each other unknown error. Therefore
// the isInfrastructureError function cannot identify it.
type panicError struct {
	value any
}

func (e *panicError) Error() string {
	return fmt.Sprintf("panic: %v", e.value)
}
