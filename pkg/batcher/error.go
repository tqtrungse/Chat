/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package batcher

import (
	"errors"
	"fmt"
)

var (
	ErrCtxNil        = errors.New("root context is nil")
	ErrHashFuncNil   = errors.New("hash function is nil")
	ErrFlushFuncNil  = errors.New("flush function is nil")
	ErrResultFuncNil = errors.New("onResult function is nil")
)

type PanicSource uint8

const (
	PanicFlush    = PanicSource(1)
	PanicCallback = PanicSource(2)
)

// PanicError holds a panic value and its recovery stack.
// safeFlush passes this error to callbacks without a prior invocation after a Flush panic.
// OnPanic also receives PanicError values from safeFlush and answer.
type PanicError struct {
	Value  any
	Stack  []byte
	Source PanicSource
}

func (e *PanicError) Error() string {
	switch e.Source {
	case PanicFlush:
		return fmt.Sprintf("batcher: flush panicked: %v", e.Value)

	case PanicCallback:
		return fmt.Sprintf("batcher: callback panicked: %v", e.Value)

	default:
		return fmt.Sprintf("batcher: unknown panic: %v", e.Value)
	}
}

// Unwrap returns the panic value if the value is an error.
// Otherwise, it returns nil.
//
// Unwrap supports errors.Is and errors.As on an error panic value.
// For example, errors.Is can match context.DeadlineExceeded in that value.
//
// Runtime failures, such as division by zero, produce values that implement runtime.Error.
// An explicit panic call can also carry a runtime.Error value.
// A type match alone does not identify the source of the panic.
func (e *PanicError) Unwrap() error {
	err, ok := e.Value.(error)
	if ok {
		return err
	}
	return nil
}

// CloseError reports that the batcher rejected a request because it is closed.
// Submit returns it, and shutdown passes it to the callback of each rejected request.
//
// The batcher builds one CloseError per instance and reuses it for every rejection.
// err holds the root context error that caused the close.
type CloseError struct {
	err error
}

func (e *CloseError) Error() string {
	return fmt.Sprintf("batcher is closed: %s", e.err.Error())
}

// Unwrap returns the close sentinel and the root context error.
//
// Both errors.Is(err, ErrClosed) and a test against the root context error match.
// For example, errors.Is can match context.Canceled or context.DeadlineExceeded
// without a separate field access.
func (e *CloseError) Unwrap() error { return e.err }
