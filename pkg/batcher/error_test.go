/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package batcher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPanicError_Unwrap_NonErrorValue(t *testing.T) {
	e := &PanicError{Value: "plain string panic"}
	require.Nil(t, e.Unwrap())
}

func TestPanicError_Unwrap_NilValue(t *testing.T) {
	e := &PanicError{Value: nil}
	require.Nil(t, e.Unwrap())
}

func TestPanicError_Unwrap_NonErrorStruct(t *testing.T) {
	type payload struct{ msg string }
	e := &PanicError{Value: payload{msg: "boom"}}
	require.Nil(t, e.Unwrap())
}
