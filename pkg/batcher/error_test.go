/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package batcher

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPanicError_Unwrap(t *testing.T) {
	type payload struct{ msg string }
	errBoom := errors.New("boom")

	cases := []struct {
		name  string
		value any
		want  error
	}{
		{
			name:  "error value",
			value: errBoom,
			want:  errBoom,
		},
		{
			name:  "non-error value",
			value: "plain string panic",
			want:  nil,
		},
		{
			name:  "nil value",
			value: nil,
			want:  nil,
		},
		{
			name:  "non-error struct",
			value: payload{msg: "boom"},
			want:  nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := &PanicError{Value: tc.value}
			require.Equal(t, tc.want, e.Unwrap())
		})
	}
}
