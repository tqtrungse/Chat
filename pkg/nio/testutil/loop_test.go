/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package testutil_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"xxx/pkg/nio/testutil"

	"github.com/stretchr/testify/require"
)

func TestLoopDefersCallbacksAndDrainsNestedWorkInOrder(t *testing.T) {
	loop := new(testutil.Loop)
	var got []int
	marker := new(int)
	ctx := context.WithValue(t.Context(), marker, "value")
	require.NoError(t, loop.Execute(ctx, func(ctx context.Context) error {
		require.Equal(t, "value", ctx.Value(marker))
		got = append(got, 1)
		return loop.Execute(ctx, func(context.Context) error { got = append(got, 3); return nil })
	}))
	require.NoError(t, loop.Execute(ctx, func(context.Context) error { got = append(got, 2); return nil }))
	require.Empty(t, got)
	loop.Drain()
	require.Equal(t, []int{1, 2, 3}, got)
	loop.Drain()
	require.Equal(t, []int{1, 2, 3}, got)
}

func TestLoopRejectsWithoutSchedulingOnError(t *testing.T) {
	want := errors.New("stopped")
	loop := &testutil.Loop{Err: want}
	called := false
	require.ErrorIs(t, loop.Execute(t.Context(), func(context.Context) error { called = true; return nil }), want)
	loop.Drain()
	require.False(t, called)
}

func TestLoopAcceptsConcurrentProducers(t *testing.T) {
	loop := new(testutil.Loop)
	const count = 64
	seen := make([]int, count)
	var wg sync.WaitGroup
	for i := range count {
		wg.Go(func() {
			require.NoError(t, loop.Execute(t.Context(), func(context.Context) error { seen[i]++; return nil }))
		})
	}
	wg.Wait()
	loop.Drain()
	for _, n := range seen {
		require.Equal(t, 1, n)
	}
}
