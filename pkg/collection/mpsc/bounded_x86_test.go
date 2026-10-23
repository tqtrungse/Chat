//go:build 386 || amd64 || amd64p32

/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package mpsc

import (
	"testing"

	"xxx/pkg/cpu"

	"github.com/stretchr/testify/require"
)

func TestBounded_SendIdxWrap(t *testing.T) {
	for _, capacity := range []int{1, 2, 8, 256} {
		q := newBoundedNearWrap(t, capacity, 2)
		c := int(q.capacity)

		// Exercise Push/Pop across the index wrap boundary.
		for i := 0; i < 4*c+3; i++ {
			require.False(t, q.Push(i))
			v, closed := q.Pop()
			require.False(t, closed)
			require.Equal(t, i, v)
		}

		// Check full detection and FIFO order around the wrap boundary.
		q = newBoundedNearWrap(t, capacity, 2)
		for round := 0; round < 5; round++ {
			for i := 0; i < c; i++ {
				require.Truef(t, q.TryPush(i), "cap=%d round=%d i=%d", c, round, i)
			}
			require.Falsef(t, q.TryPush(-1), "cap=%d round=%d: must be full", c, round)
			for i := 0; i < c; i++ {
				v, ok := q.TryPop()
				require.True(t, ok)
				require.Equal(t, i, v)
			}
		}
	}
}

// Place the queue away from the wrap `laps` lap
// point (start based on capacity because capacity | 2^32).
func newBoundedNearWrap(t *testing.T, capacity int, laps uint32) *Bounded[int] {
	q, err := NewBounded[int](capacity)
	require.NoError(t, err)

	start := 0 - laps*q.capacity
	q.readIdx = start
	q.state.Store(uint64(start) << sendIdxShift)
	for j := uint32(0); j < q.capacity; j++ {
		idx := start + j
		q.ring[cpu.CacheRemap(idx, q.capacity, q.entrySize)].Cycle.Store((idx >> q.n) << 1)
	}
	return q
}
