//go:build 386 || amd64 || amd64p32

/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mpsc

import (
	"testing"

	"xxx/pkg"

	"github.com/stretchr/testify/require"
)

func TestBounded_SendIdxWrap(t *testing.T) {
	for _, capacity := range []int{1, 2, 8, 256} {
		q := newBoundedNearWrap(t, capacity, 2)
		c := int(q.capacity)

		// Push/Pop xuyên qua điểm wrap.
		for i := 0; i < 4*c+3; i++ {
			require.False(t, q.Push(i))
			v, closed := q.Pop()
			require.False(t, closed)
			require.Equal(t, i, v)
		}

		// Full detection + FIFO ngay quanh điểm wrap.
		q = newBoundedNearWrap(t, capacity, 2)
		for round := 0; round < 5; round++ {
			for i := 0; i < c; i++ {
				require.Truef(t, q.TryPush(i), "cap=%d round=%d i=%d", c, round, i)
			}
			require.Falsef(t, q.TryPush(-1), "cap=%d round=%d: phải đầy", c, round)
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
		q.ring[pkg.CacheRemap(idx, q.capacity, q.entrySize)].Cycle.Store((idx >> q.n) << 1)
	}
	return q
}