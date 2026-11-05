/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package buffer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPool_GetGivesEmptyBufferWithPooledSlice(t *testing.T) {
	buf := Get()
	require.NotNil(t, buf)
	assert.Equal(t, 0, buf.Len())
	assert.GreaterOrEqual(t, buf.Cap(), size)
}

func TestPool_FreeDetachesTheSlice(t *testing.T) {
	buf := Get()
	buf.AppendString("entry")
	buf.Free()

	// A nil slice is the marker of a buffer that is already free. The slice
	// went to the slice pool exactly one time.
	assert.Nil(t, buf.bs)
}

// TestPool_DoubleFreeGivesNoDuplicate covers the one hazard that the transfer
// of ownership adds. If put gave the same object to the pool two times, two
// callers would get one buffer and would write in the same bytes.
func TestPool_DoubleFreeGivesNoDuplicate(t *testing.T) {
	buf := Get()
	buf.AppendString("entry")
	buf.Free()

	assert.NotPanics(t, buf.Free, "a second Free must do nothing")
	assert.Nil(t, buf.bs)

	// The object went to the pool one time. Therefore, two calls of Get
	// cannot give the same object two times.
	//
	// sync.Pool gives no promise to keep an object, so this test shows the
	// presence of the fault and not the absence of it. A put without the
	// guard fails here in practice, because sync.Pool uses the cache of the
	// current P first.
	first := Get()
	second := Get()
	assert.NotSame(t, first, second)
}

func TestPool_RoundTripAfterGrowth(t *testing.T) {
	buf := Get()
	large := make([]byte, 8*size)
	for i := range large {
		large[i] = 'x'
	}
	_, err := buf.Write(large)
	require.NoError(t, err)
	require.Equal(t, len(large), buf.Len())

	// append gave a larger array. The slice pool accepts that array and puts
	// it in the bucket of its size.
	assert.Greater(t, buf.Cap(), size)
	assert.NotPanics(t, buf.Free)

	// The pool stays usable, and the next buffer is empty.
	next := Get()
	assert.Equal(t, 0, next.Len())
	assert.GreaterOrEqual(t, next.Cap(), size)
}

func TestPool_GetAfterFreeGivesCleanLength(t *testing.T) {
	for i := 0; i < 64; i++ {
		buf := Get()
		require.Equal(t, 0, buf.Len(), "iteration %d", i)
		buf.AppendString("some text for the entry")
		buf.Free()
	}
}
