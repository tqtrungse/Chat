/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package redis

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArrAnyToU64s(t *testing.T) {
	in := []any{"123", nil, "not-a-number", "0"}
	got := arrAnyToU64s(in)
	want := []uint64{123, 0, 0, 0}
	require.Equal(t, len(want), len(got))
	for i := range want {
		require.Equal(t, want[i], got[i])
	}
}

func TestParseHeartbeatKey(t *testing.T) {
	id, ok := parseHeartbeatKey("hub:heartbeat:987654321")
	require.True(t, ok)
	require.Equal(t, uint64(987654321), id)

	_, ok = parseHeartbeatKey("some:other:key")
	require.False(t, ok)

	_, ok = parseHeartbeatKey("hub:heartbeat:not-a-number")
	require.False(t, ok)
}

func TestBucketFor(t *testing.T) {
	c, _ := newTestCache(t, 1)
	b := c.bucketFor(uint64(testBucketCount) * 7)
	require.Equal(t, uint32(0), b)

	b = c.bucketFor(uint64(testBucketCount)*7 + 1)
	require.Equal(t, uint32(1), b)
}
