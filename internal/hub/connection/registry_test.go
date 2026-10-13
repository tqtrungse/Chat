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

package connection

import (
	"testing"

	shareddevice "xxx/internal/shared/device"
	sharedsession "xxx/internal/shared/session"

	"github.com/stretchr/testify/require"
)

func TestConnectionRegistry_ReplaceLookupRemove(t *testing.T) {
	registry := newConnectionRegistry(2)
	id := shareddevice.ID(101)
	first, firstState := newTestConn()
	second, _ := newTestConn()
	firstSession := newSession(sharedsession.Keys{}, id, 1)
	secondSession := newSession(sharedsession.Keys{}, id, 2)

	old, err := registry.Replace(id, first, firstSession)
	require.NoError(t, err)
	require.Nil(t, old)
	require.EqualValues(t, 1, registry.CurConns())
	got, exists := registry.Lookup(id)
	require.True(t, exists)
	require.Same(t, first, got)
	require.Same(t, firstSession, first.Context())

	old, err = registry.Replace(id, second, secondSession)
	require.NoError(t, err)
	require.Same(t, first, old)
	require.True(t, firstSession.isClosed(), "replacement closes the previous session")
	require.EqualValues(t, 1, registry.CurConns(), "replacement inherits the previous slot")
	require.Zero(t, firstState.closeCount.Load(), "registry marks the old session closed; lifecycle owns socket close")

	got, exists = registry.Lookup(id)
	require.True(t, exists)
	require.Same(t, second, got)
	removedSession, removed := registry.Remove(first)
	require.False(t, removed, "a replaced connection cannot remove the active session")
	require.Nil(t, removedSession)
	require.EqualValues(t, 1, registry.CurConns())

	removedSession, removed = registry.Remove(second)
	require.True(t, removed)
	require.Same(t, secondSession, removedSession)
	require.True(t, secondSession.isClosed())
	require.EqualValues(t, 0, registry.CurConns())
	_, exists = registry.Lookup(id)
	require.False(t, exists)

	removedSession, removed = registry.Remove(second)
	require.False(t, removed, "removal is idempotent")
	require.Nil(t, removedSession)
}

func TestConnectionRegistry_EnforcesCapacityAndClosedState(t *testing.T) {
	registry := newConnectionRegistry(1)
	first, _ := newTestConn()
	second, _ := newTestConn()

	require.EqualValues(t, 1, registry.MaxConns())
	_, err := registry.Replace(1, first, newSession(sharedsession.Keys{}, 1, 0))
	require.NoError(t, err)

	_, err = registry.Replace(2, second, newSession(sharedsession.Keys{}, 2, 0))
	require.ErrorIs(t, err, ErrConnReachMax)
	require.Nil(t, second.Context(), "a rejected connection does not receive a session")
	require.EqualValues(t, 2, registry.CurConns(), "full capacity is reported as max plus one")

	registry.Close()
	require.EqualValues(t, 2, registry.CurConns(), "full capacity is reported as max plus one")
	_, err = registry.Replace(3, second, newSession(sharedsession.Keys{}, 3, 0))
	require.ErrorIs(t, err, ErrRouterClosed)

	_, removed := registry.Remove(first)
	require.True(t, removed)
	require.Zero(t, registry.CurConns())
}

func TestConnectionRegistry_DefaultsZeroCapacity(t *testing.T) {
	registry := newConnectionRegistry(0)
	require.EqualValues(t, defaultMaxConns, registry.MaxConns())
	require.Zero(t, registry.CurConns())
}
