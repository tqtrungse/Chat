/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connection

import (
	"sync"
	"testing"

	"xxx/internal/shared/device"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestConnectionRegistry_ReplaceLookupRemove(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	id := device.ID(101)
	first, firstState := newTestConn(t, ctrl)
	firstSession := newSession(&testChannel{}, id, 1)
	registry := newConnectionRegistry(2)
	old, err := registry.Replace(id, first, firstSession)
	require.NoError(t, err)
	require.Nil(t, old)
	require.EqualValues(t, 1, registry.CurConns())

	got, exists := registry.Lookup(id)
	require.True(t, exists)
	require.Same(t, first, got)
	require.Same(t, firstSession, first.Context())

	second, _ := newTestConn(t, ctrl)
	secondSession := newSession(&testChannel{}, id, 2)
	old, err = registry.Replace(id, second, secondSession)
	require.NoError(t, err)
	require.Same(t, first, old)
	require.True(t, firstSession.isClosed(), "replacement closes the previous session")
	require.EqualValues(t, 1, registry.CurConns(), "replacement inherits the previous slot")
	require.Zero(t, firstState.CloseCount.Load(), "registry marks the old session closed; lifecycle owns socket close")

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
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	registry := newConnectionRegistry(1)
	first, _ := newTestConn(t, ctrl)
	require.EqualValues(t, 1, registry.MaxConns())

	_, err := registry.Replace(1, first, newSession(&testChannel{}, 1, 0))
	require.NoError(t, err)

	second, _ := newTestConn(t, ctrl)
	_, err = registry.Replace(2, second, newSession(&testChannel{}, 2, 0))
	require.ErrorIs(t, err, ErrConnReachMax)
	require.Nil(
		t,
		second.Context(),
		"a rejected connection does not receive a session",
	)
	require.EqualValues(
		t,
		1,
		registry.CurConns(),
		"rejection does not change the actual count",
	)

	registry.Close()
	require.EqualValues(
		t,
		1,
		registry.CurConns(),
		"closing the registry preserves the actual count",
	)

	_, err = registry.Replace(
		3,
		second,
		newSession(&testChannel{}, 3, 0),
	)
	require.ErrorIs(t, err, ErrManagerClosed)

	_, removed := registry.Remove(first)
	require.True(t, removed)
	require.Zero(t, registry.CurConns())
}

func TestConnectionRegistry_UsesLoadedDefaultCapacity(t *testing.T) {
	var cfg Config
	cfg.Load(nil)
	registry := newConnectionRegistry(cfg.MaxConns)
	require.EqualValues(t, defaultMaxConns, registry.MaxConns())
	require.Zero(t, registry.CurConns())
}

func TestRegistryReplacementAtCapacityAndConcurrentOldRemoval(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for range 50 {
		r := newConnectionRegistry(1)
		old, _ := newTestConn(t, ctrl)
		next, _ := newTestConn(t, ctrl)
		_, err := r.Replace(
			42,
			old,
			newSession(&testChannel{}, 42, 0),
		)
		require.NoError(t, err)

		var wg sync.WaitGroup
		var replaceErr error
		wg.Add(2)

		go func() {
			defer wg.Done()
			_, replaceErr = r.Replace(
				42,
				next,
				newSession(&testChannel{}, 42, 0),
			)
		}()

		go func() {
			defer wg.Done()
			r.Remove(old)
		}()
		wg.Wait()

		require.NoError(t, replaceErr)
		got, ok := r.Lookup(42)

		require.True(t, ok)
		require.Same(t, next, got)
		require.EqualValues(t, 1, r.CurConns())

		_, removed := r.Remove(next)
		require.True(t, removed)
		require.Zero(t, r.CurConns())
	}
}

func TestRegistryConcurrentLookupDuringMutation(t *testing.T) {
	const writers = 8

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	var (
		wg          sync.WaitGroup
		registry    = newConnectionRegistry(writers)
		start       = make(chan struct{})
		stop        = make(chan struct{})
		readersDone = make(chan struct{})
	)

	go func() {
		defer close(readersDone)
		<-start
		for {
			select {
			case <-stop:
				return
			default:
				for id := range writers {
					registry.Lookup(device.ID(id + 1))
				}
			}
		}
	}()

	for id := range writers {
		conn, _ := newTestConn(t, ctrl)
		wg.Go(func() {
			<-start
			for range 100 {
				session := newSession(
					&testChannel{},
					device.ID(id+1),
					0,
				)

				_, err := registry.Replace(device.ID(id+1), conn, session)
				if err != nil {
					t.Error(err)
					return
				}
				registry.Remove(conn)
			}
		})
	}

	close(start)
	wg.Wait()
	close(stop)
	<-readersDone

	require.Zero(t, registry.CurConns())
	for id := range writers {
		_, exists := registry.Lookup(device.ID(id + 1))
		require.False(t, exists)
	}
}
