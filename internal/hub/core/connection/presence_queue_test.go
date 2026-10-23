/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connection

import (
	"context"
	"testing"

	"xxx/internal/hub/core/protocol"
	"xxx/internal/shared/device"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type orderedPresence struct {
	calls   []string
	addDone func(any, error)
}

func (p *orderedPresence) Add(_ context.Context, _ device.ID, _ int64, done func(any, error)) error {
	p.calls = append(p.calls, "add")
	p.addDone = done
	return nil
}

func (p *orderedPresence) Del(_ context.Context, _ device.ID, _ int64, done func(error)) error {
	p.calls = append(p.calls, "del")
	done(nil)
	return nil
}

func TestPresenceCompletionOrderAcrossCloseAndReconnect(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	cfg := Config{}
	cfg.Load(nil)

	presence := new(orderedPresence)
	manager := NewManager(
		cfg,
		presence,
		newTestLogger(t),
		protocol.NewEncoder(),
	)
	old, oldState := newTestConn(t, ctrl)
	next, nextState := newTestConn(t, ctrl)
	oldCallback := false

	require.NoError(
		t,
		manager.ActivateConn(
			t.Context(),
			42,
			&testChannel{},
			old,
			nil,
			func(error) { oldCallback = true },
		),
	)

	oldDone := presence.addDone
	manager.RemoveConn(t.Context(), old)

	require.NoError(
		t,
		manager.ActivateConn(
			t.Context(),
			42,
			&testChannel{},
			next,
			nil,
			func(error) {},
		),
	)
	require.Equal(
		t,
		[]string{"add"},
		presence.calls,
		"delete/reconnect must await the first Redis result",
	)

	oldDone(nil, nil)
	oldState.Loop.Drain()
	require.Equal(t, []string{"add", "del", "add"}, presence.calls)
	require.False(t, oldCallback, "old success cannot ACK the replacement")

	presence.addDone(nil, nil)
	nextState.Loop.Drain()
	manager.RemoveConn(t.Context(), next)
	require.Equal(t, []string{"add", "del", "add", "del"}, presence.calls)
	require.Empty(t, manager.presenceQueue.tasks, "completed device queues must be released")
}

func TestPresenceBacklogIsBoundedAndStillAllowsFinalCleanup(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	cfg := Config{MaxConns: 100}
	cfg.Load(nil)

	presence := new(orderedPresence)
	manager := NewManager(
		cfg,
		presence,
		newTestLogger(t),
		protocol.NewEncoder(),
	)
	first, firstState := newTestConn(t, ctrl)

	require.NoError(
		t,
		manager.ActivateConn(
			t.Context(),
			42,
			&testChannel{},
			first,
			nil,
			func(error) {}),
	)

	done := presence.addDone
	for range maxDevicePresenceBacklog - 1 {
		conn, _ := newTestConn(t, ctrl)
		require.NoError(
			t,
			manager.ActivateConn(
				t.Context(),
				42,
				&testChannel{},
				conn,
				nil,
				func(error) {}),
		)
	}

	rejected, _ := newTestConn(t, ctrl)
	ch := new(testChannel)
	releases := 0
	require.ErrorIs(
		t,
		manager.ActivateConn(
			t.Context(),
			42,
			ch,
			rejected,
			func() { releases++ },
			func(error) {},
		),
		ErrPresenceBusy,
	)
	require.Equal(t, 1, releases)
	require.True(t, ch.closed)
	require.Nil(t, rejected.Context())

	current, ok := manager.registry.Lookup(42)
	require.True(t, ok)

	manager.RemoveConn(t.Context(), current)
	done(nil, nil)
	firstState.Loop.Drain()
	require.Empty(t, manager.presenceQueue.tasks)
	require.Equal(
		t,
		[]string{"add", "del"},
		presence.calls,
		"obsolete queued adds must not reach Redis",
	)
}
