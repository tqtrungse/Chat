/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connection

import (
	"context"
	"errors"
	"testing"
	"time"

	pbpub "xxx/api/hub/v2/proto/gen/pub"
	"xxx/internal/hub/core/protocol"
	"xxx/internal/shared/device"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type testPresence struct {
	add    func(any, error)
	addErr error
	dels   []device.ID
}

func (p *testPresence) Add(_ context.Context, _ device.ID, _ int64, cb func(any, error)) error {
	p.add = cb
	return p.addErr
}

func (p *testPresence) Del(_ context.Context, id device.ID, _ int64, cb func(error)) error {
	p.dels = append(p.dels, id)
	cb(nil)
	return nil
}

func TestManagerRegistrationACKOrderRemovalAndCapacity(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	cfg := Config{
		MaxConns:       2,
		TickerDuration: 2 * time.Second,
	}
	cfg.Load(nil)

	presence := new(testPresence)
	manager := NewManager(
		cfg,
		presence,
		newTestLogger(t),
		protocol.NewEncoder(),
	)
	defer manager.Close()

	conn, connState := newTestConn(t, ctrl)
	connState.OnClose = func() { manager.RemoveConn(t.Context(), conn) }
	ch, peer := testTransport(t)
	called := false
	releases := 0
	cleanup := func() {
		releases++
		require.True(t, conn.Context().(*Session).Active())
		require.Len(t, connState.Writes, 1, "cleanup must follow ACK and activation")
	}
	require.NoError(
		t,
		manager.ActivateConn(
			t.Context(),
			42,
			ch,
			conn,
			cleanup,
			func(err error) {
				require.NoError(t, err)
				called = true
				require.NoError(
					t,
					manager.CompleteActivation(conn, SendData{
						MsgKind: pbpub.MsgKind_RESP_AUTHENTICATE,
						Msg:     &pbpub.AuthenticateResp{},
					}),
				)
			},
		),
	)
	require.False(t, conn.Context().(*Session).Active())
	require.Zero(t, releases)
	require.ErrorIs(t, manager.AsyncSendConn(t.Context(), conn, SendData{}), ErrSessionClosed)

	presence.add(nil, nil)
	require.False(t, called, "callback must run on owner loop")

	connState.Loop.Drain()
	require.True(t, called)
	require.True(t, conn.Context().(*Session).Active())
	require.Equal(t, 1, releases)
	require.Nil(t, conn.Context().(*Session).activationCleanup)
	require.Len(t, connState.Writes, 1)

	plain, err := peer.Decrypt(connState.Writes[0])
	require.NoError(t, err)

	ack, err := protocol.NewDecoder().DecodePayload(plain)
	require.NoError(t, err)
	require.Equal(t, pbpub.MsgKind_RESP_AUTHENTICATE, ack.Kind)
	require.NoError(t, conn.Close())

	connState.Loop.Drain()
	manager.RemoveConn(t.Context(), conn)
	require.Equal(t, []device.ID{42}, presence.dels)
	require.Zero(t, manager.CurConns())
	require.Equal(t, 1, releases, "closing an active connection must not repeat cleanup")
	require.Equal(t, 2*time.Second, manager.Tick())
}

func TestManagerIgnoresDelayedCallbackAfterCloseOrReplacement(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, replace := range []bool{false, true} {
		t.Run(
			map[bool]string{
				false: "closed",
				true:  "replaced",
			}[replace],
			func(t *testing.T) {
				cfg := Config{MaxConns: 3}
				cfg.Load(nil)
				presence := new(testPresence)
				manager := NewManager(
					cfg,
					presence,
					newTestLogger(t),
					protocol.NewEncoder(),
				)

				first, firstState := newTestConn(t, ctrl)
				firstState.OnClose = func() { manager.RemoveConn(t.Context(), first) }
				calls := 0
				releases := 0

				require.NoError(
					t,
					manager.ActivateConn(
						t.Context(),
						42,
						&testChannel{},
						first,
						func() { releases++ },
						func(error) { calls++ },
					),
				)

				late := presence.add
				if replace {
					next, _ := newTestConn(t, ctrl)
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
				} else {
					require.NoError(t, first.Close())
				}

				firstState.Loop.Drain()
				require.Equal(
					t,
					1,
					releases,
					"OnClose must release pending state even after replacement",
				)

				late(nil, nil)
				firstState.Loop.Drain()
				manager.RemoveConn(t.Context(), first)
				require.Equal(t, 1, releases)
				require.Zero(t, calls)
				require.Empty(t, firstState.Writes)

				if replace {
					require.Empty(t, presence.dels, "old close must not delete replacement presence")
				} else {
					require.Equal(t, []device.ID{42}, presence.dels)
				}
			})
	}
}

func TestManagerPresenceErrorsCloseAndReleaseSession(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, async := range []bool{false, true} {
		presence := new(testPresence)
		if !async {
			presence.addErr = errors.New("unavailable")
		}

		cfg := Config{}
		cfg.Load(nil)
		manager := NewManager(
			cfg,
			presence,
			newTestLogger(t),
			protocol.NewEncoder(),
		)

		conn, connState := newTestConn(t, ctrl)
		connState.OnClose = func() { manager.RemoveConn(t.Context(), conn) }
		ch := new(testChannel)
		releases := 0
		err := manager.ActivateConn(
			t.Context(),
			42,
			ch,
			conn, func() { releases++ },
			func(error) {},
		)

		if async {
			require.NoError(t, err)
			presence.add(nil, errors.New("unavailable"))
		} else {
			require.Error(t, err)
		}

		connState.Loop.Drain()
		require.True(t, connState.Closed.Load())
		require.True(t, ch.closed)
		require.Zero(t, manager.CurConns())
		require.Equal(t, 1, releases)
	}
}

func TestManagerReleasesActivationWhenRegistryRejectsConnection(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, isClosed := range []bool{false, true} {
		t.Run(
			map[bool]string{
				false: "capacity",
				true:  "closed manager",
			}[isClosed],
			func(t *testing.T) {
				cfg := Config{MaxConns: 1}
				cfg.Load(nil)

				manager := NewManager(
					cfg,
					new(testPresence),
					newTestLogger(t),
					protocol.NewEncoder(),
				)
				first, _ := newTestConn(t, ctrl)

				require.NoError(
					t,
					manager.ActivateConn(
						t.Context(),
						1,
						&testChannel{},
						first,
						nil,
						func(error) {}),
				)

				defer manager.RemoveConn(t.Context(), first)

				if isClosed {
					manager.Close()
				}
				conn, _ := newTestConn(t, ctrl)
				ch := new(testChannel)
				releases := 0
				err := manager.ActivateConn(
					t.Context(),
					2,
					ch, conn,
					func() { releases++ },
					func(error) {
						require.Fail(t, "rejected connection must not complete registration")
					},
				)
				if isClosed {
					require.ErrorIs(t, err, ErrManagerClosed)
				} else {
					require.ErrorIs(t, err, ErrConnReachMax)
				}
				require.Equal(t, 1, releases)
				require.True(t, ch.closed)
				require.Nil(t, conn.Context())
			},
		)
	}
}
