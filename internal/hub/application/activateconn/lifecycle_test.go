/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package activateconn

import (
	"context"
	"errors"
	"testing"
	"time"

	"xxx/internal/hub/core/connection"
	"xxx/internal/hub/core/protocol"
	"xxx/internal/shared/device"
	"xxx/internal/shared/ticket/tickettest"

	"xxx/pkg/nio"
	niotest "xxx/pkg/nio/testutil"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type heldPresence struct {
	complete func(any, error)
	deletes  int
}

func (p *heldPresence) Add(_ context.Context, _ device.ID, _ int64, complete func(any, error)) error {
	p.complete = complete
	return nil
}

func (p *heldPresence) Del(_ context.Context, _ device.ID, _ int64, complete func(error)) error {
	p.deletes++
	complete(nil)
	return nil
}

// Use the real Manager: registration replaces conn.Context(), so OnClose can
// only reach pending state through the Session's cleanup callback.
func TestSessionOwnsPendingActivationCleanup(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, kind := range []string{
		"active",
		"closed",
		"timeout",
		"presence error",
		"enqueue error",
		"ACK error",
	} {
		t.Run(
			kind,
			func(t *testing.T) {
				presence := new(heldPresence)
				envi := setup(
					t,
					ctrl,
					Config{
						HandshakeTimeout: time.Hour,
						PreAuthTimeout:   time.Hour,
					},
				)

				cfg := connection.Config{MaxConns: 2}
				cfg.Load(nil)

				manager := connection.NewManager(
					cfg,
					presence,
					envi.ca.logger,
					protocol.NewEncoder(),
				)
				t.Cleanup(manager.Close)
				envi.ca.manager = manager

				conn := envi.hello(t, ctrl)
				pa := conn.Context().(*pendingActivation)
				// Retain the backing storage before activation/close clears the fields.
				challenge := pa.challenge[:cap(pa.challenge)]
				binding := pa.binding[:cap(pa.binding)]
				require.NotEmpty(t, challenge)
				require.NotEmpty(t, binding)
				closeRequested := false

				conn.
					EXPECT().
					Close().
					DoAndReturn(func() error {
						closeRequested = true
						return nil
					}).
					AnyTimes()

				loop := conn.EventLoop().(*niotest.Loop)
				t.Cleanup(func() {
					envi.ca.Deactivate(t.Context(), conn)
					loop.Drain()
				})

				frame, req := envi.auth(t)
				claims := tickettest.New()
				envi.expectAuthorization(conn, req).Return(claims, nil)
				envi.service.
					EXPECT().
					CheckExpiry(claims, gomock.Any()).
					Return(nil)
				require.Equal(
					t,
					nio.None,
					envi.ca.HandleFrame(t.Context(), conn, frame),
				)

				envi.finishAuthorization(conn)
				session, ok := conn.Context().(*connection.Session)
				require.True(t, ok, "registration must replace the pending context")
				require.False(t, session.Active())
				require.EqualValues(t, 1, envi.ca.PreAuthConns())
				require.False(t, pa.released.Load())

				switch kind {
				case "active", "ACK error":
					envi.service.
						EXPECT().
						CheckExpiry(claims, gomock.Any()).
						Return(nil)

					envi.channel.
						EXPECT().
						EncryptRecord(gomock.Any()).
						Return([]byte("ACK"), nil)

					conn.
						EXPECT().
						Write([]byte("ACK")).
						DoAndReturn(func(data []byte) (int, error) {
							require.False(
								t,
								pa.released.Load(),
								"pending state must survive until ACK is written",
							)
							if kind == "ACK error" {
								return 0, errors.New("write failed")
							}
							return len(data), nil
						})

					presence.complete(nil, nil)
					loop.Drain()

					if kind == "active" {
						require.True(t, session.Active())
						require.False(t, closeRequested)
						require.Zero(t, envi.ca.PreAuthConns())
					}

				case "timeout":
					pa.timer.Reset(0)
					require.Eventually(
						t,
						func() bool {
							loop.Drain()
							return closeRequested
						},
						time.Second,
						time.Millisecond,
					)

				case "presence error":
					presence.complete(nil, errors.New("distributed cache unavailable"))
					loop.Drain()
					require.True(t, closeRequested)

				case "enqueue error":
					loop.Err = errors.New("event loop unavailable")
					presence.complete(nil, nil)
					require.True(t, closeRequested)
				}

				// Simulate OnClose on the owner loop, including after Execute rejected
				// the Redis callback. A repeated close/clear must not release twice.
				require.Equal(t, nio.None, envi.ca.Deactivate(t.Context(), conn))

				envi.ca.Deactivate(t.Context(), conn)
				pa.clear()

				require.True(t, pa.released.Load())
				require.Zero(t, envi.ca.PreAuthConns())
				require.Zero(t, manager.CurConns())
				require.Nil(t, conn.Context())
				require.Nil(t, pa.channel)
				require.Nil(t, pa.challenge)
				require.Nil(t, pa.binding)
				require.Equal(t, make([]byte, len(challenge)), challenge)
				require.Equal(t, make([]byte, len(binding)), binding)

				if kind == "closed" || kind == "timeout" {
					// The in-flight Redis result must neither ACK nor reactivate a
					// closed connection, and must allow the queued delete to run.
					presence.complete(nil, nil)
				}
				loop.Drain()

				require.Equal(t, 1, presence.deletes)
				require.Zero(t, envi.ca.PreAuthConns())
			},
		)
	}
}
