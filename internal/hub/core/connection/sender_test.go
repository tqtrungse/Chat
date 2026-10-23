/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connection

import (
	"errors"
	"sync"
	"testing"

	pbpub "xxx/api/hub/v2/proto/gen/pub"
	"xxx/internal/hub/core/protocol"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestSenderConcurrentWorkersPreserveNoiseOrderAndOwnPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	var (
		registry        = newConnectionRegistry(2)
		conn, connState = newTestConn(t, ctrl)
		ch, peer        = testTransport(t)
		session         = newSession(ch, 42, 0)
		sender          = newConnectionSender(registry, protocol.NewEncoder(), newTestLogger(t))
	)

	session.state.Store(uint32(sessionStateActive))
	_, err := registry.Replace(42, conn, session)
	require.NoError(t, err)

	const n = 64
	var wg sync.WaitGroup
	errs := make(chan error, n)

	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := &pbpub.SendMsgResp{ChannelId: uint64(i + 1)}
			expand := []byte("owned")
			errs <- sender.AsyncSend(
				t.Context(),
				42,
				SendData{
					MsgKind: pbpub.MsgKind_RESP_SEND_MSG,
					Msg:     msg,
					Expand:  expand,
				},
			)
			msg.ChannelId = 999
			clear(expand[:cap(expand)])
		}()
	}
	wg.Wait()
	close(errs)

	for subErr := range errs {
		require.NoError(t, subErr)
	}
	require.Empty(t, connState.Writes)

	connState.Loop.Drain()
	require.Len(t, connState.Writes, n)

	seen := map[uint64]bool{}
	for _, frame := range connState.Writes {
		plain, err := peer.Decrypt(frame)
		require.NoError(t, err)

		payload, err := protocol.NewDecoder().DecodePayload(plain)
		require.NoError(t, err)
		require.Equal(t, "owned", string(payload.Expand))

		id := payload.Message.(*pbpub.SendMsgResp).ChannelId
		require.LessOrEqual(t, id, uint64(n))

		seen[id] = true
	}
	require.Len(t, seen, n)
}

func TestSenderSkipsStaleTasksAndClosesAfterWriteFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	var (
		registry        = newConnectionRegistry(3)
		sender          = newConnectionSender(registry, protocol.NewEncoder(), newTestLogger(t))
		conn, connState = newTestConn(t, ctrl)
		session         = newSession(&testChannel{}, 42, 0)
	)

	for _, kind := range []string{
		"stale",
		"write error",
		"enqueue error",
	} {
		t.Run(
			kind,
			func(t *testing.T) {
				session.state.Store(uint32(sessionStateActive))
				_, err := registry.Replace(42, conn, session)
				require.NoError(t, err)

				data := SendData{
					MsgKind: pbpub.MsgKind_RESP_SEND_MSG,
					Msg:     &pbpub.SendMsgResp{},
				}
				if kind == "enqueue error" {
					connState.Loop.Err = errors.New("stopped")
					require.Error(t, sender.AsyncSendConn(t.Context(), conn, data))
					return
				}

				require.NoError(t, sender.AsyncSendConn(t.Context(), conn, data))
				require.NoError(t, sender.AsyncSendConn(t.Context(), conn, data))

				if kind == "stale" {
					next, _ := newTestConn(t, ctrl)
					_, err = registry.Replace(
						42,
						next,
						newSession(&testChannel{}, 42, 0),
					)
					require.NoError(t, err)
				} else {
					connState.WriteErr = errors.New("broken pipe")
				}

				connState.Loop.Drain()
				require.Empty(t, connState.Writes)
				if kind == "write error" {
					require.EqualValues(t, 1, connState.CloseCount.Load())
					require.False(t, session.Active())
				}
			},
		)
	}
}
