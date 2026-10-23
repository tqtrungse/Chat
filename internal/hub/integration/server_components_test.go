/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package integration_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	pbpub "xxx/api/hub/v2/proto/gen/pub"
	"xxx/internal/hub/adapter/tcp"
	"xxx/internal/hub/core/connection"
	"xxx/internal/hub/core/protocol"
	"xxx/internal/hub/infrastructure/noise/noisetest"
	"xxx/internal/shared/ticket/tickettest"

	"xxx/pkg/nio"
	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestIntegrationServerPartialHelloAtEveryByteBoundary(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for split := 1; split < 55; split++ {
		t.Run(
			fmt.Sprint(split),
			func(t *testing.T) {
				server, _, _, _ := serverFixture(t, tcp.Config{}, ctrl)
				conn, state := newIntegrationConn(t, ctrl)
				t.Cleanup(func() {
					_ = conn.Close()
					state.Loop.Drain()
				})
				state.OnClose = func() { server.OnClose(conn, nil) }

				_, action := server.OnOpen(conn)
				require.Equal(t, nio.None, action)

				peer, err := noisetest.New(
					tickettest.New().HubPublicKey[:],
					"hub.chat",
					7,
				)
				require.NoError(t, err)

				frame, err := peer.Hello(7)
				require.NoError(t, err)

				state.Input = bytes.Clone(frame[:split])
				require.Equal(t, nio.None, server.OnTraffic(conn))
				require.Empty(t, state.Writes)
				require.Len(t, state.Input, split)

				state.Input = append(state.Input, frame[split:]...)
				require.Equal(t, nio.None, server.OnTraffic(conn))
				require.Empty(t, state.Input)
				require.Len(t, state.Writes, 1)
				require.NoError(t, peer.Finish(state.Writes[0]))
			},
		)
	}
}

func TestIntegrationServerDrainsFramesWithFairnessWakeAndNoNonceReset(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	server, manager, verifier, spy := serverFixture(t, tcp.Config{MaxFramesPerTick: 2}, ctrl)
	conn, peer, state := fakeHandshake(t, ctrl, server)
	authF := authFrame(t, peer, verifier)
	// Feed authentication one byte at a time, including its length prefix.
	for _, buf := range authF {
		state.Input = append(state.Input, buf)
		require.Equal(t, nio.None, server.OnTraffic(conn))
	}

	require.Eventually(
		t,
		func() bool {
			state.Loop.Drain()
			session, ok := conn.Context().(*connection.Session)
			return ok && session.Active()
		},
		time.Second,
		time.Millisecond,
	)

	plain, err := peer.Decrypt(state.Writes[1])
	require.NoError(t, err)

	ack, err := protocol.NewDecoder().DecodePayload(plain)
	require.NoError(t, err)
	require.Equal(t, pbpub.MsgKind_RESP_AUTHENTICATE, ack.Kind)

	for i := 1; i <= 5; i++ {
		plain, err = protocol.NewEncoder().EncodePayload(
			pbpub.MsgKind_REQ_SEND_MSG,
			&pbpub.SendMsgReq{
				ChannelId:     1,
				DedupKey:      uint64(i),
				RecvDeviceIds: []uint64{2},
			},
			[]byte("E2EE"),
		)
		require.NoError(t, err)

		frame, encryptErr := peer.Encrypt(plain)
		clear(plain[:cap(plain)])
		slicepool.Put(plain)
		require.NoError(t, encryptErr)

		state.Input = append(state.Input, frame...)
	}

	require.Equal(t, nio.None, server.OnTraffic(conn))
	require.Len(t, spy.got, 2)
	require.EqualValues(t, 1, state.WakeCount.Load())

	state.Loop.Drain()
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, spy.got)
	require.Empty(t, state.Input)

	for _, buf := range spy.expand {
		require.Equal(t, "E2EE", string(buf))
	}

	require.NoError(
		t,
		manager.AsyncSend(
			t.Context(),
			42,
			connection.SendData{
				MsgKind: pbpub.MsgKind_RESP_SEND_MSG,
				Msg:     &pbpub.SendMsgResp{MsgId: 9},
			},
		),
	)

	state.Loop.Drain()
	_, err = peer.Decrypt(state.Writes[2])
	require.NoError(t, err)
}

func TestIntegrationServerRejectsOversizedLegacyBusinessBeforeAuthAndReplay(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, kind := range []string{
		"oversized",
		"zero length",
		"legacy",
		"business before auth",
		"replay",
	} {
		t.Run(
			kind,
			func(t *testing.T) {
				server, _, verifier, _ := serverFixture(t, tcp.Config{}, ctrl)
				conn, peer, state := fakeHandshake(t, ctrl, server)

				switch kind {
				case "oversized":
					state.Input = []byte{0, 0}
					binary.LittleEndian.PutUint16(state.Input, 4097)

				case "zero length":
					state.Input = []byte{0, 0}

				case "legacy":
					state.Input = []byte{2, 0, 1, 0}

				case "business before auth":
					plain, err := protocol.NewEncoder().EncodePayload(
						pbpub.MsgKind_REQ_SEND_MSG,
						&pbpub.SendMsgReq{
							ChannelId:     1,
							DedupKey:      1,
							RecvDeviceIds: []uint64{2},
						},
						nil,
					)
					require.NoError(t, err)

					state.Input, err = peer.Encrypt(plain)
					clear(plain[:cap(plain)])
					slicepool.Put(plain)
					require.NoError(t, err)

				case "replay":
					authF := authFrame(t, peer, verifier)
					state.Input = bytes.Clone(authF)
					require.Equal(t, nio.None, server.OnTraffic(conn))
					require.Eventually(
						t,
						func() bool {
							state.Loop.Drain()
							session, ok := conn.Context().(*connection.Session)
							return ok && session.Active()
						},
						time.Second,
						time.Millisecond,
					)
					state.Input = authF
				}

				require.Equal(t, nio.Close, server.OnTraffic(conn))
				require.NoError(t, conn.Close())

				state.Loop.Drain()
			},
		)
	}
}
