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
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	pbpub "xxx/api/hub/v1/proto/gen/pub"
	"xxx/internal/hub/core/protocol"
	protocolmocks "xxx/internal/hub/core/protocol/mocks"
	shareddevice "xxx/internal/shared/device"
	sharedsession "xxx/internal/shared/session"

	"xxx/pkg/nio"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

func setupSenderTest(t *testing.T, ctrl *gomock.Controller, id shareddevice.ID) (
	*connectionSender,
	*connectionRegistry,
	*protocolmocks.MockSecureChannel,
	*sharedsession.Keys,
	*Session,
	*testConn,
	*testConnState,
) {
	t.Helper()
	registry := newConnectionRegistry(4)
	secureChannel := protocolmocks.NewMockSecureChannel(ctrl)
	encoder := protocol.NewEncoder(secureChannel)
	sender := newConnectionSender(registry, encoder, newTestLogger(t))
	conn, state := newTestConn()
	keys := new(sharedsession.Keys)
	for i := range keys.SendMac {
		keys.SendMac[i] = byte(i + 1)
		keys.SendEnc[i] = byte(i + 65)
	}
	session := newSession(*keys, id, 0)
	_, err := registry.Replace(id, conn, session)
	require.NoError(t, err)
	return sender, registry, secureChannel, keys, session, conn, state
}

func TestConnectionSender_AsyncSendReportsMissingSession(t *testing.T) {
	ctrl := gomock.NewController(t)
	sender := newConnectionSender(
		newConnectionRegistry(1),
		protocol.NewEncoder(protocolmocks.NewMockSecureChannel(ctrl)),
		newTestLogger(t),
	)

	err := sender.AsyncSend(
		false,
		shareddevice.ID(7),
		SendData{
			MsgType: pbpub.MsgType_REQ_BROKER_FORWARD_MSG,
			Msg:     new(pbpub.BrokerForwardMsgReq),
		},
	)

	require.ErrorIs(t, err, ErrSessionNotFound)
}

func TestConnectionSender_AsyncSendConnRejectsMissingClosedAndStaleSessions(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry := newConnectionRegistry(3)
	sender := newConnectionSender(
		registry,
		protocol.NewEncoder(protocolmocks.NewMockSecureChannel(ctrl)),
		newTestLogger(t),
	)
	missing, _ := newTestConn()
	data := SendData{
		MsgType: pbpub.MsgType_REQ_BROKER_FORWARD_MSG,
		Msg:     new(pbpub.BrokerForwardMsgReq),
	}
	require.ErrorIs(t, sender.AsyncSendConn(false, missing, data), ErrSessionClosed)

	stale, _ := newTestConn()
	current, _ := newTestConn()
	id := shareddevice.ID(9)
	oldSession := newSession(sharedsession.Keys{}, id, 0)
	_, err := registry.Replace(id, stale, oldSession)
	require.NoError(t, err)
	_, err = registry.Replace(id, current, newSession(sharedsession.Keys{}, id, 0))
	require.NoError(t, err)
	require.ErrorIs(t, sender.AsyncSendConn(false, stale, data), ErrSessionClosed)

	_, removed := registry.Remove(current)
	require.True(t, removed)
	require.ErrorIs(t, sender.AsyncSendConn(false, current, data), ErrSessionClosed)
}

func TestConnectionSender_AsyncSendSerializesHMACSequences(t *testing.T) {
	ctrl := gomock.NewController(t)
	sender, _, secureChannel, keys, session, conn, _ := setupSenderTest(t, ctrl, shareddevice.ID(15))
	frames := make([][]byte, 0, 2)
	packetBuffers := make([][]byte, 0, 2)
	conn.asyncWriteFn = func(packet []byte, callback nio.AsyncCallback) error {
		frames = append(frames, bytes.Clone(packet))
		packetBuffers = append(packetBuffers, packet)
		return callback(conn, nil)
	}
	wantMsgs := []*pbpub.BrokerForwardMsgReq{
		{SendDeviceId: 10, RecvDeviceId: 11, MsgId: 12, ChannelId: 13},
		{SendDeviceId: 20, RecvDeviceId: 21, MsgId: 22, ChannelId: 23},
	}
	wantExpand := [][]byte{[]byte("first metadata"), []byte("second metadata")}

	for i := range wantMsgs {
		err := sender.AsyncSend(false, shareddevice.ID(15), SendData{
			MsgType: pbpub.MsgType_REQ_BROKER_FORWARD_MSG,
			Msg:     wantMsgs[i],
			Expand:  wantExpand[i],
		})
		require.NoError(t, err)
	}
	require.Len(t, frames, 2)

	decoder := protocol.NewDecoder(secureChannel)
	for i, frame := range frames {
		msg, expand, code, decodeErr := decoder.Decode(&keys.SendMac, uint64(i), frame)
		require.NoError(t, decodeErr)
		require.Equal(t, pbpub.Code_SUCCESS, code)
		require.True(t, proto.Equal(wantMsgs[i], msg))
		require.Equal(t, wantExpand[i], expand)
		if i == 1 {
			_, _, _, replayErr := decoder.Decode(&keys.SendMac, 0, frame)
			require.ErrorIs(t, replayErr, protocol.ErrPkgModified)
		}
	}
	var nextSeq uint64
	require.NoError(t, session.WithSendSeq(func(seq uint64) (bool, error) { nextSeq = seq; return false, nil }))
	require.EqualValues(t, 2, nextSeq)
	for _, buf := range packetBuffers {
		require.True(t, bytes.Equal(make([]byte, cap(buf)), buf[:cap(buf)]), "write completion clears the pooled frame buffer")
	}
}

func TestConnectionSender_AsyncSendSecureBindsSequenceIntoAAD(t *testing.T) {
	ctrl := gomock.NewController(t)
	sender, _, secureChannel, keys, _, conn, _ := setupSenderTest(t, ctrl, shareddevice.ID(16))
	var aads [][]byte
	secureChannel.EXPECT().Size(gomock.Any()).Times(2).DoAndReturn(func(n int) int { return n + 16 })
	secureChannel.EXPECT().Seal(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(2).DoAndReturn(
		func(_ *[32]byte, _ *[12]byte, plain, aad []byte) ([]byte, error) {
			aads = append(aads, bytes.Clone(aad))
			return make([]byte, len(plain)+16), nil
		},
	)
	conn.asyncWriteFn = func(_ []byte, callback nio.AsyncCallback) error { return callback(conn, nil) }
	data := SendData{MsgType: pbpub.MsgType_REQ_SEND_MSG, Msg: new(pbpub.SendMsgReq), Expand: []byte("aad-tail")}

	require.NoError(t, sender.AsyncSend(true, shareddevice.ID(16), data))
	require.NoError(t, sender.AsyncSend(true, shareddevice.ID(16), data))

	require.Len(t, aads, 2)
	const aadDomain = "xxx/hub/packet/gcm/v1\x00"
	for i, aad := range aads {
		require.Equal(t, aadDomain, string(aad[:len(aadDomain)]))
		seqOffset := len(aadDomain) + 6
		require.EqualValues(t, i, binary.LittleEndian.Uint64(aad[seqOffset:seqOffset+8]))
		require.Equal(t, data.Expand, aad[seqOffset+8:])
	}
	require.Equal(t, keys.SendEnc, conn.Context().(*Session).keys.SendEnc)
}

func TestConnectionSender_MarksSessionFailedOnAsyncWriteErrors(t *testing.T) {
	t.Run("enqueue error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		sender, _, _, _, session, conn, state := setupSenderTest(t, ctrl, shareddevice.ID(17))
		writeErr := errors.New("event loop stopped")
		conn.asyncWriteFn = func([]byte, nio.AsyncCallback) error { return writeErr }

		err := sender.AsyncSend(
			false,
			shareddevice.ID(17),
			SendData{
				MsgType: pbpub.MsgType_REQ_BROKER_FORWARD_MSG,
				Msg:     new(pbpub.BrokerForwardMsgReq),
			},
		)

		require.ErrorIs(t, err, writeErr)
		require.True(t, session.SendFailed())
		require.EqualValues(t, 1, state.closeCount.Load())
	})

	t.Run("completion error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		sender, _, _, _, session, conn, state := setupSenderTest(t, ctrl, shareddevice.ID(18))
		writeErr := errors.New("socket write failed")
		var completion nio.AsyncCallback
		conn.asyncWriteFn = func(_ []byte, callback nio.AsyncCallback) error {
			completion = callback
			return nil
		}

		err := sender.AsyncSend(false, shareddevice.ID(18), SendData{MsgType: pbpub.MsgType_REQ_BROKER_FORWARD_MSG, Msg: new(pbpub.BrokerForwardMsgReq)})
		require.NoError(t, err)
		require.False(t, session.SendFailed())
		require.NoError(t, completion(conn, writeErr))
		require.True(t, session.SendFailed())
		require.EqualValues(t, 1, state.closeCount.Load())
	})
}

func TestConnectionSender_DoesNotConsumeSequenceWhenEncodingFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	sender, _, _, _, session, _, _ := setupSenderTest(t, ctrl, shareddevice.ID(19))
	data := SendData{
		MsgType: pbpub.MsgType_REQ_BROKER_FORWARD_MSG,
		Msg:     new(pbpub.BrokerForwardMsgReq),
		Expand:  make([]byte, 1<<16),
	}
	err := sender.AsyncSend(false, shareddevice.ID(19), data)
	require.ErrorIs(t, err, protocol.ErrPkgTooLarge)
	var nextSeq uint64
	require.NoError(t, session.WithSendSeq(func(seq uint64) (bool, error) { nextSeq = seq; return false, nil }))
	require.Zero(t, nextSeq)
	require.False(t, session.SendFailed())
}
