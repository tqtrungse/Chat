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
	"context"
	"errors"
	"testing"
	"time"

	pbpub "xxx/api/hub/v1/proto/gen/pub"
	connmocks "xxx/internal/hub/core/connection/mocks"
	"xxx/internal/hub/core/protocol"
	protocolmocks "xxx/internal/hub/core/protocol/mocks"
	shareddevice "xxx/internal/shared/device"
	sharedsession "xxx/internal/shared/session"

	"xxx/pkg/nio"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

func testKeys() sharedsession.Keys {
	var k sharedsession.Keys
	for i := range k.RecvEnc {
		k.RecvEnc[i] = byte(i + 1)
		k.SendEnc[i] = byte(i + 2)
		k.RecvMac[i] = byte(i + 3)
		k.SendMac[i] = byte(i + 4)
	}
	return k
}

func newManagerForTest(t *testing.T, cfg Config) (*Manager, *connmocks.MockPresencePublisher) {
	t.Helper()
	ctrl := gomock.NewController(t)
	presence := connmocks.NewMockPresencePublisher(ctrl)
	encoder := protocol.NewEncoder(protocolmocks.NewMockSecureChannel(ctrl))
	return NewManager(cfg, presence, newTestLogger(t), encoder), presence
}

func TestManager_ForceCloseClosesConnection(t *testing.T) {
	conn, state := newTestConn()
	m, _ := newManagerForTest(
		t,
		Config{
			MaxConns:       2,
			TickerDuration: 5 * time.Second,
		},
	)
	m.ForceClose(conn)
	require.EqualValues(t, 1, state.closeCount.Load())
}

func TestManager_ActivateConnRegistersAndPublishesPresence(t *testing.T) {
	m, p := newManagerForTest(
		t,
		Config{
			MaxConns:       2,
			TickerDuration: 5 * time.Second,
		},
	)
	conn, _ := newTestConn()
	const id = shareddevice.ID(42)
	keys := testKeys()

	before := time.Now().Unix()
	var gotTS int64
	p.EXPECT().
		Add(
			gomock.Any(),
			id,
			gomock.Any(),
			gomock.Any(),
		).
		DoAndReturn(func(_ context.Context, _ shareddevice.ID, ts int64, onResult func(any, error)) error {
			gotTS = ts
			onResult(nil, nil)
			return nil
		}).
		Times(1)

	var activated []shareddevice.ID
	err := m.ActivateConn(
		context.Background(),
		id,
		&keys,
		conn,
		func(got shareddevice.ID) {
			activated = append(activated, got)
		},
	)
	require.NoError(t, err)
	require.EqualValues(t, uint32(1), m.registry.CurConns())

	active, exists := m.registry.Lookup(id)
	require.True(t, exists)
	require.Same(t, conn, active)
	require.Equal(t, keys, conn.Context().(*Session).keys)
	require.Equal(t, 5*time.Second, m.Tick())
	require.Equal(t, []shareddevice.ID{id}, activated)
	require.GreaterOrEqual(t, gotTS, before)
	require.LessOrEqual(t, gotTS, time.Now().Unix())
}

func TestManager_ActivateConnClosesConnectionWhenPresenceAddFails(t *testing.T) {
	t.Run("async result error", func(t *testing.T) {
		m, p := newManagerForTest(
			t,
			Config{
				MaxConns:       1,
				TickerDuration: time.Second,
			},
		)
		conn, state := newTestConn()
		keys := testKeys()
		addErr := errors.New("cache unavailable")

		p.EXPECT().
			Add(
				gomock.Any(),
				shareddevice.ID(5),
				gomock.Any(),
				gomock.Any(),
			).
			DoAndReturn(func(_ context.Context, _ shareddevice.ID, _ int64, onResult func(any, error)) error {
				onResult(nil, addErr)
				return nil
			}).
			Times(1)

		callbackCalled := false
		require.NoError(
			t,
			m.ActivateConn(
				context.Background(),
				5,
				&keys,
				conn,
				func(shareddevice.ID) { callbackCalled = true },
			),
		)
		require.EqualValues(t, 1, state.closeCount.Load())
		require.False(t, callbackCalled)
	})

	t.Run("submit error", func(t *testing.T) {
		m, p := newManagerForTest(
			t,
			Config{
				MaxConns:       1,
				TickerDuration: time.Second,
			},
		)
		conn, state := newTestConn()
		keys := testKeys()

		p.EXPECT().
			Add(
				gomock.Any(),
				shareddevice.ID(6),
				gomock.Any(),
				gomock.Any(),
			).
			Return(errors.New("pool closed")).
			Times(1)

		callbackCalled := false
		require.Error(
			t,
			m.ActivateConn(
				context.Background(),
				6,
				&keys,
				conn,
				func(shareddevice.ID) { callbackCalled = true },
			),
		)
		require.EqualValues(t, int32(1), state.closeCount.Load())
		require.False(t, callbackCalled)
	})
}

func TestManager_ActivateConnReplacesAndClosesOldConnection(t *testing.T) {
	m, p := newManagerForTest(
		t,
		Config{MaxConns: 3},
	)
	oldConn, oldState := newTestConn()
	newConn, _ := newTestConn()
	const id = shareddevice.ID(77)
	keys := testKeys()

	noop := func(shareddevice.ID) {}
	p.EXPECT().
		Add(
			gomock.Any(),
			id,
			gomock.Any(),
			gomock.Any(),
		).
		DoAndReturn(func(_ context.Context, _ shareddevice.ID, _ int64, onResult func(any, error)) error {
			onResult(nil, nil)
			return nil
		}).
		Times(2)

	require.NoError(t, m.ActivateConn(context.Background(), id, &keys, oldConn, noop))
	require.NoError(t, m.ActivateConn(context.Background(), id, &keys, newConn, noop))
	require.EqualValues(t, 1, oldState.closeCount.Load())
	require.True(t, oldConn.Context().(*Session).isClosed())
	require.Same(t, newConn, mustLookupConnection(t, m.registry, id))
	require.Equal(t, defaultTickerDuration, m.Tick())
}

func TestManager_RemoveConnOnlyPublishesForRegisteredConnection(t *testing.T) {
	m, p := newManagerForTest(
		t,
		Config{
			MaxConns:       2,
			TickerDuration: time.Second,
		},
	)
	conn, _ := newTestConn()
	other, _ := newTestConn()
	const id = shareddevice.ID(61)
	_, err := m.registry.Replace(
		id,
		conn,
		newSession(
			sharedsession.Keys{},
			id,
			time.Now().Unix(),
		),
	)
	require.NoError(t, err)

	// Delete correctly once for all 3 RemoveConn calls: `other` is not registered,
	// The second call on `conn` is idempotent.
	p.EXPECT().
		Del(
			gomock.Any(),
			id,
			gomock.Any(),
			gomock.Any(),
		).
		DoAndReturn(func(_ context.Context, _ shareddevice.ID, _ int64, onResult func(error)) error {
			onResult(nil)
			return nil
		}).
		Times(1)

	m.RemoveConn(context.Background(), other)
	require.EqualValues(t, 1, m.registry.CurConns())

	m.RemoveConn(context.Background(), conn)
	require.EqualValues(t, 0, m.registry.CurConns())
}

func TestManager_DelegatesActivationLifecycleAndCapacity(t *testing.T) {
	m, presence := newManagerForTest(
		t,
		Config{
			MaxConns:       2,
			TickerDuration: 250 * time.Millisecond,
		},
	)
	const id = shareddevice.ID(0x91)
	keys := testKeys()

	require.EqualValues(t, 2, m.MaxConns())
	require.Zero(t, m.CurConns())
	require.Equal(t, 250*time.Millisecond, m.Tick())

	presence.EXPECT().
		Add(
			gomock.Any(),
			id,
			gomock.Any(),
			gomock.Any(),
		).
		DoAndReturn(func(_ context.Context, _ shareddevice.ID, _ int64, onResult func(any, error)) error {
			onResult(nil, nil)
			return nil
		}).
		Times(1)

	presence.EXPECT().
		Del(
			gomock.Any(),
			id,
			gomock.Any(),
			gomock.Any(),
		).
		DoAndReturn(func(_ context.Context, _ shareddevice.ID, _ int64, onResult func(error)) error {
			onResult(nil)
			return nil
		}).
		Times(1)

	conn, _ := newTestConn()
	var activated []shareddevice.ID
	require.NoError(
		t,
		m.ActivateConn(
			context.Background(),
			id,
			&keys,
			conn,
			func(got shareddevice.ID) { activated = append(activated, got) },
		),
	)
	require.EqualValues(t, 1, m.CurConns())
	require.Equal(t, []shareddevice.ID{id}, activated)

	m.RemoveConn(context.Background(), conn)
	require.Zero(t, m.CurConns())

	toClose, closeState := newTestConn()
	m.ForceClose(toClose)
	require.EqualValues(t, 1, closeState.closeCount.Load())

	// Sau Close, ActivateConn bị từ chối trước khi chạm presence (không có EXPECT Add nào nữa).
	m.Close()
	require.ErrorIs(
		t,
		m.ActivateConn(
			context.Background(),
			id,
			&keys,
			toClose,
			func(shareddevice.ID) {},
		),
		ErrManagerClosed,
	)
	require.Zero(t, m.CurConns())
}

func TestManager_AsyncSendAndAsyncSendConnSerializeFrames(t *testing.T) {
	m, _ := newManagerForTest(
		t,
		Config{MaxConns: 1}, // No EXPECT presence: the call will fail
	)
	conn, _ := newTestConn()
	const id = shareddevice.ID(0xA1)
	keys := testKeys()

	_, err := m.registry.Replace(
		id,
		conn,
		newSession(keys, id, time.Now().Unix()),
	)
	require.NoError(t, err)

	var frames [][]byte
	conn.asyncWriteFn = func(packet []byte, callback nio.AsyncCallback) error {
		frames = append(frames, bytes.Clone(packet))
		return callback(conn, nil)
	}

	first := &pbpub.BrokerForwardMsgReq{
		SendDeviceId: 1,
		RecvDeviceId: 2,
		MsgId:        3,
		ChannelId:    4,
	}
	second := &pbpub.BrokerForwardMsgReq{
		SendDeviceId: 5,
		RecvDeviceId: 6,
		MsgId:        7,
		ChannelId:    8,
	}
	require.NoError(
		t,
		m.AsyncSend(
			false,
			id,
			SendData{
				MsgType: pbpub.MsgType_REQ_BROKER_FORWARD_MSG,
				Msg:     first,
			},
		),
	)
	require.NoError(
		t,
		m.AsyncSendConn(
			false,
			conn,
			SendData{
				MsgType: pbpub.MsgType_REQ_BROKER_FORWARD_MSG,
				Msg:     second,
			},
		),
	)
	require.Len(t, frames, 2)

	// Decode only uses HMAC, so SecureChannel is not called.
	decoder := protocol.NewDecoder(protocolmocks.NewMockSecureChannel(gomock.NewController(t)))
	for seq, want := range []*pbpub.BrokerForwardMsgReq{first, second} {
		msg, _, code, err := decoder.Decode(&keys.SendMac, uint64(seq), frames[seq])
		require.NoError(t, err)
		require.Equal(t, pbpub.Code_SUCCESS, code)
		require.True(t, proto.Equal(want, msg))
	}
}

func mustLookupConnection(t *testing.T, registry *connectionRegistry, id shareddevice.ID) nio.Conn {
	t.Helper()
	conn, ok := registry.Lookup(id)
	require.True(t, ok)
	return conn
}
