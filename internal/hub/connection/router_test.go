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
	"encoding/binary"
	"testing"
	"time"

	pbpub "xxx/api/hub/v1/proto/gen/pub"
	"xxx/internal/hub/domain/message"
	"xxx/internal/hub/protocol"
	protocolmocks "xxx/internal/hub/protocol/mocks"
	shareddevice "xxx/internal/shared/device"
	sharedticket "xxx/internal/shared/ticket"

	"xxx/pkg/nio"
	workerpool "xxx/pkg/pool/worker"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

func newRouterForTest(
	t *testing.T,
	ctx context.Context,
	config Config,
	verifier TicketVerifier,
	signer Signer,
	cache DistributedCache,
	cb CircuitBreaker,
) (*Router, *protocolmocks.MockSecureChannel) {
	t.Helper()
	ctrl := gomock.NewController(t)
	secureChannel := protocolmocks.NewMockSecureChannel(ctrl)
	router := NewRouter(
		ctx,
		config,
		signer,
		verifier,
		cache,
		cb,
		newTestLogger(t),
		mustTestPool(t),
		protocol.NewEncoder(secureChannel),
	)
	t.Cleanup(func() { router.lifecycle.presence.batcher.WaitDone() })
	return router, secureChannel
}

func mustTestPool(t *testing.T) *workerpool.Pool {
	t.Helper()
	pool, _ := newTestPool(t)
	return pool
}

func TestRouter_FacadeDelegatesActivationLifecycleAndCapacity(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	cacheAdds := make(chan []shareddevice.ID, 1)
	cacheDels := make(chan []shareddevice.ID, 1)
	cache := &testDistributedCache{
		add: func(_ context.Context, ids []shareddevice.ID) (map[shareddevice.ID][]message.Date, error) {
			cacheAdds <- append([]shareddevice.ID(nil), ids...)
			return nil, nil
		},
		del: func(_ context.Context, ids []shareddevice.ID) error {
			cacheDels <- append([]shareddevice.ID(nil), ids...)
			return nil
		},
	}
	const deviceID = uint64(0x91)
	ticket := testTicket(deviceID)
	verified := false
	verifier := testTicketVerifier{
		open: func(id uint64, _ []byte) (*sharedticket.Ticket, error) {
			require.Equal(t, deviceID, id)
			return ticket, nil
		},
		checkExpiry: func(got *sharedticket.Ticket, _ time.Time) error {
			require.Same(t, ticket, got)
			return nil
		},
	}
	signer := testSigner{verify: func(_, _, _ []byte) error { verified = true; return nil }}
	router, _ := newRouterForTest(t, rootCtx, Config{
		Router:       RouterConfig{MaxConns: 2, TickerDuration: 250 * time.Millisecond},
		PresenceSync: PresenceSyncConfig{BatchSize: 1, Linger: time.Hour},
	}, verifier, signer, cache, testCircuitBreaker{})
	t.Cleanup(func() { cancel() })

	require.EqualValues(t, 2, router.MaxConns())
	require.Zero(t, router.CurConns())
	require.Equal(t, 250*time.Millisecond, router.Tick())
	token := make([]byte, tokenSize)
	binary.LittleEndian.PutUint64(token, deviceID)
	prepared, err := router.PrepareActivation(token, []byte("ticket"), []byte("signature"))
	require.NoError(t, err)
	require.Same(t, ticket, prepared)
	require.True(t, verified)

	conn, _ := newTestConn()
	keys := ticket.Keys
	activated := make(chan shareddevice.ID, 1)
	require.NoError(t, router.ActivateConn(context.Background(), ticket, &keys, conn, func(id shareddevice.ID) { activated <- id }))
	require.EqualValues(t, 1, router.CurConns())
	require.ElementsMatch(t, []shareddevice.ID{shareddevice.ID(deviceID)}, awaitTestResult(t, cacheAdds))
	require.Equal(t, shareddevice.ID(deviceID), awaitTestResult(t, activated))

	router.RemoveConn(context.Background(), conn)
	require.ElementsMatch(t, []shareddevice.ID{shareddevice.ID(deviceID)}, awaitTestResult(t, cacheDels))
	require.Zero(t, router.CurConns())

	toClose, closeState := newTestConn()
	router.ForceClose(toClose)
	require.EqualValues(t, 1, closeState.closeCount.Load())

	router.Close()
	require.ErrorIs(t, router.ActivateConn(context.Background(), ticket, &keys, toClose, func(shareddevice.ID) {}), ErrRouterClosed)
	require.Zero(t, router.CurConns())
}

func TestRouter_AsyncSendAndAsyncSendConnSerializeFrames(t *testing.T) {
	rootCtx, cancel := context.WithCancel(context.Background())
	router, secureChannel := newRouterForTest(t, rootCtx, Config{
		Router:       RouterConfig{MaxConns: 1},
		PresenceSync: PresenceSyncConfig{BatchSize: 1, Linger: time.Hour},
	}, testTicketVerifier{}, testSigner{}, &testDistributedCache{}, testCircuitBreaker{})
	t.Cleanup(cancel)
	conn, _ := newTestConn()
	id := shareddevice.ID(0xA1)
	keys := testTicket(uint64(id)).Keys
	_, err := router.registry.Replace(id, conn, newSession(keys, id, time.Now().Unix()))
	require.NoError(t, err)
	var frames [][]byte
	conn.asyncWriteFn = func(packet []byte, callback nio.AsyncCallback) error {
		frames = append(frames, bytes.Clone(packet))
		return callback(conn, nil)
	}
	first := &pbpub.BrokerForwardMsgReq{SendDeviceId: 1, RecvDeviceId: 2, MsgId: 3, ChannelId: 4}
	second := &pbpub.BrokerForwardMsgReq{SendDeviceId: 5, RecvDeviceId: 6, MsgId: 7, ChannelId: 8}
	require.NoError(t, router.AsyncSend(false, id, SendData{MsgType: pbpub.MsgType_REQ_BROKER_FORWARD_MSG, Msg: first}))
	require.NoError(t, router.AsyncSendConn(false, conn, SendData{MsgType: pbpub.MsgType_REQ_BROKER_FORWARD_MSG, Msg: second}))
	require.Len(t, frames, 2)

	decoder := protocol.NewDecoder(secureChannel)
	for seq, want := range []*pbpub.BrokerForwardMsgReq{first, second} {
		msg, _, code, err := decoder.Decode(&keys.SendMac, uint64(seq), frames[seq])
		require.NoError(t, err)
		require.Equal(t, pbpub.Code_SUCCESS, code)
		require.True(t, proto.Equal(want, msg))
	}
	router.Close()
}
