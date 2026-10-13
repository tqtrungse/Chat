/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package activate_conn

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pbpub "xxx/api/hub/v1/proto/gen/pub"
	"xxx/internal/hub/connection"
	connectionmocks "xxx/internal/hub/connection/mocks"
	"xxx/internal/hub/protocol"
	protocolmocks "xxx/internal/hub/protocol/mocks"
	shareddevice "xxx/internal/shared/device"
	sharedticket "xxx/internal/shared/ticket"

	"xxx/pkg/log"
	"xxx/pkg/nio"
	workerpool "xxx/pkg/pool/worker"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

const testDeviceID uint64 = 0x12A

type usecaseTestEnv struct {
	uc             *connActivator
	ticketVerifier *connectionmocks.MockTicketVerifier
	signer         *connectionmocks.MockSigner
	cache          *connectionmocks.MockDistributedCache
}

// testConn implements only the nio.Conn methods used by the activation flow.
// The embedded interface keeps unrelated socket methods out of the test double.
type testConn struct {
	nio.Conn

	mu           sync.RWMutex
	ctx          any
	writeFn      func([]byte) (int, error)
	asyncWriteFn func([]byte, nio.AsyncCallback) error
}

func (c *testConn) Context() any {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ctx
}

func (c *testConn) SetContext(ctx any) {
	c.mu.Lock()
	c.ctx = ctx
	c.mu.Unlock()
}

func (c *testConn) Write(p []byte) (int, error) {
	if c.writeFn == nil {
		return 0, errors.New("unexpected synchronous write")
	}
	return c.writeFn(p)
}

func (c *testConn) AsyncWrite(p []byte, callback nio.AsyncCallback) error {
	if c.asyncWriteFn == nil {
		return errors.New("unexpected asynchronous write")
	}
	return c.asyncWriteFn(p, callback)
}

func (c *testConn) Close() error { return nil }

func newUsecaseTestEnv(t *testing.T, cfg Config) *usecaseTestEnv {
	t.Helper()

	ctrl := gomock.NewController(t)
	rootCtx, cancel := context.WithCancel(context.Background())
	pool, closePool := workerpool.New(workerpool.Config{NumWorkers: 1}, nil)
	logger, closeLogger := log.NewLogger(&log.FileConfig{
		Filename:      filepath.Join(t.TempDir(), "activate-conn-test.log"),
		BufferSize:    1024,
		FlushInterval: time.Hour,
	})

	ticketVerifier := connectionmocks.NewMockTicketVerifier(ctrl)
	signer := connectionmocks.NewMockSigner(ctrl)
	cache := connectionmocks.NewMockDistributedCache(ctrl)
	cb := connectionmocks.NewMockCircuitBreaker(ctrl)
	cb.EXPECT().Execute(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, fn func(context.Context) error) error {
			return fn(ctx)
		},
	).AnyTimes()

	router := connection.NewRouter(
		rootCtx,
		connection.Config{
			Router: connection.RouterConfig{
				MaxConns:       8,
				TickerDuration: 17 * time.Second,
			},
			PresenceSync: connection.PresenceSyncConfig{
				BatchSize:     1,
				Linger:        time.Hour,
				RetryAttempts: 1,
			},
		},
		signer,
		ticketVerifier,
		cache,
		cb,
		logger,
		pool,
		protocol.NewEncoder(protocolmocks.NewMockSecureChannel(ctrl)),
	)

	decoder := protocol.NewDecoder(protocolmocks.NewMockSecureChannel(ctrl))
	uc := New(cfg, logger, protocol.NewEncoder(protocolmocks.NewMockSecureChannel(ctrl)), decoder, router).(*connActivator)
	env := &usecaseTestEnv{
		uc:             uc,
		ticketVerifier: ticketVerifier,
		signer:         signer,
		cache:          cache,
	}
	t.Cleanup(func() {
		cancel()
		router.Close()
		closePool()
		_ = closeLogger()
	})
	return env
}

func (e *usecaseTestEnv) mockConn(t *testing.T) *testConn {
	t.Helper()
	return &testConn{}
}

func newActiveRequest() *pbpub.ActiveConnReq {
	token := make([]byte, 8)
	binary.LittleEndian.PutUint64(token, testDeviceID)
	return &pbpub.ActiveConnReq{
		Token:  token,
		Sign:   bytes.Repeat([]byte{0xA3}, 64),
		Ticket: []byte("raw-ticket-envelope"),
	}
}

func newTestTicket() *sharedticket.Ticket {
	ticket := &sharedticket.Ticket{DeviceID: testDeviceID, ExpiresAt: time.Now().Add(time.Hour).Unix()}
	for i := range ticket.Keys.RecvEnc {
		ticket.Keys.RecvEnc[i] = byte(i + 1)
		ticket.Keys.SendEnc[i] = byte(i + 2)
		ticket.Keys.RecvMac[i] = byte(i + 3)
		ticket.Keys.SendMac[i] = byte(i + 4)
		ticket.ActivationKey[i] = byte(i + 5)
		ticket.IdentityPub[i] = byte(i + 6)
	}
	return ticket
}

func marshalFrame(t *testing.T, msgType pbpub.MsgType, msg proto.Message) []byte {
	t.Helper()
	body, err := proto.Marshal(msg)
	require.NoError(t, err)
	frame := make([]byte, 4, 4+len(body))
	binary.LittleEndian.PutUint16(frame[:2], uint16(2+len(body)))
	binary.LittleEndian.PutUint16(frame[2:4], uint16(msgType))
	return append(frame, body...)
}

func (e *usecaseTestEnv) startChallenge(t *testing.T, conn *testConn) (
	*sharedticket.Ticket,
	*pbpub.ActiveConnReq,
	[]byte,
) {
	t.Helper()
	require.True(t, e.uc.Admit(conn))

	req := newActiveRequest()
	ticket := newTestTicket()
	e.ticketVerifier.EXPECT().Open(testDeviceID, gomock.Eq(req.Ticket)).Return(ticket, nil).Times(1)
	e.signer.EXPECT().Verify(
		gomock.Eq(ticket.IdentityPub[:]),
		gomock.Eq(req.Ticket),
		gomock.Eq(req.Sign),
	).Return(nil).Times(1)

	var challenge []byte
	conn.writeFn = func(frame []byte) (int, error) {
		challenge = bytes.Clone(frame)
		return len(frame), nil
	}

	action := e.uc.Activate(conn, marshalFrame(t, pbpub.MsgType_REQ_ACTIVE_CONN, req))
	require.Equal(t, nio.None, action)
	require.NotNil(t, challenge)
	require.Equal(t, uint16(pbpub.MsgType_RESP_ACTIVE_CONN), binary.LittleEndian.Uint16(challenge[2:4]))
	return ticket, req, challenge
}

func TestAdmit_EnforcesPreAuthLimitAndReleasesSlot(t *testing.T) {
	env := newUsecaseTestEnv(
		t,
		Config{
			PreAuthTimeout:  time.Hour,
			MaxPreAuthConns: 1,
		},
	)
	first := env.mockConn(t)
	second := env.mockConn(t)

	require.True(t, env.uc.Admit(first))
	require.EqualValues(t, 1, env.uc.PreAuthConns())
	require.False(t, env.uc.Admit(second))
	require.Nil(t, second.Context())

	require.Equal(t, nio.None, env.uc.Deactivate(context.Background(), first))
	require.EqualValues(t, 0, env.uc.PreAuthConns())
	require.True(t, env.uc.Admit(second), "deactivation must release its pre-auth slot")
	require.Equal(t, nio.None, env.uc.Deactivate(context.Background(), second))
	require.EqualValues(t, 0, env.uc.PreAuthConns())
}

func TestActivate_RejectsMissingStateAndMalformedRequest(t *testing.T) {
	t.Run("missing pending state", func(t *testing.T) {
		env := newUsecaseTestEnv(t, Config{MaxPreAuthConns: 1})
		conn := env.mockConn(t)
		require.Equal(t, nio.Close, env.uc.Activate(conn, []byte{0, 0, 1, 0}))
	})

	t.Run("malformed protobuf", func(t *testing.T) {
		env := newUsecaseTestEnv(t, Config{MaxPreAuthConns: 1})
		conn := env.mockConn(t)
		require.True(t, env.uc.Admit(conn))
		packet := []byte{0, 0, byte(pbpub.MsgType_REQ_ACTIVE_CONN), 0, 0xFF}
		require.Equal(t, nio.Close, env.uc.Activate(conn, packet))
		require.EqualValues(t, 0, env.uc.PreAuthConns())
	})

	t.Run("invalid fields", func(t *testing.T) {
		env := newUsecaseTestEnv(t, Config{MaxPreAuthConns: 1})
		conn := env.mockConn(t)
		require.True(t, env.uc.Admit(conn))
		packet := marshalFrame(t, pbpub.MsgType_REQ_ACTIVE_CONN, &pbpub.ActiveConnReq{Token: []byte{1}})
		require.Equal(t, nio.Close, env.uc.Activate(conn, packet))
		require.EqualValues(t, 0, env.uc.PreAuthConns())
	})
}

func TestActivate_ValidRequestIssuesFreshChallenge(t *testing.T) {
	env := newUsecaseTestEnv(
		t,
		Config{
			PreAuthTimeout:  time.Hour,
			MaxPreAuthConns: 1,
		},
	)
	conn := env.mockConn(t)
	ticket, _, challenge := env.startChallenge(t, conn)
	defer env.uc.Deactivate(context.Background(), conn)

	resp := new(pbpub.ActiveConnResp)
	require.NoError(t, proto.Unmarshal(challenge[6:], resp))
	require.Len(t, resp.Nonce, 32)
	require.Equal(t, stageChallenged, conn.Context().(*pendingActivation).stage)
	require.True(t, time.Now().Before(conn.Context().(*pendingActivation).deadline))
	require.NotEqual(t, [32]byte{}, ticket.ActivationKey)
	require.EqualValues(t, 1, env.uc.PreAuthConns())
}

func TestActivate_RejectsSecondActivationOnSameConnection(t *testing.T) {
	env := newUsecaseTestEnv(
		t,
		Config{
			PreAuthTimeout:  time.Hour,
			MaxPreAuthConns: 1,
		},
	)
	conn := env.mockConn(t)
	ticket, _, _ := env.startChallenge(t, conn)

	require.Equal(t, nio.Close, env.uc.Activate(conn, []byte{0, 0, 1, 0}))
	require.EqualValues(t, 0, env.uc.PreAuthConns())
	require.Equal(t, [32]byte{}, ticket.ActivationKey)
}

func TestProve_RejectsInvalidProofAndExpiredChallenge(t *testing.T) {
	t.Run("wrong proof", func(t *testing.T) {
		env := newUsecaseTestEnv(
			t,
			Config{
				PreAuthTimeout:  time.Hour,
				MaxPreAuthConns: 1,
			},
		)
		conn := env.mockConn(t)
		ticket, _, _ := env.startChallenge(t, conn)
		packet := marshalFrame(t, pbpub.MsgType_REQ_ACTIVE_CONN_PROOF, &pbpub.ActiveConnProofReq{
			Proof: bytes.Repeat([]byte{0xFF}, 32),
		})

		require.Equal(t, nio.Close, env.uc.Prove(context.Background(), conn, packet))
		require.EqualValues(t, 0, env.uc.PreAuthConns())
		require.Equal(t, [32]byte{}, ticket.ActivationKey)
	})

	t.Run("expired deadline", func(t *testing.T) {
		env := newUsecaseTestEnv(
			t,
			Config{
				PreAuthTimeout:  time.Hour,
				MaxPreAuthConns: 1,
			},
		)
		conn := env.mockConn(t)
		ticket, _, _ := env.startChallenge(t, conn)
		pending := conn.Context().(*pendingActivation)
		pending.deadline = time.Now().Add(-time.Second)
		packet := marshalFrame(t, pbpub.MsgType_REQ_ACTIVE_CONN_PROOF, &pbpub.ActiveConnProofReq{
			Proof: bytes.Repeat([]byte{0xFF}, 32),
		})

		require.Equal(t, nio.Close, env.uc.Prove(context.Background(), conn, packet))
		require.EqualValues(t, 0, env.uc.PreAuthConns())
		require.Equal(t, [32]byte{}, ticket.ActivationKey)
	})
}

func TestProve_ActivatesSessionAndSendsAuthenticatedAck(t *testing.T) {
	env := newUsecaseTestEnv(
		t,
		Config{
			PreAuthTimeout:  time.Hour,
			MaxPreAuthConns: 1,
		},
	)
	conn := env.mockConn(t)
	ticket, activeReq, challenge := env.startChallenge(t, conn)

	resp := new(pbpub.ActiveConnResp)
	require.NoError(t, proto.Unmarshal(challenge[6:], resp))
	var nonce [32]byte
	copy(nonce[:], resp.Nonce)
	transcript := protocol.ActivationTranscript(activeReq.Token, activeReq.Ticket, activeReq.Sign)
	proof := protocol.ActivationProof(&ticket.ActivationKey, transcript, nonce)
	connKeys, err := protocol.DeriveConnKeys(&ticket.Keys, nonce)
	require.NoError(t, err)
	defer connKeys.Clear()

	env.ticketVerifier.EXPECT().CheckExpiry(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	env.cache.EXPECT().BatchAddDevices(gomock.Any(), gomock.Any()).Return(nil, nil).Times(1)
	deleteStarted := make(chan struct{})
	env.cache.EXPECT().BatchDelDevices(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, []shareddevice.ID) error {
			close(deleteStarted)
			return nil
		},
	).Times(1)
	ackWritten := make(chan []byte, 1)
	conn.asyncWriteFn = func(frame []byte, callback nio.AsyncCallback) error {
		copyOfFrame := bytes.Clone(frame)
		writeErr := callback(conn, nil)
		ackWritten <- copyOfFrame
		return writeErr
	}

	packet := marshalFrame(t, pbpub.MsgType_REQ_ACTIVE_CONN_PROOF, &pbpub.ActiveConnProofReq{Proof: proof[:]})
	require.Equal(t, nio.None, env.uc.Prove(context.Background(), conn, packet))
	require.EqualValues(t, 0, env.uc.PreAuthConns())
	require.IsType(t, (*connection.Session)(nil), conn.Context())

	var ack []byte
	select {
	case ack = <-ackWritten:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the activation ACK")
	}
	require.Len(t, ack, 38)
	require.Equal(t, uint16(pbpub.MsgType_RESP_ACTIVE_CONN_PROOF), binary.LittleEndian.Uint16(ack[2:4]))
	require.Zero(t, binary.LittleEndian.Uint16(ack[4:6]))

	mac := hmac.New(sha256.New, connKeys.SendMac[:])
	var seq [8]byte
	binary.LittleEndian.PutUint64(seq[:], 0)
	_, _ = mac.Write(seq[:])
	_, _ = mac.Write(ack[:len(ack)-32])
	require.True(t, hmac.Equal(mac.Sum(nil), ack[len(ack)-32:]), "ACK must use the derived server-to-client MAC key and sequence zero")

	require.Equal(t, nio.None, env.uc.Deactivate(context.Background(), conn))
	select {
	case <-deleteStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for presence cleanup")
	}
}

func TestTick_ReturnsRouterInterval(t *testing.T) {
	env := newUsecaseTestEnv(t, Config{})
	delay, action := env.uc.Tick()
	require.Equal(t, 17*time.Second, delay)
	require.Equal(t, nio.None, action)
}

func TestActivate_TicketOpenFailureClosesAndReleasesPendingState(t *testing.T) {
	env := newUsecaseTestEnv(
		t,
		Config{
			PreAuthTimeout:  time.Hour,
			MaxPreAuthConns: 1,
		},
	)
	conn := env.mockConn(t)
	require.True(t, env.uc.Admit(conn))
	req := newActiveRequest()
	env.ticketVerifier.EXPECT().Open(testDeviceID, gomock.Eq(req.Ticket)).Return(nil, errors.New("invalid ticket")).Times(1)

	action := env.uc.Activate(conn, marshalFrame(t, pbpub.MsgType_REQ_ACTIVE_CONN, req))
	require.Equal(t, nio.Close, action)
	require.EqualValues(t, 0, env.uc.PreAuthConns())
}

func TestProve_WithoutChallengeCloses(t *testing.T) {
	env := newUsecaseTestEnv(t, Config{MaxPreAuthConns: 1})
	conn := env.mockConn(t)
	packet := marshalFrame(t, pbpub.MsgType_REQ_ACTIVE_CONN_PROOF, &pbpub.ActiveConnProofReq{
		Proof: bytes.Repeat([]byte{0x01}, 32),
	})
	require.Equal(t, nio.Close, env.uc.Prove(context.Background(), conn, packet))
}
