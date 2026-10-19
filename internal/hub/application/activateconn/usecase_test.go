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

package activateconn

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pbpub "xxx/api/hub/v1/proto/gen/pub"
	"xxx/internal/hub/application/activateconn/mocks"
	"xxx/internal/hub/core/activation"
	"xxx/internal/hub/core/connection"
	"xxx/internal/hub/core/protocol"
	protocolmocks "xxx/internal/hub/core/protocol/mocks"
	shareddevice "xxx/internal/shared/device"
	sharedsession "xxx/internal/shared/session"
	sharedticket "xxx/internal/shared/ticket"

	"xxx/pkg/log"
	"xxx/pkg/nio"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

const (
	testDeviceID shareddevice.ID = 0x12A
	testMaxConns uint32          = 8
)

type usecaseTestEnv struct {
	uc      *connActivator
	manager *mocks.MockConnManager
	svc     *mocks.MockActivationService
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
	return newUsecaseTestEnvWithConns(t, cfg, 0)
}

// newUsecaseTestEnvWithConns lets a test pick what manager.CurConns() reports.
func newUsecaseTestEnvWithConns(t *testing.T, cfg Config, curConns uint32) *usecaseTestEnv {
	t.Helper()

	ctrl := gomock.NewController(t)
	logger, closeLogger := log.NewLogger(
		&log.FileConfig{
			Filename:      filepath.Join(t.TempDir(), "activate-conn-test.log"),
			BufferSize:    1024,
			FlushInterval: time.Hour,
		},
	)
	t.Cleanup(func() { _ = closeLogger() })

	manager := mocks.NewMockConnManager(ctrl)
	// New (default pre-auth cap) and Admit read the capacity in almost every
	// test; it is incidental to what they verify, so it is stubbed once here.
	manager.EXPECT().MaxConns().Return(testMaxConns).AnyTimes()
	manager.EXPECT().CurConns().Return(curConns).AnyTimes()
	svc := mocks.NewMockActivationService(ctrl)

	uc := New(
		cfg,
		logger,
		protocol.NewEncoder(protocolmocks.NewMockSecureChannel(ctrl)),
		protocol.NewDecoder(protocolmocks.NewMockSecureChannel(ctrl)),
		manager,
		svc,
	).(*connActivator)

	return &usecaseTestEnv{uc: uc, manager: manager, svc: svc}
}

func (e *usecaseTestEnv) mockConn(t *testing.T) *testConn {
	t.Helper()
	return &testConn{}
}

func newActiveRequest() *pbpub.ActiveConnReq {
	token := make([]byte, 8)
	binary.LittleEndian.PutUint64(token, testDeviceID.Uint64())
	return &pbpub.ActiveConnReq{
		Token:  token,
		Sign:   bytes.Repeat([]byte{0xA3}, 64),
		Ticket: []byte("raw-ticket-envelope"),
	}
}

func newTestTicket() *sharedticket.Ticket {
	ticket := &sharedticket.Ticket{
		DeviceID:  testDeviceID,
		ExpiresAt: time.Now().Add(time.Hour).Unix(),
	}
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

// startChallenge admits conn and runs Activate against a mocked
// activation service that accepts the request and returns a fresh ticket.
func (e *usecaseTestEnv) startChallenge(t *testing.T, conn *testConn) (
	*sharedticket.Ticket,
	*pbpub.ActiveConnReq,
	[]byte,
) {
	t.Helper()
	require.True(t, e.uc.Admit(conn))

	req := newActiveRequest()
	ticket := newTestTicket()
	e.svc.EXPECT().
		Prepare(
			gomock.Eq(req.Token),
			gomock.Eq(req.Ticket),
			gomock.Eq(req.Sign),
		).
		Return(ticket, nil).
		Times(1)

	var challenge []byte
	conn.writeFn = func(frame []byte) (int, error) {
		challenge = bytes.Clone(frame)
		return len(frame), nil
	}

	action := e.uc.Activate(
		conn,
		marshalFrame(t, pbpub.MsgType_REQ_ACTIVE_CONN, req),
	)
	require.Equal(t, nio.None, action)
	require.NotNil(t, challenge)
	require.Equal(
		t,
		uint16(pbpub.MsgType_RESP_ACTIVE_CONN),
		binary.LittleEndian.Uint16(challenge[2:4]),
	)
	return ticket, req, challenge
}

// validProofFrame builds the client's REQ_ACTIVE_CONN_PROOF frame for the
// challenge and the connection keys the server must derive from it.
// Call it BEFORE Prove: Prove zeroes the ticket.
func validProofFrame(
	t *testing.T,
	ticket *sharedticket.Ticket,
	req *pbpub.ActiveConnReq,
	challenge []byte,
) ([]byte, sharedsession.Keys) {
	t.Helper()

	resp := new(pbpub.ActiveConnResp)
	require.NoError(t, proto.Unmarshal(challenge[6:], resp))

	var nonce [32]byte
	copy(nonce[:], resp.Nonce)
	transcript := activation.Transcript(req.Token, req.Ticket, req.Sign)
	proof := activation.Proof(&ticket.ActivationKey, transcript, nonce)
	connKeys, err := activation.DeriveConnKeys(&ticket.Keys, nonce)
	require.NoError(t, err)

	frame := marshalFrame(
		t,
		pbpub.MsgType_REQ_ACTIVE_CONN_PROOF,
		&pbpub.ActiveConnProofReq{Proof: proof[:]},
	)
	return frame, connKeys
}

// ---------------
// Admit
// ---------------

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

	// Deactivate always delegates to the manager. gomock.Any() on purpose:
	// two fresh testConn values are deeply equal, so Eq() could not tell them apart.
	env.manager.EXPECT().RemoveConn(gomock.Any(), gomock.Any()).Times(2)

	require.True(t, env.uc.Admit(first))
	require.EqualValues(t, 1, env.uc.PreAuthConns())
	require.False(t, env.uc.Admit(second))
	require.Nil(t, second.Context())

	require.Equal(
		t,
		nio.None,
		env.uc.Deactivate(context.Background(), first),
	)
	require.EqualValues(t, 0, env.uc.PreAuthConns())
	require.True(
		t,
		env.uc.Admit(second),
		"deactivation must release its pre-auth slot",
	)
	require.Equal(
		t,
		nio.None,
		env.uc.Deactivate(context.Background(), second),
	)
	require.EqualValues(t, 0, env.uc.PreAuthConns())
}

func TestAdmit_RejectsWhenManagerIsFull(t *testing.T) {
	// The registry reports MaxConns+1 when it is full.
	env := newUsecaseTestEnvWithConns(
		t,
		Config{PreAuthTimeout: time.Hour, MaxPreAuthConns: 1},
		testMaxConns+1,
	)
	conn := env.mockConn(t)

	require.False(t, env.uc.Admit(conn))
	require.Nil(t, conn.Context())
	require.EqualValues(t, 0, env.uc.PreAuthConns())
}

// ---------------
// Activate
// ---------------

func TestActivate_RejectsMissingStateAndMalformedRequest(t *testing.T) {
	t.Run("missing pending state", func(t *testing.T) {
		env := newUsecaseTestEnv(t, Config{MaxPreAuthConns: 1})
		conn := env.mockConn(t)
		require.Equal(
			t,
			nio.Close,
			env.uc.Activate(conn, []byte{0, 0, 1, 0}),
		)
	})

	t.Run("malformed protobuf", func(t *testing.T) {
		env := newUsecaseTestEnv(t, Config{MaxPreAuthConns: 1})
		conn := env.mockConn(t)
		require.True(t, env.uc.Admit(conn))
		packet := []byte{0, 0, byte(pbpub.MsgType_REQ_ACTIVE_CONN), 0, 0xFF}
		require.Equal(
			t,
			nio.Close,
			env.uc.Activate(conn, packet),
		)
		require.EqualValues(t, 0, env.uc.PreAuthConns())
	})

	t.Run("invalid fields", func(t *testing.T) {
		// Validation fails before Prepare: no EXPECT on the service.
		env := newUsecaseTestEnv(t, Config{MaxPreAuthConns: 1})
		conn := env.mockConn(t)
		require.True(t, env.uc.Admit(conn))
		packet := marshalFrame(
			t,
			pbpub.MsgType_REQ_ACTIVE_CONN,
			&pbpub.ActiveConnReq{Token: []byte{1}},
		)
		require.Equal(
			t,
			nio.Close,
			env.uc.Activate(conn, packet),
		)
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
	env.manager.EXPECT().RemoveConn(gomock.Any(), conn).Times(1)

	ticket, _, challenge := env.startChallenge(t, conn)
	defer env.uc.Deactivate(context.Background(), conn)

	resp := new(pbpub.ActiveConnResp)
	require.NoError(t, proto.Unmarshal(challenge[6:], resp))
	require.Len(t, resp.Nonce, 32)
	require.Equal(
		t,
		stageChallenged,
		conn.Context().(*pendingActivation).stage,
	)
	require.True(
		t,
		time.Now().Before(conn.Context().(*pendingActivation).deadline),
	)
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

	require.Equal(
		t,
		nio.Close,
		env.uc.Activate(conn, []byte{0, 0, 1, 0}),
	)
	require.EqualValues(t, 0, env.uc.PreAuthConns())
	require.Equal(t, [32]byte{}, ticket.ActivationKey)
}

func TestActivate_PrepareFailureClosesAndReleasesPendingState(t *testing.T) {
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
	env.svc.EXPECT().
		Prepare(
			gomock.Eq(req.Token),
			gomock.Eq(req.Ticket),
			gomock.Eq(req.Sign),
		).
		Return(nil, errors.New("invalid ticket")).
		Times(1)

	action := env.uc.Activate(
		conn,
		marshalFrame(t, pbpub.MsgType_REQ_ACTIVE_CONN, req),
	)
	require.Equal(t, nio.Close, action)
	require.EqualValues(t, 0, env.uc.PreAuthConns())
}

// ---------------
// Prove
// ---------------

func TestProve_RejectsInvalidProofAndExpiredChallenge(t *testing.T) {
	// Both cases fail before CheckExpiry and ActivateConn: no EXPECT on them.
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
		packet := marshalFrame(
			t,
			pbpub.MsgType_REQ_ACTIVE_CONN_PROOF,
			&pbpub.ActiveConnProofReq{
				Proof: bytes.Repeat([]byte{0xFF}, 32),
			},
		)
		env.svc.
			EXPECT().
			CheckExpiry(
				gomock.Any(),
				gomock.Any(),
			).
			Return(nil).
			Times(1)

		require.Equal(
			t,
			nio.Close,
			env.uc.Prove(context.Background(), conn, packet),
		)
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
		packet := marshalFrame(
			t,
			pbpub.MsgType_REQ_ACTIVE_CONN_PROOF,
			&pbpub.ActiveConnProofReq{
				Proof: bytes.Repeat([]byte{0xFF}, 32),
			},
		)

		require.Equal(
			t,
			nio.Close,
			env.uc.Prove(context.Background(), conn, packet),
		)
		require.EqualValues(t, 0, env.uc.PreAuthConns())
		require.Equal(t, [32]byte{}, ticket.ActivationKey)
	})
}

func TestProve_RejectsExpiredTicket(t *testing.T) {
	env := newUsecaseTestEnv(
		t,
		Config{
			PreAuthTimeout:  time.Hour,
			MaxPreAuthConns: 1,
		},
	)
	conn := env.mockConn(t)
	ticket, activeReq, challenge := env.startChallenge(t, conn)
	packet, _ := validProofFrame(t, ticket, activeReq, challenge)

	env.svc.EXPECT().
		CheckExpiry(gomock.Eq(ticket), gomock.Any()).
		Return(errors.New("ticket expired")).
		Times(1)
	// No EXPECT on manager.ActivateConn: it must not run for an expired ticket.

	require.Equal(
		t,
		nio.Close,
		env.uc.Prove(context.Background(), conn, packet),
	)
	require.EqualValues(t, 0, env.uc.PreAuthConns())
	require.Equal(t, [32]byte{}, ticket.ActivationKey)
}

func TestProve_ActivatesConnectionWithDerivedKeysAndSendsAck(t *testing.T) {
	env := newUsecaseTestEnv(
		t,
		Config{
			PreAuthTimeout:  time.Hour,
			MaxPreAuthConns: 1,
		},
	)
	conn := env.mockConn(t)
	ticket, activeReq, challenge := env.startChallenge(t, conn)
	packet, connKeys := validProofFrame(t, ticket, activeReq, challenge)

	env.svc.EXPECT().
		CheckExpiry(gomock.Eq(ticket), gomock.Any()).
		Return(nil).
		Times(1)

	// The manager must receive the device ID from the ticket and the keys
	// derived from (ticket keys, challenge nonce), not the raw ticket keys.
	// The mock then plays the manager's role: presence is written, callback fires.
	env.manager.EXPECT().
		ActivateConn(
			gomock.Any(),
			testDeviceID,
			gomock.Eq(&connKeys),
			conn,
			gomock.Any(),
		).
		DoAndReturn(
			func(
				_ context.Context,
				id shareddevice.ID,
				_ *sharedsession.Keys,
				_ nio.Conn,
				onAck func(shareddevice.ID),
			) error {
				onAck(id)
				return nil
			},
		).
		Times(1)

	var gotAck connection.SendData
	env.manager.EXPECT().
		AsyncSendConn(false, conn, gomock.Any()).
		DoAndReturn(
			func(_ bool, _ nio.Conn, data connection.SendData) error {
				gotAck = data
				return nil
			},
		).
		Times(1)

	require.Equal(
		t,
		nio.None,
		env.uc.Prove(context.Background(), conn, packet),
	)
	require.EqualValues(t, 0, env.uc.PreAuthConns())
	require.Equal(t, [32]byte{}, ticket.ActivationKey, "pending state must be wiped")

	require.Equal(t, pbpub.MsgType_RESP_ACTIVE_CONN_PROOF, gotAck.MsgType)
	require.True(
		t,
		proto.Equal(&pbpub.ActiveConnProofResp{Code: pbpub.Code_SUCCESS}, gotAck.Msg),
	)
	require.Empty(t, gotAck.Expand)
}

func TestProve_ActivateConnFailureCloses(t *testing.T) {
	env := newUsecaseTestEnv(
		t,
		Config{
			PreAuthTimeout:  time.Hour,
			MaxPreAuthConns: 1,
		},
	)
	conn := env.mockConn(t)
	ticket, activeReq, challenge := env.startChallenge(t, conn)
	packet, _ := validProofFrame(t, ticket, activeReq, challenge)

	env.svc.
		EXPECT().
		CheckExpiry(
			gomock.Any(),
			gomock.Any(),
		).
		Return(nil).
		Times(1)

	env.manager.
		EXPECT().
		ActivateConn(
			gomock.Any(),
			testDeviceID,
			gomock.Any(),
			conn,
			gomock.Any(),
		).
		Return(connection.ErrConnReachMax).
		Times(1)
	// No AsyncSendConn expectation: no ACK may be sent when activation failed.

	require.Equal(
		t,
		nio.Close,
		env.uc.Prove(context.Background(), conn, packet),
	)
	require.EqualValues(t, 0, env.uc.PreAuthConns())
	require.Equal(t, [32]byte{}, ticket.ActivationKey)
}

func TestProve_AckSendFailureForcesClose(t *testing.T) {
	env := newUsecaseTestEnv(
		t,
		Config{
			PreAuthTimeout:  time.Hour,
			MaxPreAuthConns: 1,
		},
	)
	conn := env.mockConn(t)
	ticket, activeReq, challenge := env.startChallenge(t, conn)
	packet, _ := validProofFrame(t, ticket, activeReq, challenge)

	env.svc.
		EXPECT().
		CheckExpiry(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	env.manager.
		EXPECT().
		ActivateConn(gomock.Any(), testDeviceID, gomock.Any(), conn, gomock.Any()).
		DoAndReturn(
			func(
				_ context.Context,
				id shareddevice.ID,
				_ *sharedsession.Keys,
				_ nio.Conn,
				onAck func(shareddevice.ID),
			) error {
				onAck(id)
				return nil
			},
		).
		Times(1)
	env.manager.EXPECT().
		AsyncSendConn(false, conn, gomock.Any()).
		Return(errors.New("session closed")).
		Times(1)
	env.manager.EXPECT().ForceClose(conn).Times(1)

	// The connection was activated; the failed ACK is handled by ForceClose.
	require.Equal(
		t,
		nio.None,
		env.uc.Prove(context.Background(), conn, packet),
	)
	require.EqualValues(t, 0, env.uc.PreAuthConns())
}

func TestProve_WithoutChallengeCloses(t *testing.T) {
	env := newUsecaseTestEnv(t, Config{MaxPreAuthConns: 1})
	conn := env.mockConn(t)
	packet := marshalFrame(
		t,
		pbpub.MsgType_REQ_ACTIVE_CONN_PROOF,
		&pbpub.ActiveConnProofReq{
			Proof: bytes.Repeat([]byte{0x01}, 32),
		},
	)
	require.Equal(
		t,
		nio.Close,
		env.uc.Prove(context.Background(), conn, packet),
	)
}

// ----
// Tick
// ----

func TestTick_ReturnsManagerInterval(t *testing.T) {
	env := newUsecaseTestEnv(t, Config{})
	env.manager.EXPECT().Tick().Return(17 * time.Second).Times(1)

	delay, action := env.uc.Tick()
	require.Equal(t, 17*time.Second, delay)
	require.Equal(t, nio.None, action)
}
