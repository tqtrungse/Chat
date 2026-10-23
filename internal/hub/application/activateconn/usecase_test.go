/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package activateconn

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	pbpub "xxx/api/hub/v2/proto/gen/pub"
	"xxx/internal/hub/application/activateconn/mocks"
	"xxx/internal/hub/core/auth"
	"xxx/internal/hub/core/connection"
	"xxx/internal/hub/core/protocol"
	"xxx/internal/hub/core/rejection"
	"xxx/internal/hub/core/transport"
	transportmocks "xxx/internal/hub/core/transport/mocks"
	"xxx/internal/shared/device"
	"xxx/internal/shared/hubendpoint"
	"xxx/internal/shared/ticket/tickettest"

	"xxx/pkg/log"
	"xxx/pkg/nio"
	niomocks "xxx/pkg/nio/mocks"
	niotest "xxx/pkg/nio/testutil"
	slicepool "xxx/pkg/pool/slice"
	"xxx/pkg/reject"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type env struct {
	ca        *ConnActivator
	manager   *mocks.MockConnManager
	service   *mocks.MockActivationService
	factory   *transportmocks.MockFactory
	responder *transportmocks.MockResponder
	channel   *transportmocks.MockChannel
	jobs      *queuedAuthorization
}

func setup(t *testing.T, ctrl *gomock.Controller, cfg Config) env {
	t.Helper()

	logger, closeLogger, loggerErr := log.NewLogger(nil)
	require.NoError(t, loggerErr)
	t.Cleanup(func() { _ = closeLogger() })

	manager := mocks.NewMockConnManager(ctrl)
	if cfg.MaxPreAuthConns == 0 {
		cfg.MaxPreAuthConns = 10
	}
	// Tests provide the runtime fields which production bootstrap resolves.
	if cfg.HandshakeTimeout == 0 {
		cfg.HandshakeTimeout = 5 * time.Second
	}
	if cfg.MaxFrameBytes == 0 {
		cfg.MaxFrameBytes = 4096
	}

	service := mocks.NewMockActivationService(ctrl)
	factory := transportmocks.NewMockFactory(ctrl)
	cfg.Load(nil)

	jobs := new(queuedAuthorization)
	rejections := reject.New[rejection.Reason](
		logger,
		0,
		rejection.Names(),
	)
	out := env{
		jobs: jobs,
		ca: New(
			cfg,
			logger,
			rejections,
			protocol.NewDecoder(),
			manager,
			service,
			factory,
			jobs,
		),
		manager:   manager,
		service:   service,
		factory:   factory,
		responder: transportmocks.NewMockResponder(ctrl),
		channel:   transportmocks.NewMockChannel(ctrl),
	}
	require.Same(t, rejections, out.ca.rejections)
	return out
}

func (e env) admit(t *testing.T, ctrl *gomock.Controller) *niomocks.MockConn {
	t.Helper()
	return e.admitOnLoop(t, ctrl, new(niotest.Loop))
}

func (e env) admitOnLoop(
	t *testing.T,
	ctrl *gomock.Controller,
	loop nio.EventLoop,
) *niomocks.MockConn {
	t.Helper()

	conn := niomocks.NewMockConn(ctrl)
	var mu sync.RWMutex
	var ctx any

	conn.EXPECT().
		Context().
		DoAndReturn(func() any {
			mu.RLock()
			defer mu.RUnlock()
			return ctx
		}).
		AnyTimes()

	conn.EXPECT().
		SetContext(gomock.Any()).
		Do(func(v any) {
			mu.Lock()
			defer mu.Unlock()
			ctx = v
		}).
		AnyTimes()

	conn.EXPECT().
		EventLoop().
		Return(loop).
		AnyTimes()

	require.True(t, e.ca.Admit(t.Context(), conn))

	pa := conn.Context().(*pendingActivation)
	t.Cleanup(pa.clear)
	return conn
}

func (e env) hello(t *testing.T, ctrl *gomock.Controller) *niomocks.MockConn {
	t.Helper()

	conn := e.admit(t, ctrl)
	identity := transport.Identity{
		Audience: "hub.chat",
		KeyID:    7,
		Version:  hubendpoint.WireVersion,
	}
	body := make([]byte, 53)
	body[0] = hubendpoint.WireVersion
	binary.LittleEndian.PutUint32(body[1:], identity.KeyID)
	frame, err := protocol.EncodeFrame(body, e.ca.maxFrame)
	require.NoError(t, err)

	defer func() {
		clear(frame[:cap(frame)])
		slicepool.Put(frame)
	}()

	gomock.InOrder(
		e.factory.
			EXPECT().
			NewResponder(identity.KeyID).
			Return(e.responder, nil),
		e.responder.
			EXPECT().
			ReadClientHello(body[5:]).
			Return(nil),
		e.responder.
			EXPECT().
			Identity().
			Return(identity),
		e.responder.
			EXPECT().
			WriteServerHello(gomock.Any()).
			Return(
				[]byte("server hello"),
				e.channel,
				[]byte("channel binding"),
				nil,
			),
		e.responder.
			EXPECT().
			Close(),
	)
	// The usecase owns the channel until registration transfers it to the manager.
	e.channel.
		EXPECT().
		Close().
		AnyTimes()

	conn.EXPECT().
		Write(gomock.Any()).
		DoAndReturn(func(b []byte) (int, error) {
			return len(b), nil
		}).
		Times(1)

	require.Equal(t, nio.None, e.ca.HandleFrame(t.Context(), conn, frame))

	return conn
}

func (e env) auth(t *testing.T) ([]byte, *pbpub.AuthenticateReq) {
	t.Helper()

	raw, signature := tickettest.Authorization()
	req := &pbpub.AuthenticateReq{
		Ticket:    raw,
		Signature: signature,
	}
	plain, err := protocol.NewEncoder().EncodePayload(
		pbpub.MsgKind_REQ_AUTHENTICATE,
		req,
		nil,
	)
	require.NoError(t, err)

	frame := []byte("opaque transport record")
	// DecryptRecord transfers plain to the usecase, which clears and releases it.
	e.channel.
		EXPECT().
		DecryptRecord(frame).
		Return(plain, nil)

	return frame, req
}

func (e env) expectAuthorization(c *niomocks.MockConn, req *pbpub.AuthenticateReq) *gomock.Call {
	pa := c.Context().(*pendingActivation)
	return e.service.
		EXPECT().
		VerifyAuthorization(
			req.Ticket,
			req.Signature,
			pa.binding,
			pa.challenge,
			pa.identity,
		)
}

func TestAuthenticateKeepsSlotUntilRegistrationAndACK(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	var (
		envi       = setup(t, ctrl, Config{})
		conn       = envi.hello(t, ctrl)
		frame, req = envi.auth(t)
		claims     = tickettest.New()
		complete   func(error)
		release    func()
	)

	envi.expectAuthorization(conn, req).Return(claims, nil)
	envi.service.
		EXPECT().
		CheckExpiry(claims, gomock.Any()).
		Return(nil)

	envi.manager.
		EXPECT().
		ActivateConn(
			gomock.Any(),
			claims.DeviceID,
			envi.channel,
			conn,
			gomock.Any(),
			gomock.Any(),
		).
		DoAndReturn(func(_ context.Context, _ device.ID, _ transport.Channel, _ nio.Conn, cleanup func(), cb func(error)) error {
			release = cleanup
			complete = cb
			return nil
		})

	require.Equal(t, nio.None, envi.ca.HandleFrame(t.Context(), conn, frame))
	envi.finishAuthorization(conn)
	require.EqualValues(t, 1, envi.ca.PreAuthConns())
	require.Equal(t, stageRegistering, conn.Context().(*pendingActivation).stage)
	require.Nil(t, conn.Context().(*pendingActivation).channel)
	gomock.InOrder(
		envi.service.
			EXPECT().
			CheckExpiry(claims, gomock.Any()).
			Return(nil),
		envi.manager.
			EXPECT().
			CompleteActivation(conn, gomock.Any()).
			DoAndReturn(func(_ nio.Conn, data connection.SendData) error {
				require.Equal(t, pbpub.MsgKind_RESP_AUTHENTICATE, data.MsgKind)
				require.Equal(t, pbpub.Code_SUCCESS, data.Msg.(*pbpub.AuthenticateResp).Code)
				require.EqualValues(t, 1, envi.ca.PreAuthConns())
				release()
				return nil
			}),
	)
	complete(nil)
	require.Zero(t, envi.ca.PreAuthConns())
}

func TestAuthenticateRejectsAuthorizationError(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, tc := range []struct {
		name   string
		err    error
		reason rejection.Reason
	}{
		{
			name:   "ticket rejected",
			err:    errors.New("authorization rejected"),
			reason: rejection.ActivateTicket,
		},
		{
			name:   "proof rejected",
			err:    auth.ErrDeviceProofInvalid,
			reason: rejection.ProofInvalid,
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				envi := setup(t, ctrl, Config{})
				conn := envi.hello(t, ctrl)
				frame, req := envi.auth(t)

				envi.expectAuthorization(conn, req).Return(nil, tc.err)

				envi.manager.
					EXPECT().
					ForceClose(conn)

				require.Equal(t, nio.None, envi.ca.HandleFrame(t.Context(), conn, frame))

				envi.finishAuthorization(conn)
				require.Zero(t, envi.ca.PreAuthConns())
				require.Equal(t, uint64(1), envi.ca.rejections.Count(tc.reason))
			},
		)
	}
}

func TestAuthenticateRejectsInvalidRecordAndRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, kind := range []string{
		"decrypt error",
		"malformed payload",
		"business before auth",
		"invalid request",
		"expand",
	} {
		t.Run(
			kind,
			func(t *testing.T) {
				var (
					envi       = setup(t, ctrl, Config{})
					conn       = envi.hello(t, ctrl)
					plain      []byte
					decryptErr error
				)

				switch kind {
				case "decrypt error":
					decryptErr = errors.New("record rejected")
				case "malformed payload":
					plain = []byte{1}
				case "business before auth":
					plain, _ = protocol.NewEncoder().EncodePayload(
						pbpub.MsgKind_REQ_SEND_MSG,
						&pbpub.SendMsgReq{ChannelId: 1},
						nil,
					)
				default:
					raw, signature := tickettest.Authorization()
					if kind == "invalid request" {
						raw = nil
					}

					var expand []byte
					if kind == "expand" {
						expand = []byte{1}
					}

					plain, _ = protocol.NewEncoder().EncodePayload(
						pbpub.MsgKind_REQ_AUTHENTICATE,
						&pbpub.AuthenticateReq{
							Ticket:    raw,
							Signature: signature,
						},
						expand,
					)
				}

				envi.channel.
					EXPECT().
					DecryptRecord([]byte("record")).
					Return(plain, decryptErr)

				require.Equal(
					t,
					nio.Close,
					envi.ca.HandleFrame(t.Context(), conn, []byte("record")),
				)
				require.Zero(t, envi.ca.PreAuthConns())
				reason := rejection.ActivateDecode
				switch kind {
				case "decrypt error":
					reason = rejection.ProofDecode
				case "invalid request":
					reason = rejection.ActivateValidate
				}
				require.Equal(t, uint64(1), envi.ca.rejections.Count(reason))
			},
		)
	}
}

func TestRegistrationFailuresCloseWithoutACK(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, kind := range []string{
		"start error",
		"callback error",
		"expired",
		"ACK error",
		"late callback",
	} {
		t.Run(
			kind,
			func(t *testing.T) {
				var (
					complete   func(error)
					startErr   error
					envi       = setup(t, ctrl, Config{})
					conn       = envi.hello(t, ctrl)
					frame, req = envi.auth(t)
					claims     = tickettest.New()
				)

				envi.expectAuthorization(conn, req).Return(claims, nil)

				envi.service.
					EXPECT().
					CheckExpiry(claims, gomock.Any()).
					Return(nil)

				if kind == "start error" {
					startErr = errors.New("registration unavailable")
				}

				envi.manager.
					EXPECT().
					ActivateConn(
						gomock.Any(),
						claims.DeviceID,
						envi.channel,
						conn,
						gomock.Any(),
						gomock.Any(),
					).
					DoAndReturn(func(_ context.Context, _ device.ID, _ transport.Channel, _ nio.Conn, cleanup func(), cb func(error)) error {
						complete = cb
						if startErr != nil {
							cleanup()
						}
						return startErr
					})

				if startErr != nil {
					envi.manager.
						EXPECT().
						ForceClose(conn)
				}
				action := envi.ca.HandleFrame(t.Context(), conn, frame)
				envi.finishAuthorization(conn)

				if startErr != nil {
					require.Equal(t, nio.None, action)
				} else {
					require.Equal(t, nio.None, action)
					var callbackErr error

					switch kind {
					case "callback error":
						callbackErr = errors.New("registration failed")
					case "expired":
						envi.service.
							EXPECT().
							CheckExpiry(claims, gomock.Any()).
							Return(errors.New("expired"))
					case "ACK error":
						envi.service.
							EXPECT().
							CheckExpiry(claims, gomock.Any()).
							Return(nil)

						envi.manager.
							EXPECT().
							CompleteActivation(conn, gomock.Any()).
							Return(errors.New("ACK failed"))
					case "late callback":
						conn.Context().(*pendingActivation).clear()
					}

					if kind != "late callback" {
						envi.manager.EXPECT().ForceClose(conn)
					}
					complete(callbackErr)
				}
				require.Zero(t, envi.ca.PreAuthConns())
			},
		)
	}
}

func TestAdmissionTimeoutAndCleanup(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	envi := setup(
		t,
		ctrl,
		Config{
			MaxPreAuthConns:  1,
			HandshakeTimeout: 20 * time.Millisecond,
			PreAuthTimeout:   time.Second,
		},
	)
	conn := envi.admit(t, ctrl)

	require.False(
		t,
		envi.ca.Admit(
			t.Context(),
			niomocks.NewMockConn(gomock.NewController(t)),
		),
	)

	envi.manager.
		EXPECT().
		ForceClose(conn).
		Do(func(nio.Conn) {
			envi.manager.
				EXPECT().
				RemoveConn(gomock.Any(), conn)

			envi.ca.Deactivate(t.Context(), conn)
		})

	require.Eventually(
		t,
		func() bool {
			conn.EventLoop().(*niotest.Loop).Drain()
			return envi.ca.PreAuthConns() == 0
		},
		time.Second, time.Millisecond,
	)
	require.Nil(t, conn.Context())

	next := envi.admit(t, ctrl)
	envi.manager.
		EXPECT().
		RemoveConn(gomock.Any(), next)

	require.EqualValues(t, 1, envi.ca.PreAuthConns())
	require.Equal(
		t,
		nio.None,
		envi.ca.Deactivate(t.Context(), next),
	)
	require.Zero(t, envi.ca.PreAuthConns())
}

func TestExpiredAuthPhaseReleasesAdmission(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	envi := setup(t, ctrl, Config{})
	conn := envi.hello(t, ctrl)
	conn.Context().(*pendingActivation).phaseDeadline = time.Now().Add(-time.Second)
	require.Equal(
		t,
		nio.Close,
		envi.ca.HandleFrame(t.Context(), conn, []byte("record")),
	)
	require.Zero(t, envi.ca.PreAuthConns())
}

func TestTimersCloseSilentPendingConnections(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, stage := range []uint8{
		stageConnected,
		stageAwaitAuth,
		stageVerifying,
		stageRegistering,
	} {
		t.Run(
			map[uint8]string{
				stageConnected:   "waiting for hello",
				stageAwaitAuth:   "waiting for auth",
				stageVerifying:   "verifying auth",
				stageRegistering: "registering presence",
			}[stage],
			func(t *testing.T) {
				envi := setup(
					t,
					ctrl,
					Config{
						HandshakeTimeout: time.Hour,
						PreAuthTimeout:   time.Hour,
					},
				)

				var conn *niomocks.MockConn
				if stage == stageConnected {
					conn = envi.admit(t, ctrl)
				} else {
					conn = envi.hello(t, ctrl)
				}

				if stage == stageRegistering {
					frame, req := envi.auth(t)
					claims := tickettest.New()

					envi.expectAuthorization(conn, req).Return(claims, nil)

					envi.service.
						EXPECT().
						CheckExpiry(claims, gomock.Any()).
						Return(nil)

					envi.manager.
						EXPECT().
						ActivateConn(
							gomock.Any(),
							claims.DeviceID,
							envi.channel,
							conn,
							gomock.Any(),
							gomock.Any(),
						).
						Return(nil)

					require.Equal(
						t,
						nio.None,
						envi.ca.HandleFrame(t.Context(), conn, frame),
					)
					envi.finishAuthorization(conn)
				}

				pa := conn.Context().(*pendingActivation)
				if stage == stageVerifying {
					pa.stage = stageVerifying
				}
				require.Equal(t, stage, pa.stage)

				challenge := pa.challenge
				envi.manager.
					EXPECT().
					ForceClose(conn).
					Do(func(nio.Conn) {
						envi.manager.
							EXPECT().
							RemoveConn(gomock.Any(), conn)

						envi.ca.Deactivate(t.Context(), conn)
					})

				// Fire the installed production timer immediately without waiting for its TTL.
				if stage == stageRegistering {
					pa.timer.Reset(0)
				} else {
					pa.phaseTimer.Reset(0)
				}

				require.Eventually(
					t,
					func() bool {
						conn.EventLoop().(*niotest.Loop).Drain()
						return envi.ca.PreAuthConns() == 0
					},
					time.Second, time.Millisecond,
				)
				require.True(t, pa.released.Load())
				require.Nil(t, conn.Context())
				require.Nil(t, pa.channel)
				require.Nil(t, pa.challenge)
				require.Nil(t, pa.binding)

				for _, b := range challenge {
					require.Zero(t, b)
				}
			},
		)
	}
}

func TestTimerPassesContextAndIgnoresStaleCallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, kind := range []string{
		"stage advanced",
		"released",
	} {
		t.Run(
			kind,
			func(t *testing.T) {
				envi := setup(
					t,
					ctrl,
					Config{
						HandshakeTimeout: time.Hour,
						PreAuthTimeout:   time.Hour,
					},
				)
				loop := niomocks.NewMockEventLoop(gomock.NewController(t))
				conn := envi.admitOnLoop(t, ctrl, loop)
				pa := conn.Context().(*pendingActivation)
				queued := make(chan nio.RunnableFunc, 1)

				loop.
					EXPECT().
					Execute(
						t.Context(),
						gomock.Any(),
					).
					DoAndReturn(func(_ context.Context, fn nio.RunnableFunc) error {
						queued <- fn
						return nil
					})
				pa.phaseTimer.Reset(0)

				select {
				case fn := <-queued:
					switch kind {
					case "stage advanced":
						pa.stage = stageAwaitAuth
					case "released":
						pa.clear()

					}
					// A stale callback must not close the connection.
					require.NoError(t, fn(t.Context()))
				case <-time.After(time.Second):
					t.Fatal("timer did not enqueue its callback")
				}
			},
		)
	}
}

func TestTimerClosesConnectionWhenEventLoopRejectsCallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	envi := setup(
		t,
		ctrl,
		Config{
			HandshakeTimeout: time.Hour,
			PreAuthTimeout:   time.Hour,
		},
	)
	conn := envi.admit(t, ctrl)
	conn.EventLoop().(*niotest.Loop).Err = errors.New("event loop stopped")
	closed := make(chan struct{})
	conn.EXPECT().
		Close().
		DoAndReturn(
			func() error {
				close(closed)
				return nil
			},
		)
	conn.Context().(*pendingActivation).phaseTimer.Reset(0)

	select {
	case <-closed:
	case <-time.After(time.Second):
		require.Fail(t, "connection was not closed after Execute failed")
	}
}

func TestHandshakeRejectsInvalidFrameAndFactoryFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, kind := range []string{
		"invalid frame",
		"factory failure",
	} {
		t.Run(
			kind,
			func(t *testing.T) {
				envi := setup(t, ctrl, Config{})
				conn := envi.admit(t, ctrl)
				frame := []byte("invalid frame")

				if kind == "factory failure" {
					body := bytes.Repeat([]byte{0}, 53)
					body[0] = hubendpoint.WireVersion
					binary.LittleEndian.PutUint32(body[1:], 7)
					var err error
					frame, err = protocol.EncodeFrame(body, envi.ca.maxFrame)
					require.NoError(t, err)
					t.Cleanup(func() {
						clear(frame[:cap(frame)])
						slicepool.Put(frame)
					})

					envi.factory.
						EXPECT().
						NewResponder(uint32(7)).
						Return(nil, errors.New("unknown key"))
				}
				require.Equal(t, nio.Close, envi.ca.HandleFrame(t.Context(), conn, frame))
				require.Zero(t, envi.ca.PreAuthConns())
			},
		)
	}
}

// queuedAuthorization keeps scheduling deterministic without importing an executor.
type queuedAuthorization struct {
	jobs []func()
	err  error
}

func (q *queuedAuthorization) Submit(job func()) error {
	if q.err != nil {
		return q.err
	}
	q.jobs = append(q.jobs, job)
	return nil
}

func (e env) finishAuthorization(conn *niomocks.MockConn) {
	jobs := e.jobs.jobs
	e.jobs.jobs = nil
	for _, job := range jobs {
		job()
	}
	conn.EventLoop().(*niotest.Loop).Drain()
}
