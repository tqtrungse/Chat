/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package integration_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pbpub "xxx/api/hub/v2/proto/gen/pub"
	"xxx/internal/hub/adapter/tcp"
	"xxx/internal/hub/application/activateconn"
	"xxx/internal/hub/core/activation"
	activationmocks "xxx/internal/hub/core/activation/mocks"
	"xxx/internal/hub/core/auth"
	"xxx/internal/hub/core/connection"
	"xxx/internal/hub/core/protocol"
	"xxx/internal/hub/core/rejection"
	"xxx/internal/hub/core/transport"
	"xxx/internal/hub/infrastructure/authorization"
	"xxx/internal/hub/infrastructure/crypto"
	"xxx/internal/hub/infrastructure/noise"
	"xxx/internal/hub/infrastructure/noise/noisetest"
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

type testPresence struct{}

func (*testPresence) Add(_ context.Context, _ device.ID, _ int64, cb func(any, error)) error {
	cb(nil, nil)
	return nil
}

func (*testPresence) Del(_ context.Context, _ device.ID, _ int64, cb func(error)) error {
	cb(nil)
	return nil
}

type senderSpy struct {
	got    []uint64
	expand [][]byte
}

func (s *senderSpy) Send(_ nio.Conn, req *pbpub.SendMsgReq, expand []byte) nio.Action {
	s.got = append(s.got, req.DedupKey)
	s.expand = append(s.expand, bytes.Clone(expand))
	return nio.None
}

func (*senderSpy) Stop() {}

func serverFixture(
	t *testing.T,
	cfg tcp.Config,
	ctrl *gomock.Controller,
) (
	*tcp.Server,
	*connection.Manager,
	*activationmocks.MockTicketVerifier,
	*senderSpy,
) {
	t.Helper()

	logger, closeLogger, loggerErr := log.NewLogger(nil)
	require.NoError(t, loggerErr)
	t.Cleanup(func() { _ = closeLogger() })

	factory, _ := NoiseFactory(t)
	verifier := activationmocks.NewMockTicketVerifier(ctrl)
	managerConfig := connection.Config{MaxConns: 100}
	managerConfig.Load(nil)
	manager := connection.NewManager(
		managerConfig,
		&testPresence{},
		logger,
		protocol.NewEncoder(),
	)
	t.Cleanup(manager.Close)

	spy := new(senderSpy)
	executor, closeExecutor := authorization.New(2, 8)
	cfgActivation := activateconn.Config{
		HandshakeTimeout: 5 * time.Second,
		MaxFrameBytes:    4096,
		MaxPreAuthConns:  10, // fixture manager supports 100 connections
	}

	t.Cleanup(closeExecutor)
	cfgActivation.Load(nil)
	cfg.Load(nil)

	rejections := reject.New[rejection.Reason](
		logger,
		0,
		rejection.Names(),
	)
	uc := activateconn.New(
		cfgActivation,
		logger,
		rejections,
		protocol.NewDecoder(),
		manager,
		activation.NewService(
			crypto.NewEd25519Signer(),
			verifier,
		),
		factory,
		executor,
	)
	server := tcp.NewServer(
		t.Context(),
		cfg,
		logger,
		rejections,
		uc,
		spy,
		manager.Close,
	)
	return server, manager, verifier, spy
}

func fakeHandshake(
	t *testing.T,
	ctrl *gomock.Controller,
	s *tcp.Server,
) (*niomocks.MockConn, *noisetest.Peer, *socketState) {
	t.Helper()

	conn, state := newIntegrationConn(t, ctrl)
	t.Cleanup(func() { _ = conn.Close(); state.Loop.Drain() })
	state.OnClose = func() { s.OnClose(conn, nil) }
	state.OnWake = func() {
		if s.OnTraffic(conn) == nio.Close {
			_ = conn.Close()
		}
	}

	_, action := s.OnOpen(conn)
	require.Equal(t, nio.None, action)

	peer, err := noisetest.New(tickettest.New().HubPublicKey[:], "hub.chat", 7)
	require.NoError(t, err)

	frame, err := peer.Hello(7)
	require.NoError(t, err)

	state.Input = frame
	require.Equal(t, nio.None, s.OnTraffic(conn))
	require.NoError(t, peer.Finish(state.Writes[0]))

	return conn, peer, state
}

func authFrame(
	t *testing.T,
	peer *noisetest.Peer,
	verifier *activationmocks.MockTicketVerifier,
) []byte {
	t.Helper()

	wire, _ := tickettest.Authorization()
	claims := tickettest.New()

	verifier.
		EXPECT().
		Verify(
			wire,
			transport.Identity{
				Audience:  claims.Audience,
				KeyID:     claims.HubKeyID,
				PublicKey: claims.HubPublicKey,
				Version:   claims.WireVersion,
			},
		).
		Return(claims, nil)

	// Recheck after worker completion and after presence registration, before ACK.
	verifier.
		EXPECT().
		CheckExpiry(claims, gomock.Any()).
		Return(nil).
		Times(2)

	msg, err := auth.SigningMessage(peer.Binding, wire, peer.Challenge)
	require.NoError(t, err)
	defer func() {
		clear(msg[:cap(msg)])
		slicepool.Put(msg)
	}()

	plain, err := protocol.NewEncoder().EncodePayload(
		pbpub.MsgKind_REQ_AUTHENTICATE,
		&pbpub.AuthenticateReq{
			Ticket:    wire,
			Signature: ed25519.Sign(tickettest.IdentityPrivate(), msg),
		},
		nil,
	)
	require.NoError(t, err)

	defer func() {
		clear(plain[:cap(plain)])
		slicepool.Put(plain)
	}()

	frame, err := peer.Encrypt(plain)
	require.NoError(t, err)
	return frame
}

func NoiseFactory(t *testing.T) (*noise.Factory, hubendpoint.Descriptor) {
	t.Helper()

	pub := tickettest.HubPublic()
	data, err := json.Marshal(noise.KeyFile{
		Keys: []noise.KeyEntry{
			{
				KeyID:      7,
				PrivateKey: tickettest.HubPrivate(),
				PublicKey:  pub[:],
			},
		},
	})
	require.NoError(t, err)

	path := filepath.Join(t.TempDir(), "keys.json")
	require.NoError(t, os.WriteFile(path, data, 0o600))

	ring, err := noise.LoadKeyring(path, 7)
	require.NoError(t, err)
	t.Cleanup(ring.Close)

	factory, err := noise.NewFactory(ring, "hub.chat", 4096)
	require.NoError(t, err)

	return factory, hubendpoint.Descriptor{
		Address:   "hub.example.com:9000",
		Audience:  "hub.chat",
		KeyID:     7,
		PublicKey: pub[:],
		Protocol:  hubendpoint.Protocol,
		Version:   hubendpoint.WireVersion,
	}
}

type socketState struct {
	Loop       *niotest.Loop
	mu         sync.RWMutex
	ctx        any
	Input      []byte
	Writes     [][]byte
	WriteErr   error
	Closed     atomic.Bool
	CloseCount atomic.Int32
	WakeCount  atomic.Int32
	OnClose    func()
	OnWake     func()
}

func newIntegrationConn(t *testing.T, ctrl *gomock.Controller) (*niomocks.MockConn, *socketState) {
	t.Helper()

	conn := niomocks.NewMockConn(ctrl)
	state := &socketState{
		Loop: new(niotest.Loop),
	}

	conn.EXPECT().
		Context().
		DoAndReturn(func() any {
			state.mu.RLock()
			defer state.mu.RUnlock()
			return state.ctx
		}).
		AnyTimes()

	conn.EXPECT().
		SetContext(gomock.Any()).
		Do(func(v any) {
			state.mu.Lock()
			defer state.mu.Unlock()
			state.ctx = v
		}).
		AnyTimes()

	conn.EXPECT().
		EventLoop().
		Return(state.Loop).
		AnyTimes()

	conn.EXPECT().
		Write(gomock.Any()).
		DoAndReturn(func(b []byte) (int, error) {
			if state.Closed.Load() {
				return 0, net.ErrClosed
			}

			if state.WriteErr != nil {
				return 0, state.WriteErr
			}

			state.Writes = append(state.Writes, bytes.Clone(b))
			return len(b), nil
		}).
		AnyTimes()

	conn.EXPECT().
		Close().
		DoAndReturn(func() error {
			if state.Closed.CompareAndSwap(false, true) {
				state.CloseCount.Add(1)
				return state.Loop.Execute(t.Context(), func(context.Context) error {
					if state.OnClose != nil {
						state.OnClose()
					}
					return nil
				})
			}
			return nil
		}).
		AnyTimes()

	conn.EXPECT().
		InboundBuffered().
		DoAndReturn(func() int { return len(state.Input) }).
		AnyTimes()

	conn.EXPECT().
		Peek(gomock.Any()).
		DoAndReturn(func(n int) ([]byte, error) {
			if len(state.Input) < n {
				return nil, io.ErrShortBuffer
			}
			return state.Input[:n], nil
		}).
		AnyTimes()

	conn.EXPECT().
		Next(gomock.Any()).
		DoAndReturn(func(n int) ([]byte, error) {
			if len(state.Input) < n {
				return nil, io.ErrShortBuffer
			}

			b := state.Input[:n]
			state.Input = state.Input[n:]
			return b, nil
		}).
		AnyTimes()

	conn.EXPECT().
		Wake(gomock.Any()).
		DoAndReturn(func(cb nio.AsyncCallback) error {
			state.WakeCount.Add(1)
			return state.Loop.Execute(context.Background(), func(context.Context) error {
				if state.OnWake != nil {
					state.OnWake()
				}
				if cb != nil {
					return cb(conn, nil)
				}
				return nil
			})
		}).AnyTimes()

	return conn, state
}
