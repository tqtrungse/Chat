/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connection

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"xxx/internal/hub/core/protocol"
	"xxx/internal/hub/core/transport"
	"xxx/internal/hub/infrastructure/noise"
	"xxx/internal/hub/infrastructure/noise/noisetest"
	"xxx/internal/shared/ticket/tickettest"

	"xxx/pkg/log"
	niomocks "xxx/pkg/nio/mocks"
	niotest "xxx/pkg/nio/testutil"
	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func newTestLogger(t *testing.T) *log.Logger {
	t.Helper()

	logger, closeLogger, loggerErr := log.NewLogger(nil)
	require.NoError(t, loggerErr)
	t.Cleanup(func() { _ = closeLogger() })
	return logger
}

type testConnState struct {
	Loop       *niotest.Loop
	mu         sync.RWMutex
	ctx        any
	Writes     [][]byte
	WriteErr   error
	Closed     atomic.Bool
	CloseCount atomic.Int32
	OnClose    func()
}

func newTestConn(t *testing.T, ctrl *gomock.Controller) (*niomocks.MockConn, *testConnState) {
	t.Helper()

	conn := niomocks.NewMockConn(ctrl)
	state := &testConnState{Loop: new(niotest.Loop)}

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
		}).AnyTimes()

	conn.EXPECT().
		Close().
		DoAndReturn(func() error {
			if state.Closed.CompareAndSwap(false, true) {
				state.CloseCount.Add(1)
				return state.Loop.Execute(context.Background(), func(context.Context) error {
					if state.OnClose != nil {
						state.OnClose()
					}
					return nil
				})
			}
			return nil
		}).
		AnyTimes()

	return conn, state
}

type testChannel struct{ closed bool }

func (c *testChannel) EncryptRecord(b []byte) ([]byte, error) {
	return append([]byte(nil), b...), nil
}

func (c *testChannel) DecryptRecord(b []byte) ([]byte, error) {
	return append([]byte(nil), b...), nil
}

func (c *testChannel) Close() {
	c.closed = true
}

func testTransport(t *testing.T) (transport.Channel, *noisetest.Peer) {
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

	f, err := noise.NewFactory(ring, "hub.chat", 4096)
	require.NoError(t, err)

	r, err := f.NewResponder(7)
	require.NoError(t, err)

	p, err := noisetest.New(pub[:], "hub.chat", 7)
	require.NoError(t, err)

	hello, err := p.Hello(7)
	require.NoError(t, err)
	require.NoError(t, r.ReadClientHello(hello[7:]))

	msg, ch, _, err := r.WriteServerHello(make([]byte, 32))
	require.NoError(t, err)
	defer func() {
		clear(msg[:cap(msg)])
		slicepool.Put(msg)
	}()

	frame, err := protocol.EncodeFrame(msg, 4096)
	require.NoError(t, err)
	defer func() {
		clear(frame[:cap(frame)])
		slicepool.Put(frame)
	}()
	require.NoError(t, p.Finish(frame))
	t.Cleanup(ch.Close)

	return ch, p
}
