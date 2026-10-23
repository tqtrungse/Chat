/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package tcp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"syscall"
	"testing"
	"time"

	"xxx/internal/hub/adapter/tcp/mocks"
	"xxx/internal/hub/core/connection"
	"xxx/internal/hub/core/protocol"
	"xxx/internal/hub/core/rejection"

	"xxx/pkg/log"
	"xxx/pkg/nio"
	niomocks "xxx/pkg/nio/mocks"
	slicepool "xxx/pkg/pool/slice"
	"xxx/pkg/reject"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func unitServer(
	t *testing.T,
	ctrl *gomock.Controller,
	cfg Config,
) (*Server, *mocks.MockConnectionActivator) {
	t.Helper()

	logger, closeLogger, loggerErr := log.NewLogger(nil)
	require.NoError(t, loggerErr)
	t.Cleanup(func() { _ = closeLogger() })

	cfg.Load(nil)

	activator := mocks.NewMockConnectionActivator(ctrl)
	rejections := reject.New[rejection.Reason](
		logger,
		0,
		rejection.Names(),
	)
	server := NewServer(
		t.Context(),
		cfg,
		logger,
		rejections,
		activator,
		nil,
		func() {},
	)
	require.Same(t, rejections, server.rejections)
	return server, activator
}

func unitFrame(t *testing.T, body string) []byte {
	t.Helper()

	frame, err := protocol.EncodeFrame([]byte(body), 4096)
	require.NoError(t, err)
	t.Cleanup(func() {
		clear(frame[:cap(frame)])
		slicepool.Put(frame)
	})
	return frame
}

func TestServerUsesLoadedDefaults(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	server, _ := unitServer(t, ctrl, Config{})
	require.Equal(t, 4096, server.cfg.MaxFrameBytes)
	require.Equal(t, 64, server.cfg.MaxFramesPerTick)
	require.Equal(t, 256*1024, server.cfg.MaxBytesPerTick)
}

func TestServerOnOpenUsesAdmissionResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, tc := range []struct {
		name     string
		admitted bool
		want     nio.Action
	}{
		{name: "admitted", admitted: true, want: nio.None},
		{name: "rejected", admitted: false, want: nio.Close},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				server, activator := unitServer(t, ctrl, Config{})
				conn := niomocks.NewMockConn(ctrl)

				activator.
					EXPECT().
					Admit(server.ctx, conn).
					Return(tc.admitted)

				data, action := server.OnOpen(conn)
				require.Nil(t, data)
				require.Equal(t, tc.want, action)
			},
		)
	}
}

func TestServerOnCloseDelegatesCleanup(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "normal"},
		{name: "EOF", err: io.EOF},
		{name: "peer reset", err: syscall.ECONNRESET},
		{name: "broken pipe", err: syscall.EPIPE},
		{name: "socket error", err: errors.New("socket failed")},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				server, activator := unitServer(t, ctrl, Config{})
				conn := niomocks.NewMockConn(ctrl)

				activator.
					EXPECT().
					Deactivate(server.ctx, conn).
					Return(nio.None)

				require.Equal(t, nio.None, server.OnClose(conn, tc.err))
				if errors.Is(tc.err, syscall.ECONNRESET) || errors.Is(tc.err, syscall.EPIPE) {
					require.Equal(t, uint64(1), server.rejections.Count(rejection.ConnectionClosed))
				} else {
					require.Zero(t, server.rejections.Count(rejection.ConnectionClosed))
				}
			},
		)
	}
}

func TestServerOnTickDelegates(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	server, activator := unitServer(t, ctrl, Config{})
	activator.
		EXPECT().
		Tick().
		Return(time.Second, nio.None)

	delay, action := server.OnTick()
	require.Equal(t, time.Second, delay)
	require.Equal(t, nio.None, action)
}

func TestServerOnShutdownRunsCleanup(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	server, _ := unitServer(t, ctrl, Config{})
	calls := 0
	server.cleanup = func() { calls++ }
	server.OnShutdown(nio.Engine{})
	require.Equal(t, 1, calls)
}

func TestServerBuffersPartialFrameBeforeDispatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	frame := unitFrame(t, "opaque handshake")
	for split := range frame {
		t.Run(
			strconv.Itoa(split),
			func(t *testing.T) {
				server, activator := unitServer(t, ctrl, Config{})
				conn, input := bufferedConn(t, ctrl, nil)
				*input = bytes.Clone(frame[:split])
				// No activator call is allowed until the complete frame arrives.
				require.Equal(t, nio.None, server.OnTraffic(conn))
				require.Equal(t, split, len(*input))

				activator.
					EXPECT().
					HandleFrame(server.ctx, conn, frame).
					Return(nio.None)

				*input = append(*input, frame[split:]...)
				require.Equal(t, nio.None, server.OnTraffic(conn))
				require.Empty(t, *input)
			},
		)
	}
}

func TestServerRejectsInvalidFrameLength(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, size := range []int{0, 4097} {
		name := "empty"
		if size != 0 {
			name = "oversized"
		}
		t.Run(
			name,
			func(t *testing.T) {
				server, _ := unitServer(t, ctrl, Config{})
				conn, input := bufferedConn(t, ctrl, nil)
				*input = binary.LittleEndian.AppendUint16(nil, uint16(size))
				require.Equal(t, nio.Close, server.OnTraffic(conn))

				reason := rejection.FrameTooSmall
				if size != 0 {
					reason = rejection.FrameTooLarge
				}
				require.Equal(t, uint64(1), server.rejections.Count(reason))
			},
		)
	}
}

func TestServerYieldsAtProcessingBudget(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	frame := unitFrame(t, "frame")
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{name: "frame budget", cfg: Config{MaxFramesPerTick: 2}},
		// The byte budget includes the prefix and allows finishing the last frame.
		{name: "byte budget", cfg: Config{MaxBytesPerTick: 2*len(frame) - 1}},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				server, activator := unitServer(t, ctrl, tc.cfg)
				conn, input := bufferedConn(t, ctrl, nil)
				*input = bytes.Repeat(frame, 3)

				conn.EXPECT().
					Wake(gomock.Any()).
					Return(nil).
					Times(1)

				activator.
					EXPECT().
					HandleFrame(server.ctx, conn, frame).
					Return(nio.None).
					Times(2)

				require.Equal(t, nio.None, server.OnTraffic(conn))
				require.Equal(t, frame, *input)

				activator.
					EXPECT().
					HandleFrame(server.ctx, conn, frame).
					Return(nio.None)

				require.Equal(t, nio.None, server.OnTraffic(conn))
				require.Empty(t, *input)
			},
		)
	}
}

func TestServerClosesWhenWakeFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	server, activator := unitServer(t, ctrl, Config{MaxFramesPerTick: 1})
	conn, input := bufferedConn(t, ctrl, nil)
	frame := unitFrame(t, "frame")
	*input = bytes.Repeat(frame, 2)

	conn.EXPECT().
		Wake(gomock.Any()).
		Return(errors.New("event loop stopped"))

	activator.
		EXPECT().
		HandleFrame(server.ctx, conn, frame).
		Return(nio.None)

	require.Equal(t, nio.Close, server.OnTraffic(conn))
	require.Equal(t, frame, *input)
}

func TestServerStopsDrainingWhenActivatorCloses(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	server, activator := unitServer(t, ctrl, Config{})
	conn, input := bufferedConn(t, ctrl, nil)
	frame := unitFrame(t, "frame")
	*input = bytes.Repeat(frame, 2)

	activator.
		EXPECT().
		HandleFrame(server.ctx, conn, frame).
		Return(nio.Close)

	require.Equal(t, nio.Close, server.OnTraffic(conn))
	require.Equal(t, frame, *input)
}

func TestServerRejectsInactiveSessionBeforeDecrypting(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	server, _ := unitServer(t, ctrl, Config{})
	conn, input := bufferedConn(t, ctrl, &connection.Session{})
	*input = unitFrame(t, "frame")
	require.Equal(t, nio.Close, server.OnTraffic(conn))
}

// bufferedConn wires NIO reader methods to a test-owned input slice. The
// connection itself remains a generated mock; unexpected operations fail.
func bufferedConn(
	t *testing.T,
	ctrl *gomock.Controller,
	ctx any,
) (*niomocks.MockConn, *[]byte) {
	t.Helper()

	var input []byte
	conn := niomocks.NewMockConn(ctrl)

	conn.EXPECT().
		Context().
		Return(ctx).
		AnyTimes()

	conn.EXPECT().
		InboundBuffered().
		DoAndReturn(func() int { return len(input) }).
		AnyTimes()

	conn.EXPECT().
		Peek(gomock.Any()).
		DoAndReturn(
			func(n int) ([]byte, error) {
				if n > len(input) {
					return nil, io.ErrShortBuffer
				}
				return input[:n], nil
			},
		).
		AnyTimes()

	conn.EXPECT().
		Next(gomock.Any()).
		DoAndReturn(
			func(n int) ([]byte, error) {
				if n > len(input) {
					return nil, io.ErrShortBuffer
				}
				b := input[:n]
				input = input[n:]
				return b, nil
			},
		).
		AnyTimes()

	return conn, &input
}
