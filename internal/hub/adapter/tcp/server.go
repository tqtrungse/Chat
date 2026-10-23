/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package tcp

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"runtime"
	"syscall"
	"time"

	"xxx/api/hub/v2/proto/gen/pub"
	"xxx/internal/hub/core/connection"
	"xxx/internal/hub/core/protocol"
	"xxx/internal/hub/core/rejection"

	"xxx/pkg/log"
	"xxx/pkg/nio"
	slicepool "xxx/pkg/pool/slice"
	"xxx/pkg/reject"

	"go.uber.org/zap"
)

const frameSizeBytes = 2

type Server struct {
	nio.BuiltinEventEngine
	eng        nio.Engine
	ctx        context.Context
	ready      chan struct{}
	addr       string
	cfg        Config
	logger     *log.Logger
	rejections *reject.Stats[rejection.Reason]
	decoder    *protocol.Decoder
	activator  ConnectionActivator
	sender     MessageSender
	cleanup    func()
}

func NewServer(
	ctx context.Context,
	cfg Config,
	logger *log.Logger,
	rejections *reject.Stats[rejection.Reason],
	connActivator ConnectionActivator,
	msgSender MessageSender,
	cleanup func(),
) *Server {
	return &Server{
		ctx:        ctx,
		ready:      make(chan struct{}),
		cfg:        cfg,
		logger:     logger,
		rejections: rejections,
		decoder:    protocol.NewDecoder(),
		activator:  connActivator,
		sender:     msgSender,
		cleanup:    cleanup,
	}
}

func (s *Server) Run() error {
	return nio.Run(
		s,
		s.cfg.Addr,

		nio.WithLogger(s.logger),

		// In container/Kubernetes environment, runtime.NumCPU() can return number CPUs of node,
		// not actually CPU limit of container.
		nio.WithNumEventLoop(runtime.GOMAXPROCS(0)),

		// Helps event-loop to actually parallel on multi-cores.
		nio.WithLockOSThread(true),
		nio.WithLoadBalancing(nio.LeastConnections),

		// Limits for read buffer in kernel-space (on RAM).
		nio.WithSocketRecvBuffer(s.cfg.ReadBufferCap),

		// Limits for read buffer in user-space (on RAM).
		nio.WithReadBufferCap(s.cfg.ReadBufferCap),
		nio.WithWriteBufferCap(s.cfg.WriteBufferCap),

		// Data is immediately sent after writing.
		nio.WithTCPNoDelay(nio.TCPNoDelay),
		nio.WithTCPKeepAlive(s.cfg.TCPKeepAlive),
		nio.WithTCPKeepInterval(s.cfg.TCPKeepInterval),
		nio.WithTCPKeepCount(s.cfg.TCPKeepCount),

		// When a TCP connection closes, the server side doesn't close immediately
		// but enters a TIME_WAIT state for 2 × MSL (usually 60-120 seconds on Linux).
		// After restarting, allow the new process to bind immediately service
		// although that service still got connections with TIME_WAIT state.
		nio.WithReuseAddr(true),

		nio.WithTicker(true),
	)
}

func (s *Server) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), 10*time.Second)
	defer cancel()

	select {
	case <-s.ready:
	case <-ctx.Done():
		return ctx.Err()
	}

	return s.eng.Stop(ctx)
}

// Ready closes after OnBoot has published the engine for Shutdown.
// Startup failures are reported by Run instead.
func (s *Server) Ready() <-chan struct{} {
	return s.ready
}

// Addr returns the bound listener address after Ready closes, or an empty
// string before readiness. An OS-assigned port replaces a configured port 0.
func (s *Server) Addr() string {
	select {
	case <-s.ready:
		return s.addr
	default:
		return ""
	}
}

func (s *Server) OnBoot(eng nio.Engine) nio.Action {
	s.eng = eng

	addrs := eng.ListenerAddresses()
	if len(addrs) != 0 {
		s.addr = addrs[0]
	}
	close(s.ready)

	return nio.None
}

func (s *Server) OnShutdown(_ nio.Engine) {
	s.cleanup()
}

func (s *Server) OnOpen(conn nio.Conn) ([]byte, nio.Action) {
	if s.activator.Admit(s.ctx, conn) {
		return nil, nio.None
	}
	return nil, nio.Close
}

func (s *Server) OnClose(conn nio.Conn, err error) nio.Action {
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
		var netErr net.Error
		if errors.Is(err, syscall.ECONNRESET) ||
			errors.Is(err, syscall.EPIPE) ||
			errors.Is(err, syscall.ETIMEDOUT) ||
			(errors.As(err, &netErr) && netErr.Timeout()) {
			s.rejections.Record(rejection.ConnectionClosed, err)
		} else {
			s.logger.Warn("connection closed", zap.Error(err))
		}
	}
	return s.activator.Deactivate(s.ctx, conn)
}

// OnTraffic
//
// Drain complete frames without invoking Noise on partial input. Wake resumes
// user-space buffered data after yielding the event loop's fairness budget.
func (s *Server) OnTraffic(conn nio.Conn) nio.Action {
	bytesRead := 0
	for frames := 0; ; frames++ {
		if conn.InboundBuffered() < frameSizeBytes {
			return nio.None
		}

		if frames >= s.cfg.MaxFramesPerTick ||
			bytesRead >= s.cfg.MaxBytesPerTick {
			if err := conn.Wake(nil); err != nil {
				return nio.Close
			}
			return nio.None
		}

		header, err := conn.Peek(frameSizeBytes)
		if err != nil {
			return nio.Close
		}

		n := int(binary.LittleEndian.Uint16(header))
		if n == 0 {
			s.rejections.Record(rejection.FrameTooSmall, nil)
			return nio.Close
		}

		if n > s.cfg.MaxFrameBytes {
			s.rejections.Record(rejection.FrameTooLarge, nil)
			return nio.Close
		}

		if conn.InboundBuffered() < n+frameSizeBytes {
			return nio.None
		}

		frame, err := conn.Next(n + frameSizeBytes)
		if err != nil || len(frame) != n+frameSizeBytes {
			return nio.Close
		}

		bytesRead += n + frameSizeBytes
		action := s.dispatch(conn, frame)
		if action != nio.None {
			return action
		}
	}
}

func (s *Server) OnTick() (delay time.Duration, action nio.Action) {
	return s.activator.Tick()
}

func (s *Server) dispatch(conn nio.Conn, frame []byte) nio.Action {
	session, ok := conn.Context().(*connection.Session)
	if !ok {
		return s.activator.HandleFrame(s.ctx, conn, frame)
	}

	if !session.Active() {
		return nio.Close
	}

	plain, err := session.DecryptRecord(frame)
	if err != nil {
		s.rejections.Record(rejection.RecordDecrypt, err)
		return nio.Close
	}
	defer func() {
		clear(plain[:cap(plain)])
		slicepool.Put(plain)
	}()

	payload, err := s.decoder.DecodePayload(plain)
	if err != nil {
		reason := rejection.PayloadDecode
		if errors.Is(err, protocol.ErrMsgKindInvalid) {
			reason = rejection.FrameBadType
		}
		s.rejections.Record(reason, err)
		return nio.Close
	}

	switch payload.Kind {
	case pub.MsgKind_REQ_SEND_MSG:
		req := payload.Message.(*pub.SendMsgReq)

		err = req.Validate()
		if err != nil {
			s.rejections.Record(rejection.PayloadValidate, err)
			return nio.Close
		}

		if s.sender != nil {
			return s.sender.Send(conn, req, payload.Expand)
		}

	case pub.MsgKind_REQ_ACK_RECV_MSG:
		err = payload.Message.(*pub.AckRecvMsgReq).Validate()
		if err != nil {
			s.rejections.Record(rejection.PayloadValidate, err)
			return nio.Close
		}

		if len(payload.Expand) != 0 {
			s.rejections.Record(rejection.PayloadValidate, nil)
			return nio.Close
		}

	default:
		s.rejections.Record(rejection.FrameBadType, nil)
		return nio.Close
	}
	return nio.None
}
