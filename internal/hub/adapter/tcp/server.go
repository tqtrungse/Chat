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

package tcp

import (
	"context"
	"encoding/binary"
	"runtime"
	"time"

	"xxx/api/hub/v1/proto/gen/pub"
	"xxx/internal/hub/application/accept_conn"
	"xxx/internal/hub/application/activate_conn"
	"xxx/internal/hub/application/send_message"

	"xxx/pkg/log"
	"xxx/pkg/nio"

	"go.uber.org/zap"
)

type Server struct {
	nio.BuiltinEventEngine
	eng       nio.Engine
	ctx       context.Context
	cfg       Config
	logger    *log.Logger
	acceptor  accept_conn.Usecase
	activator activate_conn.Usecase
	sender    send_message.Usecase
	cleanup   func()
}

func NewServer(
	ctx context.Context,
	cfg Config,
	logger *log.Logger,
	connAcceptor accept_conn.Usecase,
	connActivator activate_conn.Usecase,
	msgSender send_message.Usecase,
	cleanup func(),
) *Server {
	return &Server{
		ctx:       ctx,
		cfg:       cfg,
		logger:    logger,
		acceptor:  connAcceptor,
		activator: connActivator,
		sender:    msgSender,
		cleanup:   cleanup,
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
		nio.WithSocketRecvBuffer(int(s.cfg.ReadBufferCap)),

		// Limits for read buffer in user-space (on RAM).
		nio.WithReadBufferCap(int(s.cfg.ReadBufferCap)),
		nio.WithWriteBufferCap(int(s.cfg.WriteBufferCap)),

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
	ctx, cancel := context.WithTimeout(s.ctx, 10*time.Second)
	defer cancel()
	return s.eng.Stop(ctx)
}

func (s *Server) OnBoot(eng nio.Engine) nio.Action {
	s.eng = eng
	return nio.None
}

func (s *Server) OnShutdown(_ nio.Engine) {
	s.cleanup()
}

func (s *Server) OnOpen(conn nio.Conn) ([]byte, nio.Action) {
	if s.acceptor.Accept(conn) {
		return nil, nio.None
	}
	return nil, nio.Close
}

func (s *Server) OnClose(conn nio.Conn, err error) nio.Action {
	if err != nil {
		s.logger.Warn("connection closed", zap.Error(err))
	}
	return s.activator.Deactivate(s.ctx, conn)
}

func (s *Server) OnTraffic(conn nio.Conn) nio.Action {
	header, err := conn.Peek(2)
	if err != nil {
		return nio.None
	}

	size := binary.LittleEndian.Uint16(header)
	if size < 2 {
		// The packet body must at least contain packet_type.
		return nio.Close
	}

	frameBytes := int(size) + 2
	if frameBytes > int(s.cfg.ReadBufferCap) {
		return nio.Close
	}
	if conn.InboundBuffered() < frameBytes {
		return nio.None
	}

	// Error can return is io.ErrShortBuffer when needed bytes is larger than connection buffer.
	// That will never happen because we checked it.
	packet, err := conn.Next(frameBytes)
	if err != nil || len(packet) != frameBytes {
		return nio.Close
	}

	pkgType := binary.LittleEndian.Uint16(packet[2:4])

	switch pub.PacketType(pkgType) {
	case pub.PacketType_REQ_ACTIVE_CONN:
		return s.activator.Activate(conn, packet)

	case pub.PacketType_REQ_ACTIVE_CONN_PROOF:
		return s.activator.Prove(s.ctx, conn, packet)

	case pub.PacketType_REQ_SEND_MSG:
		return s.sender.Send(conn, packet)

	case pub.PacketType_REQ_ACK_RECV_MSG:
		return nio.None

	default:
		s.logger.Error("invalid packet type, force close", zap.Uint16("type", pkgType))
		return nio.Close
	}
}

func (s *Server) OnTick() (delay time.Duration, action nio.Action) {
	return s.activator.Tick()
}
