/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connection

import (
	"context"
	"io"

	pbpub "xxx/api/hub/v2/proto/gen/pub"
	"xxx/internal/hub/core/protocol"
	"xxx/internal/shared/device"

	"xxx/pkg/log"
	"xxx/pkg/nio"
	slicepool "xxx/pkg/pool/slice"

	"google.golang.org/protobuf/proto"
)

type SendData struct {
	MsgKind pbpub.MsgKind
	Msg     proto.Message
	Expand  []byte
}
type connectionSender struct {
	registry *connectionRegistry
	encoder  *protocol.Encoder
	logger   *log.Logger
}

// newConnectionSender connects registry lookup, payload encoding, and loop-owned writes.
func newConnectionSender(
	registry *connectionRegistry,
	encoder *protocol.Encoder,
	logger *log.Logger,
) *connectionSender {
	return &connectionSender{
		registry: registry,
		encoder:  encoder,
		logger:   logger,
	}
}

// AsyncSend resolves the device connection and queues a send, or reports that no session exists.
func (s *connectionSender) AsyncSend(ctx context.Context, id device.ID, data SendData) error {
	conn, ok := s.registry.Lookup(id)
	if !ok {
		return ErrSessionNotFound
	}
	return s.AsyncSendConn(ctx, conn, data)
}

// AsyncSendConn encodes the message and queues encryption and writing on the owning loop.
//
// Success means that the event loop accepted the task, not that the socket
// write succeeded. Encoding here takes ownership before caller buffers change.
func (s *connectionSender) AsyncSendConn(ctx context.Context, conn nio.Conn, data SendData) error {
	session, ok := conn.Context().(*Session)
	if !ok || !session.Active() {
		return ErrSessionClosed
	}

	payload, err := s.encoder.EncodePayload(data.MsgKind, data.Msg, data.Expand)
	if err != nil {
		return err
	}

	err = conn.EventLoop().Execute(ctx, func(context.Context) error {
		defer func() {
			clear(payload[:cap(payload)])
			slicepool.Put(payload)
		}()

		current, existence := s.registry.Lookup(session.deviceID)
		if !existence ||
			current != conn ||
			conn.Context() != session ||
			!session.Active() {
			return nil
		}

		writeErr := writeRecord(conn, session, payload)
		if writeErr != nil {
			forceCloseConnection(s.logger, conn)
		}
		return nil
	})
	if err != nil {
		clear(payload[:cap(payload)])
		slicepool.Put(payload)
	}
	return err
}

// writeRecord encrypts and writes one payload, releasing the temporary frame afterward.
// Only the owning event loop may call it. A failure marks the session unusable
// because the Noise send nonce may already have advanced; the caller must close it.
func writeRecord(conn nio.Conn, s *Session, payload []byte) error {
	if s.channel == nil ||
		s.isClosed() ||
		s.failed.Load() {
		return ErrSessionClosed
	}

	frame, err := s.channel.EncryptRecord(payload)
	if err != nil {
		s.failed.Store(true)
		return err
	}

	defer func() {
		clear(frame[:cap(frame)])
		slicepool.Put(frame)
	}()

	n, err := conn.Write(frame)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}

	if err != nil {
		s.failed.Store(true)
	}
	return err
}
