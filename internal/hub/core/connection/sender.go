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
	pbpub "xxx/api/hub/v1/proto/gen/pub"
	"xxx/internal/hub/core/protocol"
	shareddevice "xxx/internal/shared/device"

	"xxx/pkg/log"
	"xxx/pkg/nio"
	slicepool "xxx/pkg/pool/slice"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type SendData struct {
	MsgType pbpub.MsgType
	Msg     proto.Message
	Expand  []byte
}

// connectionSender encodes frames and serializes their sequence allocation
// with nio.AsyncWrite enqueue order.
type connectionSender struct {
	registry *connectionRegistry
	encoder  *protocol.Encoder
	logger   *log.Logger
}

func newConnectionSender(
	registry *connectionRegistry,
	encoder *protocol.Encoder,
	logger *log.Logger,
) *connectionSender {
	return &connectionSender{registry: registry, encoder: encoder, logger: logger}
}

func (s *connectionSender) AsyncSend(secure bool, deviceID shareddevice.ID, data SendData) error {
	conn, exists := s.registry.Lookup(deviceID)
	if !exists {
		return ErrSessionNotFound
	}
	return s.AsyncSendConn(secure, conn, data)
}

// AsyncSendConn is tied to one activation so a delayed callback cannot send
// to a replacement connection for the same device.
func (s *connectionSender) AsyncSendConn(secure bool, conn nio.Conn, data SendData) error {
	session, ok := conn.Context().(*Session)
	if !ok || session.isClosed() {
		return ErrSessionClosed
	}

	current, exists := s.registry.Lookup(session.deviceID)
	if !exists || current != conn {
		return ErrSessionClosed
	}

	var packet []byte
	err := session.WithSendSeq(func(seq uint64) (bool, error) {
		var err error
		if secure {
			packet, err = s.encoder.EncodeS(
				&session.keys.SendEnc,
				seq,
				data.MsgType,
				data.Msg,
				data.Expand,
			)
		} else {
			packet, err = s.encoder.Encode(
				&session.keys.SendMac,
				seq,
				data.MsgType,
				data.Msg,
				data.Expand,
			)
		}
		if err != nil {
			return false, err
		}

		err = conn.AsyncWrite(packet, func(_ nio.Conn, writeErr error) error {
			clear(packet[:cap(packet)])
			slicepool.Put(packet)
			if writeErr != nil {
				session.MarkSendFailed()
				s.logger.Error("failed to write session frame", zap.Error(writeErr))
				forceCloseConnection(s.logger, conn)
			}
			return nil
		})
		return true, err
	})
	if err != nil {
		if session.SendFailed() {
			forceCloseConnection(s.logger, conn)
		}
		return err
	}
	return nil
}
