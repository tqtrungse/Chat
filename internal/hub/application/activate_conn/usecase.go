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

package activate_conn

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"time"

	pbpub "xxx/api/hub/v1/proto/gen/pub"
	"xxx/internal/hub/connection"
	"xxx/internal/hub/protocol"
	shareddevice "xxx/internal/shared/device"
	sharedsession "xxx/internal/shared/session"

	"xxx/pkg/log"
	"xxx/pkg/nio"
	slicepool "xxx/pkg/pool/slice"

	"go.uber.org/zap"
)

const activationChallengeTTL = 5 * time.Second

type Usecase interface {
	Activate(conn nio.Conn, packet []byte) nio.Action
	Prove(ctx context.Context, conn nio.Conn, packet []byte) nio.Action
	Deactivate(ctx context.Context, conn nio.Conn) nio.Action
	Tick() (time.Duration, nio.Action)
}

type connActivator struct {
	logger  *log.Logger
	encoder *protocol.Encoder
	decoder *protocol.Decoder
	router  *connection.Router
}

type pendingActivation struct {
	token         [8]byte
	ticket        []byte
	sign          []byte
	activationKey [32]byte
	transcript    [32]byte
	nonce         [32]byte
	deadline      time.Time
	timer         *time.Timer
}

func New(
	logger *log.Logger,
	encoder *protocol.Encoder,
	decoder *protocol.Decoder,
	router *connection.Router,
) Usecase {
	return &connActivator{
		logger:  logger,
		encoder: encoder,
		decoder: decoder,
		router:  router,
	}
}

// Activate validates the ticket and identity signature, then issues a fresh
// challenge bound to this TCP connection. The connection is not activated yet.
func (ca *connActivator) Activate(conn nio.Conn, packet []byte) nio.Action {
	if conn.Context() != nil {
		return nio.Close
	}

	var req pbpub.ActiveConnReq
	if err := ca.decoder.DecodeActivePack(packet[4:], &req); err != nil {
		ca.logger.Error("failed to decode active request, force close", zap.Error(err))
		return nio.Close
	}
	defer func() {
		clear(req.Token)
		clear(req.Sign)
		clear(req.Ticket)
		slicepool.Put(req.Token)
		slicepool.Put(req.Sign)
		slicepool.Put(req.Ticket)
	}()

	if err := req.Validate(); err != nil {
		ca.logger.Error("failed to validate activate request, force close", zap.Error(err))
		return nio.Close
	}

	ticket, err := ca.router.PrepareActivation(req.Token, req.Ticket, req.Sign)
	if err != nil {
		ca.logger.Error("failed to prepare activation, force close", zap.Error(err))
		return nio.Close
	}
	defer ticket.Clear()

	pending := &pendingActivation{
		ticket:        bytes.Clone(req.Ticket),
		sign:          bytes.Clone(req.Sign),
		activationKey: ticket.ActivationKey,
		transcript:    sharedsession.ActivationTranscript(req.Token, req.Ticket, req.Sign),
		deadline:      time.Now().Add(activationChallengeTTL),
	}
	copy(pending.token[:], req.Token)
	if _, err = rand.Read(pending.nonce[:]); err != nil {
		pending.clear()
		ca.logger.Error("failed to generate activation challenge, force close", zap.Error(err))
		return nio.Close
	}

	// Close idle pre-auth connections so their temporary activation key is not
	// retained indefinitely. OnClose -> Deactivate clears the pending state.
	pending.timer = time.AfterFunc(activationChallengeTTL, func() {
		_ = conn.Close()
	})
	conn.SetContext(pending)

	frame, err := ca.encoder.EncodeHandshake(
		pbpub.PacketType_RESP_ACTIVE_CONN_CHALLENGE,
		&pbpub.ActiveConnChallengeResp{Nonce: pending.nonce[:]},
	)
	if err == nil {
		defer func() {
			clear(frame)
			slicepool.Put(frame)
		}()
		_, err = conn.Write(frame)
	}
	if err != nil {
		pending.clear()
		conn.SetContext(nil)
		ca.logger.Error("failed to send activation challenge, force close", zap.Error(err))
		return nio.Close
	}
	return nio.None
}

// Prove verifies possession of the ECDH-derived activation key. The nonce is
// accepted only for the pending state on the connection that issued it.
func (ca *connActivator) Prove(ctx context.Context, conn nio.Conn, packet []byte) nio.Action {
	var req pbpub.ActiveConnProofReq
	if err := ca.decoder.DecodeActiveProofPack(packet[4:], &req); err != nil {
		ca.logger.Error("failed to decode activation proof, force close", zap.Error(err))
		return ca.rejectPending(conn, conn.Context())
	}
	defer func() {
		clear(req.Proof)
		slicepool.Put(req.Proof)
	}()

	if err := req.Validate(); err != nil {
		ca.logger.Error("invalid activation proof, force close", zap.Error(err))
		return ca.rejectPending(conn, conn.Context())
	}

	pending, ok := conn.Context().(*pendingActivation)
	if !ok || time.Now().After(pending.deadline) {
		return ca.rejectPending(conn, conn.Context())
	}
	want := sharedsession.ActivationProof(
		&pending.activationKey,
		pending.transcript,
		pending.nonce,
	)
	if !hmac.Equal(want[:], req.Proof) {
		return ca.rejectPending(conn, pending)
	}
	if pending.timer == nil || !pending.timer.Stop() {
		return ca.rejectPending(conn, pending)
	}

	err := ca.router.ActivateConn(
		ctx,
		pending.token[:],
		pending.ticket,
		pending.sign,
		conn,
		ca.onActivated(conn),
	)
	pending.clear()
	if conn.Context() == pending {
		conn.SetContext(nil)
	}
	if err != nil {
		ca.logger.Error("failed to activate connection, force close", zap.Error(err))
		return nio.Close
	}
	return nio.None
}

func (ca *connActivator) onActivated(conn nio.Conn) func(id shareddevice.ID) {
	return func(deviceID shareddevice.ID) {
		err := ca.router.Send(
			deviceID,
			connection.SendData{
				PackType: pbpub.PacketType_RESP_ACTIVE_CONN,
				Msg:      &pbpub.ActiveConnResp{Code: pbpub.Code_SUCCESS},
			},
		)
		if err != nil {
			ca.logger.Error("failed to respond, force close", zap.Error(err))
			ca.router.ForceClose(conn)
		}
	}
}

func (ca *connActivator) rejectPending(conn nio.Conn, state any) nio.Action {
	if pending, ok := state.(*pendingActivation); ok {
		pending.clear()
		if conn.Context() == pending {
			conn.SetContext(nil)
		}
	}
	return nio.Close
}

func (p *pendingActivation) clear() {
	if p == nil {
		return
	}
	if p.timer != nil {
		p.timer.Stop()
	}
	clear(p.token[:])
	clear(p.ticket)
	clear(p.sign)
	clear(p.activationKey[:])
	clear(p.transcript[:])
	clear(p.nonce[:])
}

func (ca *connActivator) Deactivate(ctx context.Context, conn nio.Conn) nio.Action {
	if pending, ok := conn.Context().(*pendingActivation); ok {
		pending.clear()
		conn.SetContext(nil)
	}
	ca.router.RemoveConn(ctx, conn)
	return nio.None
}

func (ca *connActivator) Tick() (time.Duration, nio.Action) {
	return ca.router.Tick(), nio.None
}
