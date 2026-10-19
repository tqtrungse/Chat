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

package activateconn

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"sync/atomic"
	"time"

	pbpub "xxx/api/hub/v1/proto/gen/pub"
	"xxx/internal/hub/core/activation"
	"xxx/internal/hub/core/connection"
	"xxx/internal/hub/core/protocol"
	shareddevice "xxx/internal/shared/device"

	"xxx/pkg/log"
	"xxx/pkg/nio"
	slicepool "xxx/pkg/pool/slice"

	"go.uber.org/zap"
)

const (
	activationChallengeTTL = 5 * time.Second // challenge issued -> proof received
	defaultPreAuthTimeout  = 10 * time.Second
)

const (
	stageConnected  uint8 = iota // accepted, nothing valid received yet
	stageChallenged              // challenge sent, waiting for proof
)

type Usecase interface {
	// Admit is called from OnOpen. false means: close the connection.
	Admit(conn nio.Conn) bool
	Activate(conn nio.Conn, packet []byte) nio.Action
	Prove(ctx context.Context, conn nio.Conn, packet []byte) nio.Action
	Deactivate(ctx context.Context, conn nio.Conn) nio.Action
	Tick() (time.Duration, nio.Action)
}

type connActivator struct {
	logger         *log.Logger
	encoder        *protocol.Encoder
	decoder        *protocol.Decoder
	manager        ConnManager
	activationSvc  ActivationService
	preAuthTimeout time.Duration
	maxPreAuth     int64
	preAuth        atomic.Int64
}

func New(
	cfg Config,
	logger *log.Logger,
	encoder *protocol.Encoder,
	decoder *protocol.Decoder,
	manager ConnManager,
	activationSvc ActivationService,
) Usecase {
	ca := &connActivator{
		logger:         logger,
		encoder:        encoder,
		decoder:        decoder,
		manager:        manager,
		activationSvc:  activationSvc,
		preAuthTimeout: cfg.PreAuthTimeout,
		maxPreAuth:     int64(cfg.MaxPreAuthConns),
	}
	if ca.preAuthTimeout <= 0 {
		ca.preAuthTimeout = defaultPreAuthTimeout
	}
	if ca.maxPreAuth <= 0 {
		ca.maxPreAuth = max(int64(manager.MaxConns()/10), 1)
	}
	return ca
}

// Admit reserves a pre-auth slot and arms the deadline. Don't log per rejection:
// under attack that becomes a log storm; export PreAuthConns() as a metric instead.
func (ca *connActivator) Admit(conn nio.Conn) bool {
	if ca.manager.CurConns() > ca.manager.MaxConns() {
		return false
	}

	if ca.preAuth.Add(1) > ca.maxPreAuth {
		ca.preAuth.Add(-1)
		return false
	}

	p := &pendingActivation{
		owner: ca,
		stage: stageConnected,
	}
	p.timer = time.AfterFunc(ca.preAuthTimeout, func() { _ = conn.Close() })
	conn.SetContext(p)
	return true
}

func (ca *connActivator) PreAuthConns() int64 { return ca.preAuth.Load() }

// Activate validates the ticket and identity signature, then issues a fresh
// challenge bound to this TCP connection. The connection is not activated yet.
func (ca *connActivator) Activate(conn nio.Conn, packet []byte) nio.Action {
	pending, ok := conn.Context().(*pendingActivation)
	if !ok || pending.stage != stageConnected {
		return ca.reject(pending) // already activated, or a second ActiveConnReq
	}

	var req pbpub.ActiveConnReq
	err := ca.decoder.DecodeActivationPacket(
		packet[4:],
		&req,
	)
	if err != nil {
		ca.logger.Error("failed to decode active request, force close", zap.Error(err))
		return ca.reject(pending)
	}
	defer func() {
		clear(req.Token)
		clear(req.Sign)
		clear(req.Ticket)
		slicepool.Put(req.Token)
		slicepool.Put(req.Sign)
		slicepool.Put(req.Ticket)
	}()

	if err = req.Validate(); err != nil {
		ca.logger.Error("failed to validate activate request, force close", zap.Error(err))
		return ca.reject(pending)
	}

	// The only place the ticket is opened and the signatures are verified.
	ticket, err := ca.activationSvc.Prepare(
		req.Token,
		req.Ticket,
		req.Sign,
	)
	if err != nil {
		ca.logger.Error("failed to prepare activation, force close", zap.Error(err))
		return ca.reject(pending)
	}

	pending.ticket = ticket // owned by pending from here; zeroed by pending.clear()
	pending.transcript = activation.Transcript(
		req.Token,
		req.Ticket,
		req.Sign,
	)
	pending.deadline = time.Now().Add(activationChallengeTTL)
	_, err = rand.Read(pending.nonce[:])
	if err != nil {
		ca.logger.Error("failed to generate activation challenge, force close", zap.Error(err))
		return ca.reject(pending)
	}
	pending.stage = stageChallenged

	frame, err := ca.encoder.EncodeHandshake(
		pbpub.MsgType_RESP_ACTIVE_CONN,
		&pbpub.ActiveConnResp{Nonce: pending.nonce[:]},
	)
	if err == nil {
		defer func() {
			clear(frame)
			slicepool.Put(frame)
		}()
		_, err = conn.Write(frame)
	}
	if err != nil {
		ca.logger.Error("failed to send activation challenge, force close", zap.Error(err))
		return ca.reject(pending)
	}
	return nio.None
}

// Prove verifies possession of the ECDH-derived activation key. The nonce is
// accepted only for the pending state on the connection that issued it.
func (ca *connActivator) Prove(ctx context.Context, conn nio.Conn, packet []byte) nio.Action {
	pending, ok := conn.Context().(*pendingActivation)
	if !ok || pending.stage != stageChallenged || time.Now().After(pending.deadline) {
		return ca.reject(pending)
	}

	if err := ca.activationSvc.CheckExpiry(pending.ticket, time.Now()); err != nil {
		ca.logger.Error("ticket expired before activation, force close", zap.Error(err))
		return ca.reject(pending)
	}

	var req pbpub.ActiveConnProofReq
	err := ca.decoder.DecodeActivationProofPack(packet[4:], &req)
	if err != nil {
		ca.logger.Error("failed to decode activation proof, force close", zap.Error(err))
		return ca.reject(pending)
	}
	defer func() {
		clear(req.Proof)
		slicepool.Put(req.Proof)
	}()

	err = req.Validate()
	if err != nil {
		ca.logger.Error("invalid activation proof, force close", zap.Error(err))
		return ca.reject(pending)
	}

	want := activation.Proof(
		&pending.ticket.ActivationKey,
		pending.transcript,
		pending.nonce,
	)
	if !hmac.Equal(want[:], req.Proof) {
		return ca.reject(pending)
	}
	// Stop() == false: the pre-auth timer already fired and the conn is closing.
	if !pending.timer.Stop() {
		return ca.reject(pending)
	}

	connKeys, err := activation.DeriveConnKeys(
		&pending.ticket.Keys,
		pending.nonce,
	)
	if err != nil {
		ca.logger.Error("failed to derive connection keys, force close", zap.Error(err))
		return ca.reject(pending)
	}
	defer connKeys.Clear()

	// ActivateConn copies the session keys by value and replaces the conn context
	// with the session, so pending can be wiped right after.
	err = ca.manager.ActivateConn(
		ctx,
		pending.ticket.DeviceID,
		&connKeys,
		conn,
		ca.onAck(conn),
	)
	pending.clear()

	if err != nil {
		ca.logger.Error("failed to activate connection, force close", zap.Error(err))
		return nio.Close
	}
	return nio.None
}

func (ca *connActivator) Deactivate(ctx context.Context, conn nio.Conn) nio.Action {
	if pending, ok := conn.Context().(*pendingActivation); ok {
		pending.clear()
		conn.SetContext(nil)
	}
	ca.manager.RemoveConn(ctx, conn)
	return nio.None
}

func (ca *connActivator) Tick() (time.Duration, nio.Action) {
	return ca.manager.Tick(), nio.None
}

func (ca *connActivator) onAck(conn nio.Conn) func(id shareddevice.ID) {
	return func(_ shareddevice.ID) {
		err := ca.manager.AsyncSendConn(
			false,
			conn,
			connection.SendData{
				MsgType: pbpub.MsgType_RESP_ACTIVE_CONN_PROOF,
				Msg:     &pbpub.ActiveConnProofResp{Code: pbpub.Code_SUCCESS},
			},
		)
		if err != nil {
			ca.logger.Error("failed to respond, force close", zap.Error(err))
			ca.manager.ForceClose(conn)
		}
	}
}

// reject wipes the pending state (nil-safe) and closes. The context stays set so
// OnClose -> Deactivate finds it; clear() is idempotent.
func (ca *connActivator) reject(p *pendingActivation) nio.Action {
	p.clear()
	return nio.Close
}
