/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package activateconn

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"

	pbpub "xxx/api/hub/v2/proto/gen/pub"
	"xxx/internal/hub/core/auth"
	"xxx/internal/hub/core/connection"
	"xxx/internal/hub/core/protocol"
	"xxx/internal/hub/core/rejection"
	"xxx/internal/hub/core/transport"
	"xxx/internal/shared/hubendpoint"
	"xxx/internal/shared/ticket"

	"xxx/pkg/log"
	"xxx/pkg/nio"
	slicepool "xxx/pkg/pool/slice"
	"xxx/pkg/reject"
)

const (
	activationChallengeTTL = 5 * time.Second
	// [version: 1][hub_key_id: 4 byte][client public key: 32 byte][authentication tag: 16 byte]
	handshakeBodySize = 53

	stageConnected   = uint8(0)
	stageAwaitAuth   = uint8(1)
	stageVerifying   = uint8(2)
	stageRegistering = uint8(3)
	stageAny         = uint8(255)
)

type ConnActivator struct {
	logger           *log.Logger
	rejections       *reject.Stats[rejection.Reason]
	decoder          *protocol.Decoder
	manager          ConnManager
	activationSvc    ActivationService
	verifier         AuthorizationExecutor
	factory          transport.Factory
	preAuthTimeout   time.Duration
	handshakeTimeout time.Duration
	maxPreAuth       int64
	preAuth          atomic.Int64
	maxFrame         int
}

func New(
	cfg Config,
	logger *log.Logger,
	rejections *reject.Stats[rejection.Reason],
	decoder *protocol.Decoder,
	manager ConnManager,
	svc ActivationService,
	factory transport.Factory,
	verifier AuthorizationExecutor,
) *ConnActivator {
	return &ConnActivator{
		logger:           logger,
		rejections:       rejections,
		decoder:          decoder,
		manager:          manager,
		activationSvc:    svc,
		verifier:         verifier,
		factory:          factory,
		preAuthTimeout:   cfg.PreAuthTimeout,
		handshakeTimeout: cfg.HandshakeTimeout,
		maxPreAuth:       int64(cfg.MaxPreAuthConns),
		maxFrame:         cfg.MaxFrameBytes,
	}
}

// Admit runs once per connection, on its owning event loop.
func (ca *ConnActivator) Admit(ctx context.Context, conn nio.Conn) bool {
	if ca.preAuth.Add(1) > ca.maxPreAuth {
		ca.preAuth.Add(-1)
		return false
	}

	now := time.Now()
	pa := &pendingActivation{
		owner:         ca,
		deadline:      now.Add(ca.preAuthTimeout),
		phaseDeadline: now.Add(ca.handshakeTimeout),
		stage:         stageConnected,
	}
	conn.SetContext(pa)
	pa.timer = ca.arm(ctx, conn, pa, ca.preAuthTimeout, stageAny)
	pa.phaseTimer = ca.arm(ctx, conn, pa, ca.handshakeTimeout, stageConnected)
	return true
}

func (ca *ConnActivator) PreAuthConns() int64 {
	return ca.preAuth.Load()
}

func (ca *ConnActivator) HandleFrame(
	ctx context.Context,
	conn nio.Conn,
	frame []byte,
) nio.Action {
	pa, ok := conn.Context().(*pendingActivation)
	if !ok || pa.released.Load() {
		ca.rejections.Record(rejection.ActivationState, nil)
		return ca.reject(pa)
	}

	if time.Now().After(pa.deadline) || time.Now().After(pa.phaseDeadline) {
		ca.rejections.Record(expiryReason(pa.stage), nil)
		return ca.reject(pa)
	}

	switch pa.stage {
	case stageConnected:
		return ca.Handshake(ctx, pa, conn, frame)
	case stageAwaitAuth:
		return ca.Authenticate(ctx, pa, conn, frame)
	default:
		ca.rejections.Record(rejection.ActivationState, nil)
		return ca.reject(pa)
	}
}

// Handshake
//
// Body  = [version: 1][hub_key_id: 4 byte][ephemeral public key: 32 byte][authentication tag: 16 byte]
//
// Frame = [length: 2][Body]
func (ca *ConnActivator) Handshake(
	ctx context.Context,
	pa *pendingActivation,
	conn nio.Conn,
	frame []byte,
) nio.Action {
	if pa.stage != stageConnected {
		ca.rejections.Record(rejection.ActivationState, nil)
		return ca.reject(pa)
	}

	body, err := protocol.FrameBody(frame, ca.maxFrame)
	if err != nil || len(body) != handshakeBodySize || body[0] != hubendpoint.WireVersion {
		ca.rejections.Record(rejection.HandshakeInvalid, err)
		return ca.reject(pa)
	}

	responder, err := ca.factory.NewResponder(binary.LittleEndian.Uint32(body[1:5]))
	if err != nil {
		ca.rejections.Record(rejection.HandshakeKey, err)
		return ca.reject(pa)
	}
	defer responder.Close()

	err = responder.ReadClientHello(body[5:])
	if err != nil {
		ca.rejections.Record(rejection.HandshakeInvalid, err)
		return ca.reject(pa)
	}

	pa.identity = responder.Identity()
	pa.challenge = slicepool.Get(auth.ChallengeSize)

	_, err = rand.Read(pa.challenge)
	if err != nil {
		return ca.reject(pa)
	}

	hello, ch, binding, err := responder.WriteServerHello(pa.challenge)
	if err != nil {
		return ca.reject(pa)
	}

	defer func() {
		clear(hello[:cap(hello)])
		slicepool.Put(hello)
	}()
	pa.channel = ch
	pa.binding = binding

	if time.Now().After(pa.deadline) || time.Now().After(pa.phaseDeadline) {
		ca.rejections.Record(rejection.HandshakeExpired, nil)
		return ca.reject(pa)
	}

	out, err := protocol.EncodeFrame(hello, ca.maxFrame)
	if err != nil {
		return ca.reject(pa)
	}

	defer func() {
		clear(out[:cap(out)])
		slicepool.Put(out)
	}()

	n, err := conn.Write(out)
	if err != nil || n != len(out) {
		return ca.reject(pa)
	}

	pa.stage = stageAwaitAuth
	pa.phaseDeadline = time.Now().Add(activationChallengeTTL)
	pa.phaseTimer.Stop()
	pa.phaseTimer = ca.arm(ctx, conn, pa, activationChallengeTTL, stageAwaitAuth)

	return nio.None
}

// Authenticate
//
// Frame = [length: 2][Body]
func (ca *ConnActivator) Authenticate(
	ctx context.Context,
	pa *pendingActivation,
	conn nio.Conn,
	frame []byte,
) nio.Action {
	if pa.stage != stageAwaitAuth {
		ca.rejections.Record(rejection.ActivationState, nil)
		return ca.reject(pa)
	}

	plain, err := pa.channel.DecryptRecord(frame)
	if err != nil {
		ca.rejections.Record(rejection.ProofDecode, err)
		return ca.reject(pa)
	}

	defer func() {
		clear(plain[:cap(plain)])
		slicepool.Put(plain)
	}()

	payload, err := ca.decoder.DecodePayload(plain)
	if err != nil || payload.Kind != pbpub.MsgKind_REQ_AUTHENTICATE || len(payload.Expand) != 0 {
		ca.rejections.Record(rejection.ActivateDecode, err)
		return ca.reject(pa)
	}

	req, ok := payload.Message.(*pbpub.AuthenticateReq)
	if !ok {
		ca.rejections.Record(rejection.ActivateValidate, nil)
		return ca.reject(pa)
	}

	err = req.Validate()
	if err != nil {
		ca.rejections.Record(rejection.ActivateValidate, err)
		return ca.reject(pa)
	}

	// Neither decoded bytes nor pending activation buffers may escape to a
	// worker: the loop can clear/reuse them as soon as this callback returns.
	var (
		raw           = req.Ticket
		signature     = req.Signature
		binding       = bytes.Clone(pa.binding)
		challenge     = bytes.Clone(pa.challenge)
		identity      = pa.identity
		deadline      = pa.deadline
		phaseDeadline = pa.phaseDeadline
		loop          = conn.EventLoop()
	)

	pa.stage = stageVerifying
	cleanup := func() {
		clear(raw[:cap(raw)])
		clear(signature[:cap(signature)])
		clear(binding[:cap(binding)])
		clear(challenge[:cap(challenge)])
	}

	err = ca.verifier.Submit(func() {
		defer cleanup()

		// Skip work for connections closed or expired while waiting in the pool.
		if pa.released.Load() {
			return
		}

		var (
			claims    *ticket.Info
			verifyErr error
		)

		if ctx.Err() != nil ||
			time.Now().After(deadline) ||
			time.Now().After(phaseDeadline) {
			verifyErr = context.DeadlineExceeded
		} else {
			claims, verifyErr = ca.activationSvc.VerifyAuthorization(
				raw,
				signature,
				binding,
				challenge,
				identity,
			)
		}

		executeErr := loop.Execute(ctx, func(context.Context) error {
			// All connection state and channel ownership stay on the owning loop.
			if pa.released.Load() || conn.Context() != pa || pa.stage != stageVerifying {
				return nil
			}

			// Do not count shutdown cancellation as a client rejection.
			if verifyErr != nil || claims == nil || ctx.Err() != nil ||
				time.Now().After(pa.deadline) ||
				time.Now().After(pa.phaseDeadline) {
				if ctx.Err() == nil {
					reason := rejection.ActivateTicket
					if errors.Is(verifyErr, context.DeadlineExceeded) ||
						time.Now().After(pa.deadline) ||
						time.Now().After(pa.phaseDeadline) {
						reason = rejection.ProofExpired
					} else if errors.Is(verifyErr, auth.ErrDeviceProofInvalid) {
						reason = rejection.ProofInvalid
					}
					ca.rejections.Record(reason, verifyErr)
				}
				pa.clear()
				ca.manager.ForceClose(conn)
				return nil
			}
			// Completion may have waited in the loop queue after verification.
			// Reject an expired ticket before replacing any existing connection.
			err = ca.activationSvc.CheckExpiry(claims, time.Now())
			if err != nil {
				ca.rejections.Record(rejection.ProofExpired, err)
				pa.clear()
				ca.manager.ForceClose(conn)
				return nil
			}
			ca.register(ctx, pa, conn, claims)
			return nil
		})
		if executeErr != nil {
			// Close is safe from outside the loop; Deactivate owns pending cleanup.
			_ = conn.Close()
		}
	})
	if err != nil {
		cleanup()
		return ca.reject(pa)
	}
	return nio.None
}

// register resumes authorization on the owning connection event loop.
func (ca *ConnActivator) register(
	ctx context.Context,
	pa *pendingActivation,
	conn nio.Conn,
	info *ticket.Info,
) {
	pa.stage = stageRegistering
	pa.phaseTimer.Stop()
	ch := pa.channel
	pa.channel = nil // Manager consumes ownership, including on error.

	err := ca.manager.ActivateConn(
		ctx,
		info.DeviceID,
		ch,
		conn,
		pa.clear,
		func(regErr error) {
			if pa.released.Load() {
				return
			}

			// Registration can outlive the verified ticket; recheck before ACK.
			if regErr != nil ||
				time.Now().After(pa.deadline) ||
				ca.activationSvc.CheckExpiry(info, time.Now()) != nil {
				pa.clear()
				ca.manager.ForceClose(conn)
				return
			}

			caErr := ca.manager.CompleteActivation(
				conn,
				connection.SendData{
					MsgKind: pbpub.MsgKind_RESP_AUTHENTICATE,
					Msg:     &pbpub.AuthenticateResp{Code: pbpub.Code_SUCCESS},
				},
			)
			if caErr != nil {
				pa.clear()
				ca.manager.ForceClose(conn)
			}
		},
	)
	if err != nil {
		pa.clear()
		ca.manager.ForceClose(conn)
	}
}

func (ca *ConnActivator) Deactivate(ctx context.Context, conn nio.Conn) nio.Action {
	pa, ok := conn.Context().(*pendingActivation)
	if ok {
		pa.clear()
	}
	ca.manager.RemoveConn(ctx, conn)
	conn.SetContext(nil)
	return nio.None
}

func (ca *ConnActivator) Tick() (time.Duration, nio.Action) {
	return ca.manager.Tick(), nio.None
}

// Timer goroutines enqueue closure checks. They never touch channel state.
func (ca *ConnActivator) arm(
	ctx context.Context,
	conn nio.Conn,
	pa *pendingActivation,
	duration time.Duration,
	stage uint8,
) *time.Timer {
	return time.AfterFunc(duration, func() {
		err := conn.EventLoop().Execute(ctx, func(context.Context) error {
			if !pa.released.Load() &&
				(stage == stageAny ||
					pa.stage == stage ||
					(stage == stageAwaitAuth && pa.stage == stageVerifying)) {
				if ctx.Err() == nil {
					ca.rejections.Record(expiryReason(pa.stage), nil)
				}
				ca.manager.ForceClose(conn)
			}
			return nil
		})
		if err != nil {
			_ = conn.Close()
		}
	})
}

func (ca *ConnActivator) reject(pa *pendingActivation) nio.Action {
	pa.clear()
	return nio.Close
}

func expiryReason(stage uint8) rejection.Reason {
	if stage == stageConnected {
		return rejection.HandshakeExpired
	}
	return rejection.ProofExpired
}
