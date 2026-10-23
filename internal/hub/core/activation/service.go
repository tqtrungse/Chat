/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package activation

import (
	"errors"
	"time"

	"xxx/internal/hub/core/auth"
	"xxx/internal/hub/core/transport"
	"xxx/internal/shared/ticket"

	slicepool "xxx/pkg/pool/slice"
)

type Service struct {
	signer         Signer
	ticketVerifier TicketVerifier
}

// NewService creates an authorization service using the ticket and device-proof verifiers.
func NewService(signer Signer, verifier TicketVerifier) *Service {
	return &Service{
		signer,
		verifier,
	}
}

// VerifyAuthorization validates the signed ticket against the local Hub identity,
// then verifies the device signature bound to the ticket, channel, and challenge.
func (s *Service) VerifyAuthorization(
	raw []byte,
	signature []byte,
	binding []byte,
	challenge []byte,
	hub transport.Identity,
) (*ticket.Info, error) {
	meta, err := s.ticketVerifier.Verify(raw, hub)
	if err != nil {
		return nil, err
	}

	msg, err := auth.SigningMessage(binding, raw, challenge)
	if err != nil {
		return nil, err
	}
	defer slicepool.Put(msg)

	err = s.signer.Verify(meta.IdentityPub[:], msg, signature)
	if err != nil {
		// Preserve the verifier error while exposing a transport-independent reason.
		return nil, errors.Join(auth.ErrDeviceProofInvalid, err)
	}
	return meta, nil
}

// CheckExpiry rechecks the ticket validity window after asynchronous work.
func (s *Service) CheckExpiry(info *ticket.Info, now time.Time) error {
	return s.ticketVerifier.CheckExpiry(info, now)
}
