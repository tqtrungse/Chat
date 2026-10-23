/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package crypto

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"time"

	slicepool "xxx/pkg/pool/slice"

	"xxx/internal/hub/core/transport"
	"xxx/internal/shared/ticket"
)

var (
	ErrTicketPubKeysRequired   = errors.New("ticket verification keys are required")
	ErrTicketClockSkewRequired = errors.New("ticket clock skew must be nonnegative")
	ErrTicketExpired           = errors.New("ticket expired or issued in the future")
)

type TicketVerifier struct {
	clockSkew  time.Duration
	publicKeys map[byte]ed25519.PublicKey
}

func NewTicketVerifier(
	keys map[byte]ed25519.PublicKey,
	skew time.Duration,
) (*TicketVerifier, error) {
	if len(keys) == 0 {
		return nil, ErrTicketPubKeysRequired
	}

	if skew < 0 {
		return nil, ErrTicketClockSkewRequired
	}

	owned := make(map[byte]ed25519.PublicKey, len(keys))
	for id, key := range keys {
		if id == 0 || len(key) != ed25519.PublicKeySize {
			return nil, ticket.ErrTicketInvalid
		}
		owned[id] = append(ed25519.PublicKey(nil), key...)
	}

	return &TicketVerifier{
		clockSkew:  skew,
		publicKeys: owned,
	}, nil
}

func (tv *TicketVerifier) Verify(
	wire []byte,
	expected transport.Identity,
) (*ticket.Info, error) {
	payloadLen := len(wire) - ticket.SignedHeaderSize - ed25519.SignatureSize

	if len(wire) < ticket.MinSignedSize ||
		len(wire) > ticket.MaxSignedSize ||
		wire[0] != ticket.SignedVersion ||
		int(binary.LittleEndian.Uint16(wire[2:])) != payloadLen {
		return nil, ticket.ErrTicketInvalid
	}

	key, ok := tv.publicKeys[wire[1]]
	if !ok {
		return nil, ticket.ErrTicketInvalid
	}

	msgLen := len(wire) - ed25519.SignatureSize
	message := ticket.SigningMessage(wire[:msgLen])
	defer func() {
		clear(message[:cap(message)])
		slicepool.Put(message)
	}()

	valid := ed25519.Verify(key, message, wire[msgLen:])
	if !valid {
		return nil, ticket.ErrTicketInvalid
	}

	info, err := ticket.DecodePayload(wire[ticket.SignedHeaderSize:msgLen])
	if err != nil {
		return nil, err
	}

	err = info.ValidateClaims()
	if err != nil {
		return nil, err
	}

	if info.Audience != expected.Audience ||
		info.HubKeyID != expected.KeyID ||
		info.HubPublicKey != expected.PublicKey ||
		info.WireVersion != expected.Version {
		return nil, ticket.ErrTicketInvalid
	}

	// The exact Hub key has already been validated by the local transport
	// configuration. Repeating an X25519 probe here adds no validation.
	err = tv.CheckExpiry(info, time.Now())
	if err != nil {
		return nil, err
	}

	err = ticket.ValidateIdentity(info.IdentityPub[:])
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (tv *TicketVerifier) CheckExpiry(info *ticket.Info, now time.Time) error {
	if info == nil {
		return ticket.ErrTicketInvalid
	}
	if now.After(time.Unix(info.ExpiresAt, 0).Add(tv.clockSkew)) ||
		now.Before(time.Unix(info.IssuedAt, 0).Add(-tv.clockSkew)) {
		return ErrTicketExpired
	}
	return nil
}
