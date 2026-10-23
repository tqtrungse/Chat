/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package crypto

import (
	"crypto/ed25519"
	"encoding/binary"
	"fmt"

	"xxx/internal/shared/ticket"

	slicepool "xxx/pkg/pool/slice"
)

type TicketSigner struct {
	keyID      byte
	privateKey ed25519.PrivateKey
}

// NewTicketSigner validates the nonzero signing key ID and Ed25519 seed length,
// then derives and retains its own private key without retaining the seed slice.
func NewTicketSigner(id byte, seed []byte) (*TicketSigner, error) {
	if id == 0 || len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("invalid ticket signing key ID or seed length")
	}
	return &TicketSigner{
		keyID:      id,
		privateKey: ed25519.NewKeyFromSeed(seed),
	}, nil
}

// Sign validates and serializes the claims, then signs the domain-separated ticket
// envelope with Ed25519. The caller must clear the returned pooled buffer and
// return it to slicepool after use.
func (s *TicketSigner) Sign(info *ticket.Info) ([]byte, error) {
	payload, err := ticket.MarshalPayload(info)
	if err != nil {
		return nil, err
	}

	l := len(payload)
	unsignedLen := ticket.SignedHeaderSize + l
	out := slicepool.Get(unsignedLen + ed25519.SignatureSize)
	out[0] = ticket.SignedVersion
	out[1] = s.keyID
	binary.LittleEndian.PutUint16(out[2:], uint16(l))
	copy(out[ticket.SignedHeaderSize:], payload)
	message := ticket.SigningMessage(out[:unsignedLen])
	signature := ed25519.Sign(s.privateKey, message)
	copy(out[unsignedLen:], signature)

	clear(payload[:cap(payload)])
	clear(message[:cap(message)])
	clear(signature[:cap(signature)])
	slicepool.Put(payload)
	slicepool.Put(message)
	slicepool.Put(signature)

	return out, nil
}
