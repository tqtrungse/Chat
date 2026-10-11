/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package session

import (
	"crypto/ed25519"
	"fmt"

	slicepool "xxx/pkg/pool/slice"
)

const (
	signedTicketVersion      byte = 2
	signedTicketHeaderSize        = 2
	signedTicketUnsignedSize      = signedTicketHeaderSize + ticketSize
	SignedTicketSize              = signedTicketUnsignedSize + ed25519.SignatureSize

	ticketSigningDomain = "xxx/iam/ticket-signature/v1\x00"
)

type TicketSigner struct {
	sealer     *TicketSealer
	keyID      byte
	privateKey ed25519.PrivateKey
}

func NewTicketSigner(
	sealer *TicketSealer,
	keyID byte,
	seed []byte,
) (*TicketSigner, error) {
	if sealer == nil {
		return nil, ErrTicketSealerRequired
	}

	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("ticket signing seed: want %d bytes, got %d", ed25519.SeedSize, len(seed))
	}

	return &TicketSigner{
		sealer:     sealer,
		keyID:      keyID,
		privateKey: ed25519.NewKeyFromSeed(seed),
	}, nil
}

// Seal encrypts the payload and signs the complete unsigned envelope.
func (s *TicketSigner) Seal(t *Ticket) ([]byte, error) {
	if t == nil {
		return nil, ErrTicketInvalid
	}

	sealed, err := s.sealer.Seal(t)
	if err != nil {
		return nil, err
	}

	out := make([]byte, SignedTicketSize)
	out[0], out[1] = signedTicketVersion, s.keyID
	copy(out[signedTicketHeaderSize:signedTicketUnsignedSize], sealed)

	message := ticketSigningMessage(out[:signedTicketUnsignedSize])
	signature := ed25519.Sign(s.privateKey, message)
	clear(message)
	slicepool.Put(message)
	copy(out[signedTicketUnsignedSize:], signature)

	return out, nil
}

func ticketSigningMessage(unsigned []byte) []byte {
	n := len(ticketSigningDomain)
	message := slicepool.Get(n + len(unsigned))
	copy(message[:n], ticketSigningDomain)
	copy(message[n:], unsigned)
	return message
}
