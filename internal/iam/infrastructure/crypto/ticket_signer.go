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

package crypto

import (
	"crypto/ed25519"
	"fmt"

	sharedticket "xxx/internal/shared/ticket"

	slicepool "xxx/pkg/pool/slice"
)

type TicketSigner struct {
	sealer     *sharedticket.Sealer
	keyID      byte
	privateKey ed25519.PrivateKey
}

func NewTicketSigner(
	sealer *sharedticket.Sealer,
	keyID byte,
	seed []byte,
) (*TicketSigner, error) {
	if sealer == nil {
		return nil, sharedticket.ErrTicketSealerRequired
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
func (s *TicketSigner) Seal(t *sharedticket.Ticket) ([]byte, error) {
	if t == nil {
		return nil, sharedticket.ErrTicketInvalid
	}

	sealed, err := s.sealer.Seal(t)
	if err != nil {
		return nil, err
	}

	out := make([]byte, sharedticket.SignedSize)
	out[0], out[1] = sharedticket.SignedVersion, s.keyID
	copy(out[sharedticket.SignedHeaderSize:sharedticket.SignedUnsignedSize], sealed)

	message := sharedticket.SigningMessage(out[:sharedticket.SignedUnsignedSize])
	signature := ed25519.Sign(s.privateKey, message)
	clear(message)
	slicepool.Put(message)
	copy(out[sharedticket.SignedUnsignedSize:], signature)

	return out, nil
}
