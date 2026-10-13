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
	"errors"
	"fmt"
	"time"

	sharedticket "xxx/internal/shared/ticket"

	slicepool "xxx/pkg/pool/slice"
)

var (
	ErrTicketPubKeysRequired   = errors.New("ticket verification keys are required")
	ErrTicketClockSkewRequired = errors.New("ticket clock skew is required")
	ErrTicketExpired           = errors.New("ticket expired")
)

type TicketVerifier struct {
	sealer     *sharedticket.Sealer
	clockSkew  time.Duration
	publicKeys map[byte]ed25519.PublicKey
}

// NewTicketVerifier
//
// ClockSkew is the allowed clock difference between IAM and Hub.
// Zero disables the allowance.
func NewTicketVerifier(
	sealer *sharedticket.Sealer,
	publicKeys map[byte]ed25519.PublicKey,
	clockSkew time.Duration,
) (*TicketVerifier, error) {
	if sealer == nil {
		return nil, sharedticket.ErrTicketSealerRequired
	}

	if len(publicKeys) == 0 {
		return nil, ErrTicketPubKeysRequired
	}

	if clockSkew < 0 {
		return nil, ErrTicketClockSkewRequired
	}

	keys := make(map[byte]ed25519.PublicKey, len(publicKeys))
	for id, key := range publicKeys {
		if len(key) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("ticket public key %d: want %d bytes, got %d", id, ed25519.PublicKeySize, len(key))
		}
		keys[id] = append(ed25519.PublicKey(nil), key...)
	}

	return &TicketVerifier{
		sealer:     sealer,
		clockSkew:  clockSkew,
		publicKeys: keys,
	}, nil
}

// Open verifies IAM's signature before decrypting.
// The caller owns the returned ticket and must call Clear when finished.
func (v *TicketVerifier) Open(deviceID uint64, wire []byte) (*sharedticket.Ticket, error) {
	if len(wire) != sharedticket.SignedSize || wire[0] != sharedticket.SignedVersion {
		return nil, sharedticket.ErrTicketInvalid
	}

	publicKey, ok := v.publicKeys[wire[1]]
	if !ok {
		return nil, sharedticket.ErrTicketInvalid
	}

	message := sharedticket.SigningMessage(wire[:sharedticket.SignedUnsignedSize])
	success := ed25519.Verify(
		publicKey,
		message,
		wire[sharedticket.SignedUnsignedSize:],
	)
	clear(message)
	slicepool.Put(message)

	if !success {
		return nil, sharedticket.ErrTicketInvalid
	}

	t, err := v.sealer.Open(
		deviceID,
		wire[sharedticket.SignedHeaderSize:sharedticket.SignedUnsignedSize],
	)
	if err != nil {
		return nil, err
	}

	if err = v.CheckExpiry(t, time.Now()); err != nil {
		t.Clear() // Open owns the ticket until it returns successfully.
		return nil, err
	}
	return t, nil
}

// CheckExpiry checks only the expiration of an already authenticated ticket.
// It does not decrypt, verify signatures, mutate or clear the ticket.
func (v *TicketVerifier) CheckExpiry(t *sharedticket.Ticket, now time.Time) error {
	if t == nil {
		return sharedticket.ErrTicketInvalid
	}

	deadline := time.Unix(t.ExpiresAt, 0).Add(v.clockSkew)
	if now.After(deadline) {
		return ErrTicketExpired
	}
	return nil
}
