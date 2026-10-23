/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package ticket

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"time"

	"xxx/internal/shared/device"
	"xxx/internal/shared/hubendpoint"

	"filippo.io/edwards25519"
)

const (
	MaxTTL = 15 * time.Second
	IDSize = 16
)

var identityPoint = edwards25519.NewIdentityPoint()

// ValidateIdentity checks the Ed25519 public-key size and canonical point encoding,
// rejecting malformed and small-order points. It does not verify a signature.
func ValidateIdentity(pub []byte) error {
	if len(pub) != ed25519.PublicKeySize {
		return ErrTicketInvalid
	}

	var p edwards25519.Point
	_, err := p.SetBytes(pub)
	if err != nil || !bytes.Equal(p.Bytes(), pub) {
		return ErrTicketInvalid
	}

	p.MultByCofactor(&p)
	if p.Equal(identityPoint) == 1 {
		return ErrTicketInvalid
	}
	return nil
}

// Info contains only public authorization claims signed by IAM.
type Info struct {
	TicketID     [IDSize]byte
	DeviceID     device.ID
	IssuedAt     int64
	ExpiresAt    int64
	IdentityPub  [ed25519.PublicKeySize]byte
	Audience     string
	HubKeyID     uint32
	HubPublicKey [hubendpoint.X25519PublicKeySize]byte
	WireVersion  uint8
}

// ValidateClaims checks identifiers, the claimed lifetime, audience, and wire
// version without elliptic-curve operations. It does not check current expiry.
func (i *Info) ValidateClaims() error {
	if i == nil ||
		i.TicketID == [IDSize]byte{} ||
		i.DeviceID == 0 ||
		i.IssuedAt <= 0 ||
		i.ExpiresAt <= i.IssuedAt ||
		i.ExpiresAt-i.IssuedAt > int64(MaxTTL/time.Second) ||
		i.HubKeyID == 0 ||
		i.WireVersion != hubendpoint.WireVersion {
		return ErrTicketInvalid
	}

	if hubendpoint.ValidateAudience(i.Audience) != nil {
		return ErrTicketInvalid
	}
	return nil
}

// Validate checks the claims, device identity key, and Hub X25519 key for callers
// without a trusted Hub identity. It does not authenticate the ticket or check
// expiry against the current time.
func (i *Info) Validate() error {
	err := i.ValidateClaims()
	if err != nil {
		return err
	}

	err = ValidateIdentity(i.IdentityPub[:])
	if err != nil {
		return err
	}

	pub, err := ecdh.X25519().NewPublicKey(i.HubPublicKey[:])
	if err != nil {
		return ErrTicketInvalid
	}

	var (
		probeKey [hubendpoint.X25519PublicKeySize]byte
		probe    *ecdh.PrivateKey
	)

	probe, err = ecdh.X25519().NewPrivateKey(probeKey[:])
	if err != nil {
		return ErrTicketInvalid
	}

	_, err = probe.ECDH(pub)
	if err != nil {
		return ErrTicketInvalid
	}
	return nil
}

// Clear resets all ticket claims and is safe to call on a nil receiver.
func (i *Info) Clear() {
	if i != nil {
		*i = Info{}
	}
}
