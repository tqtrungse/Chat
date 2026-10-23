/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

// Package tickettest supplies deterministic public test keys, never production secrets.
package tickettest

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"encoding/binary"
	"time"

	"xxx/internal/shared/hubendpoint"
	"xxx/internal/shared/ticket"

	slicepool "xxx/pkg/pool/slice"
)

// IdentityPrivate returns a newly allocated, deterministic Ed25519 device identity for tests.
func IdentityPrivate() ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x11}, ed25519.SeedSize))
}

// SigningSeed returns a newly allocated, deterministic Ed25519 ticket-signing seed for tests.
func SigningSeed() []byte {
	return bytes.Repeat([]byte{0x22}, ed25519.SeedSize)
}

// HubPrivate returns a newly allocated, deterministic X25519 private key for tests.
func HubPrivate() []byte {
	return bytes.Repeat([]byte{0x33}, 32)
}

// HubPublic derives the X25519 public key matching the deterministic HubPrivate fixture.
func HubPublic() [hubendpoint.X25519PublicKeySize]byte {
	p, _ := ecdh.X25519().NewPrivateKey(HubPrivate())
	return [hubendpoint.X25519PublicKeySize]byte(p.PublicKey().Bytes())
}

// New returns public ticket claims for the test device and Hub, valid for 15 seconds
// from the current time. The claims are not signed.
func New() *ticket.Info {
	now := time.Now().Unix()
	info := &ticket.Info{
		DeviceID:     42,
		IssuedAt:     now,
		ExpiresAt:    now + 15,
		Audience:     "hub.chat",
		HubKeyID:     7,
		HubPublicKey: HubPublic(),
		WireVersion:  hubendpoint.WireVersion,
	}
	info.TicketID[0] = 1
	copy(info.IdentityPub[:], IdentityPrivate().Public().(ed25519.PublicKey))
	return info
}

// Authorization returns opaque, structurally valid wire fields for application tests.
// These bytes are not a cryptographically valid ticket or proof.
func Authorization() (rawTicket, signature []byte) {
	return bytes.Repeat([]byte{0x44}, ticket.MinSignedSize), bytes.Repeat([]byte{0x55}, ed25519.SignatureSize)
}

// Signed returns a valid, non-pooled ticket fixture using the shared wire
// contract and public test signing seed, independently of the IAM signer.
func Signed(keyID byte, claims *ticket.Info) ([]byte, error) {
	payload, err := ticket.MarshalPayload(claims)
	if err != nil {
		return nil, err
	}
	defer func() {
		clear(payload[:cap(payload)])
		slicepool.Put(payload)
	}()

	wire := make([]byte, ticket.SignedHeaderSize)
	wire[0] = ticket.SignedVersion
	wire[1] = keyID
	binary.LittleEndian.PutUint16(wire[2:], uint16(len(payload)))
	wire = append(wire, payload...)
	message := append([]byte(ticket.SigningDomain), wire...)
	private := ed25519.NewKeyFromSeed(SigningSeed())
	defer clear(private)
	return append(wire, ed25519.Sign(private, message)...), nil
}
