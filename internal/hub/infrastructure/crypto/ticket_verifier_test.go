/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package crypto

import (
	"bytes"
	"crypto/ed25519"
	"encoding/binary"
	"testing"
	"time"

	"xxx/internal/hub/core/transport"
	"xxx/internal/shared/ticket"
	"xxx/internal/shared/ticket/tickettest"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
)

func TestVerifierAuthenticatesBeforeParsingAndSupportsRotation(t *testing.T) {
	seed := tickettest.SigningSeed()
	private := ed25519.NewKeyFromSeed(seed)
	pub := private.Public().(ed25519.PublicKey)
	verifier, err := NewTicketVerifier(
		map[byte]ed25519.PublicKey{
			1: pub,
			2: pub,
		},
		5*time.Second,
	)
	require.NoError(t, err)
	clear(pub[:cap(pub)])

	for _, id := range []byte{1, 2} {
		var out *ticket.Info
		in := tickettest.New()
		wire := signedTicketFixture(t, id, private, in)
		before := bytes.Clone(wire)
		out, err = verifier.Verify(
			wire,
			transport.Identity{
				Audience:  in.Audience,
				KeyID:     in.HubKeyID,
				PublicKey: in.HubPublicKey,
				Version:   in.WireVersion,
			},
		)
		require.NoError(t, err)
		require.Equal(t, in, out)
		require.Equal(t, before, wire)

		for i := range wire {
			bad := bytes.Clone(wire)
			bad[i] ^= 1
			_, err = verifier.Verify(
				bad,
				transport.Identity{
					Audience:  in.Audience,
					KeyID:     in.HubKeyID,
					PublicKey: in.HubPublicKey,
					Version:   in.WireVersion,
				},
			)
			require.Error(t, err, "byte %d", i)
		}

		for n := range wire {
			_, err = verifier.Verify(
				wire[:n],
				transport.Identity{
					Audience:  in.Audience,
					KeyID:     in.HubKeyID,
					PublicKey: in.HubPublicKey,
					Version:   in.WireVersion,
				},
			)
			require.Error(t, err)
		}

		_, err = verifier.Verify(
			append(wire, 0),
			transport.Identity{
				Audience:  in.Audience,
				KeyID:     in.HubKeyID,
				PublicKey: in.HubPublicKey,
				Version:   in.WireVersion,
			},
		)
		require.Error(t, err)
	}

	now := time.Now()
	in := tickettest.New()
	in.IssuedAt = now.Add(-20 * time.Second).Unix()
	in.ExpiresAt = in.IssuedAt + 15
	deadline := time.Unix(in.ExpiresAt, 0).Add(5 * time.Second)
	require.NoError(t, verifier.CheckExpiry(in, deadline))
	require.ErrorIs(
		t,
		verifier.CheckExpiry(in, deadline.Add(time.Nanosecond)),
		ErrTicketExpired,
	)

	in = tickettest.New()
	require.Error(
		t,
		verifier.CheckExpiry(in, time.Unix(in.IssuedAt, 0).Add(-5*time.Second-time.Nanosecond)),
	)
	require.ErrorIs(
		t,
		verifier.CheckExpiry(nil, now),
		ticket.ErrTicketInvalid,
	)

	_, err = NewTicketVerifier(nil, 0)
	require.Error(t, err)

	_, err = NewTicketVerifier(
		map[byte]ed25519.PublicKey{1: make([]byte, ed25519.PublicKeySize)},
		-1,
	)
	require.Error(t, err)

	_, err = NewTicketVerifier(
		map[byte]ed25519.PublicKey{1: make([]byte, ed25519.PublicKeySize-1)},
		0,
	)
	require.Error(t, err)
}

// signedTicketFixture builds a signed envelope from the shared wire contract
// without depending on the IAM signer implementation. The result is not pooled.
func signedTicketFixture(
	t *testing.T,
	keyID byte,
	private ed25519.PrivateKey,
	claims *ticket.Info,
) []byte {
	t.Helper()

	payload, err := ticket.MarshalPayload(claims)
	require.NoError(t, err)
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
	return append(wire, ed25519.Sign(private, message)...)
}

func TestVerifierRejectsWrongHubAndInvalidSignedClaims(t *testing.T) {
	private := ed25519.NewKeyFromSeed(tickettest.SigningSeed())
	verifier, err := NewTicketVerifier(
		map[byte]ed25519.PublicKey{1: private.Public().(ed25519.PublicKey)},
		0,
	)
	require.NoError(t, err)

	claims := tickettest.New()
	wire := signedTicketFixture(
		t,
		1,
		private,
		claims,
	)
	expected := transport.Identity{
		Audience:  claims.Audience,
		KeyID:     claims.HubKeyID,
		PublicKey: claims.HubPublicKey,
		Version:   claims.WireVersion,
	}

	for _, field := range []string{
		"audience",
		"key ID",
		"public key",
		"version",
	} {
		t.Run(
			field,
			func(t *testing.T) {
				wrong := expected

				switch field {
				case "audience":
					wrong.Audience = "other"
				case "key ID":
					wrong.KeyID++
				case "public key":
					wrong.PublicKey[0] ^= 1
				case "version":
					wrong.Version++
				}

				_, err2 := verifier.Verify(wire, wrong)
				require.ErrorIs(t, err2, ticket.ErrTicketInvalid)
			},
		)
	}

	for _, field := range []string{
		"identity",
		"ticket ID",
		"TTL",
		"expiry",
		"future",
	} {
		t.Run(
			field,
			func(t *testing.T) {
				bad := bytes.Clone(wire)
				payload := bad[ticket.SignedHeaderSize : len(bad)-ed25519.SignatureSize]
				want := ticket.ErrTicketInvalid

				switch field {
				case "identity":
					identity := payload[40:72:72]
					clear(identity[:cap(identity)])

				case "ticket ID":
					id := payload[:ticket.IDSize:ticket.IDSize]
					clear(id[:cap(id)])

				case "TTL":
					binary.LittleEndian.PutUint64(
						payload[32:],
						uint64(claims.IssuedAt+int64(ticket.MaxTTL/time.Second)+1),
					)

				case "expiry":
					now := time.Now().Unix()
					binary.LittleEndian.PutUint64(payload[24:], uint64(now-20))
					binary.LittleEndian.PutUint64(payload[32:], uint64(now-5))
					want = ErrTicketExpired

				case "future":
					now := time.Now().Unix()
					binary.LittleEndian.PutUint64(payload[24:], uint64(now+30))
					binary.LittleEndian.PutUint64(payload[32:], uint64(now+40))
					want = ErrTicketExpired
				}

				msgLen := len(bad) - ed25519.SignatureSize
				message := append([]byte(ticket.SigningDomain), bad[:msgLen]...)
				copy(bad[msgLen:], ed25519.Sign(private, message))
				_, err2 := verifier.Verify(bad, expected)
				require.ErrorIs(t, err2, want)
			},
		)
	}
}

func BenchmarkTicketVerifier(b *testing.B) {
	claims := tickettest.New()
	wire, err := tickettest.Signed(1, claims)
	if err != nil {
		b.Fatal(err)
	}

	private := ed25519.NewKeyFromSeed(tickettest.SigningSeed())
	verifier, err := NewTicketVerifier(
		map[byte]ed25519.PublicKey{1: private.Public().(ed25519.PublicKey)},
		time.Hour,
	)
	if err != nil {
		b.Fatal(err)
	}

	expected := transport.Identity{
		Audience:  claims.Audience,
		KeyID:     claims.HubKeyID,
		PublicKey: claims.HubPublicKey,
		Version:   claims.WireVersion,
	}

	for _, name := range []string{"valid", "wrong hub"} {
		b.Run(
			name,
			func(b *testing.B) {
				hub := expected
				if name == "wrong hub" {
					hub.Audience = "other"
				}

				b.ReportAllocs()
				for b.Loop() {
					_, err2 := verifier.Verify(wire, hub)
					if (err2 != nil) != (name == "wrong hub") {
						b.Fatal(err2)
					}
				}
			},
		)
	}
}
