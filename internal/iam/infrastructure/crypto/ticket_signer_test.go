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

	"xxx/internal/shared/ticket"
	"xxx/internal/shared/ticket/tickettest"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
)

func TestSignerOwnsSeedAndEmitsPublicSignedClaims(t *testing.T) {
	seed := tickettest.SigningSeed()
	private := ed25519.NewKeyFromSeed(seed)
	signer, err := NewTicketSigner(1, seed)
	require.NoError(t, err)
	clear(seed[:cap(seed)])

	meta := tickettest.New()
	before := *meta
	wire, err := signer.Sign(meta)
	t.Cleanup(func() {
		clear(wire[:cap(wire)])
		slicepool.Put(wire)
	})
	require.NoError(t, err)
	require.Equal(t, before, *meta)
	require.Equal(t, byte(2), wire[0])
	require.True(t, bytes.Contains(wire, meta.IdentityPub[:]))
	require.True(t, bytes.Contains(wire, meta.HubPublicKey[:]))

	payload, err := ticket.MarshalPayload(meta)
	t.Cleanup(func() {
		clear(payload[:cap(payload)])
		slicepool.Put(payload)
	})
	require.NoError(t, err)
	require.Len(t, wire, ticket.SignedHeaderSize+len(payload)+ed25519.SignatureSize)
	require.Equal(t, byte(1), wire[1])
	require.Equal(t, uint16(len(payload)), binary.LittleEndian.Uint16(wire[2:4]))

	unsigned := wire[:len(wire)-ed25519.SignatureSize]
	require.Equal(t, payload, unsigned[ticket.SignedHeaderSize:])

	message := append([]byte("xxx/iam/ticket-signature/v2\x00"), unsigned...)
	require.True(
		t,
		ed25519.Verify(
			private.Public().(ed25519.PublicKey),
			message,
			wire[len(unsigned):],
		),
	)

	_, err = signer.Sign(nil)
	require.ErrorIs(t, err, ticket.ErrTicketInvalid)

	for _, n := range []int{0, ed25519.SeedSize - 1, ed25519.SeedSize + 1, ed25519.PrivateKeySize} {
		_, err = NewTicketSigner(1, make([]byte, n))
		require.Error(t, err)
	}

	_, err = NewTicketSigner(0, make([]byte, ed25519.SeedSize))
	require.Error(t, err)
}
