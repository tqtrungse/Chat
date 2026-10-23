/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package integration_test

import (
	"bytes"
	"crypto/ed25519"
	"testing"
	"time"

	"xxx/internal/hub/core/transport"
	hubcrypto "xxx/internal/hub/infrastructure/crypto"
	iamcrypto "xxx/internal/iam/infrastructure/crypto"
	"xxx/internal/shared/ticket/tickettest"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
)

func TestIAMSignedTicketIsAcceptedByHub(t *testing.T) {
	seed := tickettest.SigningSeed()
	public := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	verifier, err := hubcrypto.NewTicketVerifier(
		map[byte]ed25519.PublicKey{7: public},
		5*time.Second,
	)
	require.NoError(t, err)

	signer, err := iamcrypto.NewTicketSigner(7, seed)
	require.NoError(t, err)

	claims := tickettest.New()
	wire, err := signer.Sign(claims)
	require.NoError(t, err)
	defer func() {
		clear(wire[:cap(wire)])
		slicepool.Put(wire)
	}()

	got, err := verifier.Verify(
		wire,
		transport.Identity{
			Audience:  claims.Audience,
			KeyID:     claims.HubKeyID,
			PublicKey: claims.HubPublicKey,
			Version:   claims.WireVersion,
		},
	)
	require.NoError(t, err)
	require.Equal(t, claims, got)

	deadline := time.Unix(got.ExpiresAt, 0).Add(5 * time.Second)
	require.NoError(t, verifier.CheckExpiry(got, deadline))
	require.ErrorIs(
		t,
		verifier.CheckExpiry(got, deadline.Add(time.Nanosecond)),
		hubcrypto.ErrTicketExpired,
	)

	tampered := bytes.Clone(wire)
	tampered[len(tampered)-1] ^= 1
	_, err = verifier.Verify(
		tampered,
		transport.Identity{
			Audience:  claims.Audience,
			KeyID:     claims.HubKeyID,
			PublicKey: claims.HubPublicKey,
			Version:   claims.WireVersion,
		},
	)
	require.Error(t, err)
}
