/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package crypto

import (
	"bytes"
	"crypto/ed25519"
	"testing"
	"time"

	sharedticket "xxx/internal/shared/ticket"

	"github.com/stretchr/testify/require"
)

func verifierTestFixture(t *testing.T, expiresAt int64) (
	*sharedticket.Sealer,
	ed25519.PrivateKey,
	ed25519.PublicKey,
	*sharedticket.Ticket,
	[]byte,
) {
	t.Helper()

	aesKey := bytes.Repeat([]byte{0xA1}, 32)
	sealer, err := sharedticket.NewSealer(3, map[byte][]byte{3: aesKey})
	clear(aesKey)
	require.NoError(t, err)

	seed := bytes.Repeat([]byte{0xB2}, ed25519.SeedSize)
	privateKey := ed25519.NewKeyFromSeed(seed)
	clear(seed)
	publicKey := bytes.Clone(privateKey.Public().(ed25519.PublicKey))
	t.Cleanup(func() {
		clear(privateKey)
		clear(publicKey)
	})

	in := &sharedticket.Ticket{DeviceID: 42, ExpiresAt: expiresAt}
	for i := range in.Keys.RecvEnc {
		in.Keys.RecvEnc[i] = 0x11
		in.Keys.SendEnc[i] = 0x22
		in.Keys.RecvMac[i] = 0x33
		in.Keys.SendMac[i] = 0x44
		in.ActivationKey[i] = 0x55
		in.IdentityPub[i] = 0x66
	}
	t.Cleanup(in.Clear)

	sealed, err := sealer.Seal(in)
	require.NoError(t, err)
	unsigned := make([]byte, sharedticket.SignedUnsignedSize)
	unsigned[0], unsigned[1] = sharedticket.SignedVersion, 7
	copy(unsigned[sharedticket.SignedHeaderSize:], sealed)
	message := append([]byte("xxx/iam/ticket-signature/v1\x00"), unsigned...)
	wire := append(unsigned, ed25519.Sign(privateKey, message)...)
	return sealer, privateKey, publicKey, in, wire
}

func TestNewTicketVerifier_ValidationAndPublicKeyOwnership(t *testing.T) {
	sealer, _, publicKey, _, _ := verifierTestFixture(t, time.Now().Add(time.Hour).Unix())

	verifier, err := NewTicketVerifier(nil, map[byte]ed25519.PublicKey{7: publicKey}, 0)
	require.ErrorIs(t, err, sharedticket.ErrTicketSealerRequired)
	require.Nil(t, verifier)

	verifier, err = NewTicketVerifier(sealer, nil, 0)
	require.ErrorIs(t, err, ErrTicketPubKeysRequired)
	require.Nil(t, verifier)

	for _, skew := range []time.Duration{-time.Nanosecond, -time.Second} {
		verifier, err = NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{7: publicKey}, skew)
		require.ErrorIs(t, err, ErrTicketClockSkewRequired)
		require.Nil(t, verifier)
	}

	verifier, err = NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{7: publicKey[:ed25519.PublicKeySize-1]}, 0)
	require.Error(t, err)
	require.Nil(t, verifier)
}

func TestTicketVerifier_OpenRoundTripAndInputOwnership(t *testing.T) {
	sealer, _, publicKey, in, wire := verifierTestFixture(t, time.Now().Add(time.Hour).Unix())
	verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{7: publicKey}, 5*time.Second)
	require.NoError(t, err)
	before := bytes.Clone(wire)

	out, err := verifier.Open(in.DeviceID, wire)
	require.NoError(t, err)
	defer out.Clear()
	require.Equal(t, in, out)
	require.Equal(t, before, wire, "Open must not mutate the caller's network buffer")
}

func TestTicketVerifier_OpenRejectsMalformedOrUnauthenticatedTicket(t *testing.T) {
	sealer, _, publicKey, in, wire := verifierTestFixture(t, time.Now().Add(time.Hour).Unix())
	verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{7: publicKey}, 0)
	require.NoError(t, err)

	for _, tc := range []struct {
		name string
		wire []byte
	}{
		{"nil", nil},
		{"short", wire[:len(wire)-1]},
		{"long", append(bytes.Clone(wire), 0)},
		{"wrong version", func() []byte { b := bytes.Clone(wire); b[0]++; return b }()},
		{"unknown signing key", func() []byte { b := bytes.Clone(wire); b[1]++; return b }()},
		{"altered signature", func() []byte { b := bytes.Clone(wire); b[len(b)-1] ^= 1; return b }()},
		{"altered signed payload", func() []byte { b := bytes.Clone(wire); b[2] ^= 1; return b }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := verifier.Open(in.DeviceID, tc.wire)
			require.ErrorIs(t, err, sharedticket.ErrTicketInvalid)
			require.Nil(t, out)
		})
	}

	out, err := verifier.Open(in.DeviceID+1, wire)
	require.ErrorIs(t, err, sharedticket.ErrTicketInvalid, "AES-GCM binds the ticket to its device ID")
	require.Nil(t, out)
}

func TestTicketVerifier_OpenEnforcesExpiryAfterAuthentication(t *testing.T) {
	sealer, _, publicKey, in, wire := verifierTestFixture(t, time.Now().Add(-time.Hour).Unix())
	verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{7: publicKey}, 5*time.Second)
	require.NoError(t, err)

	out, err := verifier.Open(in.DeviceID, wire)
	require.ErrorIs(t, err, ErrTicketExpired)
	require.Nil(t, out)
}

func TestTicketVerifier_CheckExpiryBoundariesAndOwnership(t *testing.T) {
	sealer, _, publicKey, _, _ := verifierTestFixture(t, time.Now().Add(time.Hour).Unix())
	verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{7: publicKey}, 5*time.Second)
	require.NoError(t, err)

	expiresAt := time.Unix(1_800_000_000, 0)
	in := &sharedticket.Ticket{ExpiresAt: expiresAt.Unix()}
	before := *in
	deadline := expiresAt.Add(5 * time.Second)
	require.NoError(t, verifier.CheckExpiry(in, deadline))
	require.ErrorIs(t, verifier.CheckExpiry(in, deadline.Add(time.Nanosecond)), ErrTicketExpired)
	require.Equal(t, before, *in, "CheckExpiry does not clear caller-owned tickets")
	require.ErrorIs(t, verifier.CheckExpiry(nil, expiresAt), sharedticket.ErrTicketInvalid)
}

func TestTicketVerifier_CopiesConfiguredPublicKeys(t *testing.T) {
	sealer, _, publicKey, in, wire := verifierTestFixture(t, time.Now().Add(time.Hour).Unix())
	keys := map[byte]ed25519.PublicKey{7: bytes.Clone(publicKey)}
	verifier, err := NewTicketVerifier(sealer, keys, 0)
	require.NoError(t, err)
	clear(keys[7])
	delete(keys, 7)

	out, err := verifier.Open(in.DeviceID, wire)
	require.NoError(t, err, "verifier should retain its independent key copy")
	defer out.Clear()
	require.Equal(t, in, out)
}
