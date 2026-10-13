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

func signerTestSealer(t *testing.T) *sharedticket.Sealer {
	t.Helper()
	key := bytes.Repeat([]byte{0xA1}, 32)
	t.Cleanup(func() { clear(key) })
	sealer, err := sharedticket.NewSealer(3, map[byte][]byte{3: key})
	require.NoError(t, err)
	return sealer
}

func TestNewTicketSigner_ValidationAndSeedOwnership(t *testing.T) {
	sealer := signerTestSealer(t)
	_, err := NewTicketSigner(nil, 7, make([]byte, ed25519.SeedSize))
	require.ErrorIs(t, err, sharedticket.ErrTicketSealerRequired)

	for _, n := range []int{0, 1, ed25519.SeedSize - 1, ed25519.SeedSize + 1, ed25519.PrivateKeySize} {
		t.Run("seed length", func(t *testing.T) {
			signer, err := NewTicketSigner(sealer, 7, make([]byte, n))
			require.Error(t, err)
			require.Nil(t, signer)
		})
	}

	seed := bytes.Repeat([]byte{0xB2}, ed25519.SeedSize)
	wantPrivate := ed25519.NewKeyFromSeed(seed)
	defer clear(wantPrivate)
	signer, err := NewTicketSigner(sealer, 7, seed)
	require.NoError(t, err)
	t.Cleanup(func() { clear(signer.privateKey) })

	clear(seed)
	require.Equal(t, wantPrivate, signer.privateKey, "signer must copy the seed-derived key")
}

func TestTicketSigner_SealWireFormatSignatureAndAESPayload(t *testing.T) {
	sealer := signerTestSealer(t)
	seed := bytes.Repeat([]byte{0xB2}, ed25519.SeedSize)
	signer, err := NewTicketSigner(sealer, 7, seed)
	clear(seed)
	require.NoError(t, err)
	defer clear(signer.privateKey)

	in := &sharedticket.Ticket{
		DeviceID:  42,
		ExpiresAt: time.Now().Add(time.Hour).Unix(),
	}
	for i := range in.Keys.RecvEnc {
		in.Keys.RecvEnc[i] = 0x11
		in.Keys.SendEnc[i] = 0x22
		in.Keys.RecvMac[i] = 0x33
		in.Keys.SendMac[i] = 0x44
		in.ActivationKey[i] = 0x55
		in.IdentityPub[i] = 0x66
	}
	defer in.Clear()
	snapshot := *in

	wire, err := signer.Seal(in)
	require.NoError(t, err)
	require.Equal(t, snapshot, *in, "Seal must not mutate the input ticket")
	require.Len(t, wire, 303)
	require.Equal(t, byte(2), wire[0], "envelope version")
	require.Equal(t, byte(7), wire[1], "signing key ID")
	require.Equal(t, byte(3), wire[2], "AES key ID")

	// Verify independently from SigningMessage so this pins the wire domain.
	message := append([]byte("xxx/iam/ticket-signature/v1\x00"), wire[:239]...)
	publicKey := signer.privateKey.Public().(ed25519.PublicKey)
	require.True(t, ed25519.Verify(publicKey, message, wire[239:]))
	require.False(t, ed25519.Verify(publicKey, wire[:239], wire[239:]))

	opened, err := sealer.Open(in.DeviceID, wire[2:239])
	require.NoError(t, err)
	defer opened.Clear()
	require.Equal(t, in, opened)

	second, err := signer.Seal(in)
	require.NoError(t, err)
	require.NotEqual(t, wire[3:15], second[3:15], "each ticket must use a fresh GCM nonce")
	require.NotEqual(t, wire, second)
}

func TestTicketSigner_RejectsNilTicketAndUnauthenticatedChanges(t *testing.T) {
	sealer := signerTestSealer(t)
	seed := bytes.Repeat([]byte{0xB2}, ed25519.SeedSize)
	signer, err := NewTicketSigner(sealer, 7, seed)
	clear(seed)
	require.NoError(t, err)
	defer clear(signer.privateKey)

	wire, err := signer.Seal(nil)
	require.ErrorIs(t, err, sharedticket.ErrTicketInvalid)
	require.Nil(t, wire)

	in := &sharedticket.Ticket{
		DeviceID:  42,
		ExpiresAt: time.Now().Add(time.Hour).Unix(),
	}
	defer in.Clear()
	wire, err = signer.Seal(in)
	require.NoError(t, err)

	bad := bytes.Clone(wire)
	bad[0]++
	pub := signer.privateKey.Public().(ed25519.PublicKey)
	require.False(t, ed25519.Verify(pub, append([]byte("xxx/iam/ticket-signature/v1\x00"), bad[:239]...), bad[239:]))

	// The encrypted inner payload keeps its fixed protocol length.
	require.Len(t, wire[2:239], 237)
}
