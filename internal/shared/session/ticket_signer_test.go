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
	"bytes"
	"crypto/ed25519"
	"fmt"
	"testing"
	"time"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
)

func newTicketSignatureTestSigner(
	t *testing.T,
	sealer *TicketSealer,
	keyID byte,
) (*TicketSigner, ed25519.PublicKey) {
	t.Helper()

	seed := ticketTestKey(t)
	signer, err := NewTicketSigner(sealer, keyID, seed)
	clear(seed)
	require.NoError(t, err)
	t.Cleanup(func() { clear(signer.privateKey) })
	return signer, signer.privateKey.Public().(ed25519.PublicKey)
}

func TestNewTicketSigner_Validation(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)

	signer, err := NewTicketSigner(nil, 1, make([]byte, ed25519.SeedSize))
	require.ErrorIs(t, err, ErrTicketSealerRequired)
	require.Nil(t, signer)

	for _, size := range []int{0, 1, 31, 33, ed25519.PrivateKeySize} {
		t.Run(fmt.Sprintf("seed length %d", size), func(t *testing.T) {
			signer2, err2 := NewTicketSigner(sealer, 1, make([]byte, size))
			require.Error(t, err2)
			require.Nil(t, signer2)
		})
	}

	for _, id := range []byte{0, 1, 255} {
		t.Run(fmt.Sprintf("key ID %d", id), func(t *testing.T) {
			seed := ticketTestKey(t)
			signer2, err2 := NewTicketSigner(sealer, id, seed)
			clear(seed)
			require.NoError(t, err2)
			defer clear(signer2.privateKey)
			require.Equal(t, id, signer2.keyID)
			require.Len(t, signer2.privateKey, ed25519.PrivateKeySize)
		})
	}
}

func TestNewTicketSigner_DoesNotRetainCallerSeed(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)
	seed := ticketTestKey(t)
	expectedPrivate := ed25519.NewKeyFromSeed(seed)
	defer clear(expectedPrivate)
	publicKey := expectedPrivate.Public().(ed25519.PublicKey)

	signer, err := NewTicketSigner(sealer, 7, seed)
	require.NoError(t, err)
	defer clear(signer.privateKey)
	require.Equal(t, expectedPrivate, signer.privateKey)

	clear(seed)
	require.Equal(t, expectedPrivate, signer.privateKey)
	verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{7: publicKey}, 5*time.Second)
	require.NoError(t, err)

	in := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
	defer in.Clear()
	wire, err := signer.Seal(in)
	require.NoError(t, err)

	out, err := verifier.Open(in.DeviceID, wire)
	require.NoError(t, err)
	defer out.Clear()
	require.Equal(t, in, out)
}

func TestTicketSigner_Seal_RejectsNilTicket(t *testing.T) {
	signer, _ := newTicketSignatureTestSigner(t, newTicketTestSealer(t, 1, 1), 7)
	wire, err := signer.Seal(nil)
	require.ErrorIs(t, err, ErrTicketInvalid)
	require.Nil(t, wire)
}

// Literal offsets and the domain below pin the protocol independently of
// the implementation's constants and ticketSigningMessage helper.
func TestTicketSigner_Seal_WireFormatAndSignature(t *testing.T) {
	sealer := newTicketTestSealer(t, 3, 3)
	signer, publicKey := newTicketSignatureTestSigner(t, sealer, 7)
	in := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
	defer in.Clear()
	snapshot := *in

	wire, err := signer.Seal(in)
	require.NoError(t, err)
	require.Equal(t, snapshot, *in, "Seal must not modify the ticket")
	require.Equal(t, 303, SignedTicketSize)
	require.Equal(t, 2, signedTicketHeaderSize)
	require.Equal(t, 239, signedTicketUnsignedSize)
	require.Equal(t, byte(2), signedTicketVersion)
	require.Len(t, wire, 303)
	require.Equal(t, byte(2), wire[0], "envelope version")
	require.Equal(t, byte(7), wire[1], "signing key ID")
	require.Equal(t, byte(3), wire[2], "independent AES key ID")

	opened, err := sealer.Open(in.DeviceID, wire[2:239])
	require.NoError(t, err)
	defer opened.Clear()
	require.Equal(t, in, opened)

	message := append([]byte("xxx/iam/ticket-signature/v2\x00"), wire[:239]...)
	require.True(t, ed25519.Verify(publicKey, message, wire[239:]))
	require.False(t, ed25519.Verify(publicKey, wire[:239], wire[239:]), "a signature without the domain must not verify")
	require.False(t, ed25519.Verify(publicKey, wire[2:239], wire[239:]), "signing just the AES ticket must not verify")
	require.Equal(t, ed25519.Sign(signer.privateKey, message), wire[239:])
}

func TestTicketSigner_Seal_UsesFreshAESNonce(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)
	signer, publicKey := newTicketSignatureTestSigner(t, sealer, 7)
	verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{7: publicKey}, 5*time.Second)
	require.NoError(t, err)
	in := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
	defer in.Clear()

	first, err := signer.Seal(in)
	require.NoError(t, err)
	second, err := signer.Seal(in)
	require.NoError(t, err)
	require.NotEqual(t, first[3:15], second[3:15], "AES nonce must be fresh")
	require.NotEqual(t, first, second)

	for _, wire := range [][]byte{first, second} {
		out, err2 := verifier.Open(in.DeviceID, wire)
		require.NoError(t, err2)
		require.Equal(t, in, out)
		out.Clear()
	}
}

func TestTicketSigningMessage_DomainAndInputOwnership(t *testing.T) {
	for _, size := range []int{0, 1, 2, 239, 256, 512} {
		t.Run(fmt.Sprintf("unsigned length %d", size), func(t *testing.T) {
			unsigned := bytes.Repeat([]byte{0xA5}, size)
			before := bytes.Clone(unsigned)
			expected := append([]byte("xxx/iam/ticket-signature/v2\x00"), unsigned...)
			message := ticketSigningMessage(unsigned)
			defer func() {
				clear(message)
				slicepool.Put(message)
			}()

			require.Equal(t, expected, message, "all logical bytes must be initialized")
			require.Equal(t, before, unsigned)
			clear(unsigned)
			require.Equal(t, expected, message, "message must not alias input")
		})
	}
}
