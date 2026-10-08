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
	"crypto/rand"
	"crypto/sha256"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"
)

// genX25519KeyPair generates a random X25519 private/public key pair,
// standing in for the "peer" side of a handshake in tests.
func genX25519KeyPair(t *testing.T) (priv, pub [32]byte) {
	t.Helper()

	_, err := rand.Read(priv[:])
	require.NoError(t, err)

	pubBytes, err := curve25519.X25519(priv[:], curve25519.Basepoint)
	require.NoError(t, err)
	copy(pub[:], pubBytes)
	return
}

// deriveExpected independently derives one labeled key for the peer-side
// assertions below. The labels are part of the exchange-key derivation
// contract and must match the client implementation.
func deriveExpected(t *testing.T, rawSecret, baseInfo []byte, label string) [32]byte {
	t.Helper()

	info := make([]byte, 0, len(baseInfo)+1+len(label))
	info = append(info, baseInfo...)
	info = append(info, '/')
	info = append(info, label...)

	var key [32]byte
	_, err := io.ReadFull(hkdf.New(sha256.New, rawSecret, nil, info), key[:])
	require.NoError(t, err)
	return key
}

// TestHkdfDeriver_DeriveKeys_MatchesManualECDH is the core correctness
// test: it verifies the full ECDH handshake round-trips. The peer
// independently recomputes the shared secret using its own private key and
// the ephemeral public key DeriveKeys returned, then derives a key via
// HKDF-SHA256 exactly as the implementation does, and checks it matches.
func TestHkdfDeriver_DeriveKeys_MatchesManualECDH(t *testing.T) {
	deriver := NewHkdfDeriver()

	peerPriv, peerPub := genX25519KeyPair(t)
	info := []byte("session-info")

	secretKey, hmacKey, ephemeralPub, err := deriver.DeriveKeys(&peerPub, info)
	require.NoError(t, err)

	// The peer computes the same shared secret from its own private key
	// and the ephemeral public key returned by DeriveKeys.
	rawSecret, err := curve25519.X25519(peerPriv[:], ephemeralPub[:])
	require.NoError(t, err)

	expectedEncryptionKey := deriveExpected(t, rawSecret, info, "encryption")
	expectedHmacKey := deriveExpected(t, rawSecret, info, "packet-hmac")
	require.Equal(t, expectedEncryptionKey, secretKey)
	require.Equal(t, expectedHmacKey, hmacKey)
	require.NotEqual(t, secretKey, hmacKey)
}

func TestHkdfDeriver_DeriveKeys_ReturnsValidEphemeralPublicKey(t *testing.T) {
	deriver := NewHkdfDeriver()
	_, peerPub := genX25519KeyPair(t)

	_, _, ephemeralPub, err := deriver.DeriveKeys(&peerPub, []byte("info"))
	require.NoError(t, err)

	var zero [32]byte
	require.False(t, bytes.Equal(ephemeralPub[:], zero[:]))
}

func TestHkdfDeriver_DeriveKeys_ProducesFreshEphemeralKeyEachCall(t *testing.T) {
	deriver := NewHkdfDeriver()
	_, peerPub := genX25519KeyPair(t)
	info := []byte("session-info")

	secret1, hmac1, pub1, err := deriver.DeriveKeys(&peerPub, info)
	require.NoError(t, err)

	secret2, hmac2, pub2, err := deriver.DeriveKeys(&peerPub, info)
	require.NoError(t, err)
	require.False(t, bytes.Equal(pub1[:], pub2[:]))
	require.False(t, bytes.Equal(secret1[:], secret2[:]))
	require.False(t, bytes.Equal(hmac1[:], hmac2[:]))
}

func TestHkdfDeriver_DeriveKeys_DifferentInfoYieldsDifferentKeys(t *testing.T) {
	deriver := NewHkdfDeriver()
	peerPriv, peerPub := genX25519KeyPair(t)

	// DeriveKeys generates a fresh ephemeral key on every call (see
	// ProducesFreshEphemeralKeyEachCall above), so calling it twice with
	// just different info would also change the output for that reason
	// alone -- it wouldn't isolate whether info specifically matters.
	// Instead: derive the raw ECDH secret once, the same way
	// MatchesManualECDH does, and hold it fixed while varying only info.
	_, _, ephemeralPub, err := deriver.DeriveKeys(&peerPub, []byte("context-A"))
	require.NoError(t, err)

	rawSecret, err := curve25519.X25519(peerPriv[:], ephemeralPub[:])
	require.NoError(t, err)

	keyA := deriveExpected(t, rawSecret, []byte("context-A"), "encryption")
	keyB := deriveExpected(t, rawSecret, []byte("context-B"), "encryption")
	require.False(t, bytes.Equal(keyA[:], keyB[:]))

	// Ties this back to the real implementation: MatchesManualECDH
	// already confirms DeriveKeys' actual output equals this same manual
	// HKDF(rawSecret, info) computation, so keyA differing from keyB here
	// means swapping info would genuinely have changed DeriveKeys' output
	// too -- info isn't being silently ignored.
}

func TestHkdfDeriver_DeriveKeys_NilInfoIsAccepted(t *testing.T) {
	deriver := NewHkdfDeriver()
	_, peerPub := genX25519KeyPair(t)

	_, _, _, err := deriver.DeriveKeys(&peerPub, nil)
	require.NoError(t, err)
}

// TestHkdfDeriver_DeriveKeys_RejectsLowOrderPeerKey checks the low-order
// point guard. The all-zero point is a well-known low-order point on
// Curve25519 (X25519(anything, 0) == 0), so it must be rejected rather than
// silently producing an insecure, attacker-predictable shared secret.
//
// Note: golang.org/x/crypto/curve25519's X25519 already returns an error
// itself for known low-order inputs, so this may be caught before ever
// reaching the explicit subtle.ConstantTimeCompare check in DeriveKeys --
// either path is acceptable, the test only asserts that some error occurs.
func TestHkdfDeriver_DeriveKeys_RejectsLowOrderPeerKey(t *testing.T) {
	deriver := NewHkdfDeriver()
	var lowOrderPub [32]byte // all-zero

	_, _, _, err := deriver.DeriveKeys(&lowOrderPub, []byte("info"))
	require.Error(t, err)
}

// TestHkdfDeriver_DeriveKeys_SecretIsBoundToIntendedPeer replaces what was
// previously named DifferentPeersYieldDifferentSecrets. That version
// called DeriveKeys twice, once per peer, and compared the two results --
// but since DeriveKeys mints a fresh ephemeral key on every call (see
// ProducesFreshEphemeralKeyEachCall), the two secrets would already have
// differed for that reason alone, regardless of whether the peer key was
// actually mixed into the derivation correctly. Same confound as the info
// test above, just on the other axis.
//
// This version holds the ephemeral key fixed instead -- there's only ONE
// DeriveKeys call, so only one ephemeralPub -- and has two different
// peers independently recompute the shared secret from that same
// ephemeralPub. The intended peer (A) must land on DeriveKeys' actual
// output; a different peer (B), holding a different private key, must
// not. This isolates the peer-key's contribution without needing any
// internal access to DeriveKeys' ephemeral private key.
func TestHkdfDeriver_DeriveKeys_SecretIsBoundToIntendedPeer(t *testing.T) {
	deriver := NewHkdfDeriver()
	peerAPriv, peerAPub := genX25519KeyPair(t)
	peerBPriv, _ := genX25519KeyPair(t)
	info := []byte("info")

	secretA, hmacA, ephemeralPub, err := deriver.DeriveKeys(&peerAPub, info)
	require.NoError(t, err)

	deriveAs := func(priv [32]byte, label string) [32]byte {
		raw, err := curve25519.X25519(priv[:], ephemeralPub[:])
		require.NoError(t, err)
		return deriveExpected(t, raw, info, label)
	}

	// The intended recipient (peer A, whose public key was passed to
	// DeriveKeys) recomputes the exact same secret DeriveKeys returned --
	// this mirrors MatchesManualECDH.
	wantEncryptionA := deriveAs(peerAPriv, "encryption")
	wantHmacA := deriveAs(peerAPriv, "packet-hmac")
	require.Equal(t, wantEncryptionA, secretA)
	require.Equal(t, wantHmacA, hmacA)

	// Peer B, holding a different private key, gets a different secret
	// from that same ephemeral public key -- the shared secret is
	// genuinely bound to the intended recipient's private key, not just
	// to the ephemeral key or the plaintext info string.
	secretB := deriveAs(peerBPriv, "encryption")
	hmacB := deriveAs(peerBPriv, "packet-hmac")
	require.NotEqual(t, secretA, secretB)
	require.NotEqual(t, hmacA, hmacB)
}
