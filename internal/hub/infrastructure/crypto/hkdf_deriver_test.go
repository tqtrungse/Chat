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
	"crypto/rand"
	"crypto/sha256"
	"io"
	"testing"

	"xxx/internal/hub/application/exchange_key"
	"xxx/internal/hub/domain/session"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"
)

// genX25519KeyPair generates a random X25519 private/public key pair,
// standing in for the "peer" (client) side of a handshake in tests.
func genX25519KeyPair(t *testing.T) (priv, pub [32]byte) {
	t.Helper()

	_, err := rand.Read(priv[:])
	require.NoError(t, err)

	pubBytes, err := curve25519.X25519(priv[:], curve25519.Basepoint)
	require.NoError(t, err)
	copy(pub[:], pubBytes)
	return
}

// transcriptSalt rebuilds the HKDF salt DeriveKeys uses:
// clientPub || serverPub (fixed 64 bytes). From DeriveKeys' point of view
// the peer is the client and the freshly generated ephemeral key is the
// server's.
func transcriptSalt(clientPub, serverPub [32]byte) []byte {
	salt := make([]byte, 0, 64)
	salt = append(salt, clientPub[:]...)
	salt = append(salt, serverPub[:]...)
	return salt
}

// deriveExpected independently derives one labeled key for the peer-side
// assertions below: HKDF-Extract(rawSecret, salt) followed by
// HKDF-Expand(prk, baseInfo + "/" + label). The labels are part of the
// exchange-key derivation contract and must match the client implementation.
func deriveExpected(t *testing.T, rawSecret, salt, baseInfo []byte, label string) [32]byte {
	t.Helper()

	info := make([]byte, 0, len(baseInfo)+1+len(label))
	info = append(info, baseInfo...)
	info = append(info, '/')
	info = append(info, label...)

	prk := hkdf.Extract(sha256.New, rawSecret, salt)

	var key [32]byte
	_, err := io.ReadFull(hkdf.Expand(sha256.New, prk, info), key[:])
	require.NoError(t, err)
	return key
}

// deriveExpectedKeys derives the full session.Keys set the way the server
// side is expected to hold it. Labels are directional: "c2s" (client to
// server) is what the server receives, "s2c" is what it sends.
func deriveExpectedKeys(t *testing.T, rawSecret, salt, info []byte) session.Keys {
	t.Helper()

	return session.Keys{
		RecvEnc: deriveExpected(t, rawSecret, salt, info, "c2s/enc"),
		SendEnc: deriveExpected(t, rawSecret, salt, info, "s2c/enc"),
		RecvMac: deriveExpected(t, rawSecret, salt, info, "c2s/mac"),
		SendMac: deriveExpected(t, rawSecret, salt, info, "s2c/mac"),
	}
}

type namedKey struct {
	name string
	key  [32]byte
}

func namedKeys(k session.Keys) []namedKey {
	return []namedKey{
		{"RecvEnc", k.RecvEnc},
		{"SendEnc", k.SendEnc},
		{"RecvMac", k.RecvMac},
		{"SendMac", k.SendMac},
	}
}

// requireAllDistinct asserts that no two of the four derived keys are equal
// (the per-direction / per-purpose labels must actually separate them).
func requireAllDistinct(t *testing.T, k session.Keys) {
	t.Helper()

	nk := namedKeys(k)
	for i := range nk {
		for j := i + 1; j < len(nk); j++ {
			require.NotEqual(t, nk[i].key, nk[j].key, "%s and %s must differ", nk[i].name, nk[j].name)
		}
	}
}

// requireEachDiffers asserts that every key in a differs from its
// counterpart (same field) in b.
func requireEachDiffers(t *testing.T, a, b session.Keys) {
	t.Helper()

	na, nb := namedKeys(a), namedKeys(b)
	for i := range na {
		require.NotEqual(t, na[i].key, nb[i].key, "%s must differ", na[i].name)
	}
}

// TestHkdfDeriver_DeriveKeys_MatchesManualECDH is the core correctness
// test: it verifies the full ECDH handshake round-trips. The peer
// independently recomputes the shared secret using its own private key and
// the ephemeral public key DeriveKeys returned, rebuilds the transcript salt
// (clientPub || serverPub), then derives all four keys via HKDF-SHA256
// exactly as the implementation does, and checks they match.
//
// This test is also what pins the salt layout and the label -> field
// mapping (c2s -> Recv*, s2c -> Send*): if the implementation dropped the
// salt, swapped its order, or crossed two labels, the expected keys here
// would no longer match.
func TestHkdfDeriver_DeriveKeys_MatchesManualECDH(t *testing.T) {
	deriver := NewHkdfDeriver()

	peerPriv, peerPub := genX25519KeyPair(t)
	info := []byte("session-info")

	keys, ephemeralPub, err := deriver.DeriveKeys(&peerPub, info)
	require.NoError(t, err)

	// The peer computes the same shared secret from its own private key
	// and the ephemeral public key returned by DeriveKeys.
	rawSecret, err := curve25519.X25519(peerPriv[:], ephemeralPub[:])
	require.NoError(t, err)

	salt := transcriptSalt(peerPub, ephemeralPub)
	require.Equal(t, deriveExpectedKeys(t, rawSecret, salt, info), keys)
	requireAllDistinct(t, keys)
}

func TestHkdfDeriver_DeriveKeys_ReturnsValidEphemeralPublicKey(t *testing.T) {
	deriver := NewHkdfDeriver()
	peerPriv, peerPub := genX25519KeyPair(t)

	_, ephemeralPub, err := deriver.DeriveKeys(&peerPub, []byte("info"))
	require.NoError(t, err)
	require.NotEqual(t, [32]byte{}, ephemeralPub)

	// The peer must be able to complete ECDH with it.
	shared, err := curve25519.X25519(peerPriv[:], ephemeralPub[:])
	require.NoError(t, err)
	require.NotEqual(t, make([]byte, 32), shared)
}

func TestHkdfDeriver_DeriveKeys_ProducesFreshEphemeralKeyEachCall(t *testing.T) {
	deriver := NewHkdfDeriver()
	_, peerPub := genX25519KeyPair(t)
	info := []byte("session-info")

	keys1, pub1, err := deriver.DeriveKeys(&peerPub, info)
	require.NoError(t, err)

	keys2, pub2, err := deriver.DeriveKeys(&peerPub, info)
	require.NoError(t, err)

	require.NotEqual(t, pub1, pub2)
	requireEachDiffers(t, keys1, keys2)
}

func TestHkdfDeriver_DeriveKeys_DifferentInfoYieldsDifferentKeys(t *testing.T) {
	deriver := NewHkdfDeriver()
	peerPriv, peerPub := genX25519KeyPair(t)

	// DeriveKeys generates a fresh ephemeral key on every call (see
	// ProducesFreshEphemeralKeyEachCall above), so calling it twice with
	// just different info would also change the output for that reason
	// alone -- it wouldn't isolate whether info specifically matters.
	// Instead: derive the raw ECDH secret and transcript salt once, the same
	// way MatchesManualECDH does, and hold both fixed while varying only info.
	keys, ephemeralPub, err := deriver.DeriveKeys(&peerPub, []byte("context-A"))
	require.NoError(t, err)

	rawSecret, err := curve25519.X25519(peerPriv[:], ephemeralPub[:])
	require.NoError(t, err)
	salt := transcriptSalt(peerPub, ephemeralPub)

	keyA := deriveExpected(t, rawSecret, salt, []byte("context-A"), "c2s/enc")
	keyB := deriveExpected(t, rawSecret, salt, []byte("context-B"), "c2s/enc")
	require.NotEqual(t, keyA, keyB)

	// Ties this back to the real implementation: DeriveKeys' actual output
	// is the context-A derivation, so keyA differing from keyB means
	// swapping info would genuinely have changed DeriveKeys' output too --
	// info isn't being silently ignored.
	require.Equal(t, keyA, keys.RecvEnc)
}

func TestHkdfDeriver_DeriveKeys_NilInfoIsAccepted(t *testing.T) {
	deriver := NewHkdfDeriver()
	_, peerPub := genX25519KeyPair(t)

	_, _, err := deriver.DeriveKeys(&peerPub, nil)
	require.NoError(t, err)
}

// TestHkdfDeriver_DeriveKeys_RejectsLowOrderPeerKey checks the low-order
// point guard. The all-zero point is a well-known low-order point on
// Curve25519 (X25519(anything, 0) == 0), so it must be rejected rather than
// silently producing an insecure, attacker-predictable shared secret.
//
// Note: golang.org/x/crypto/curve25519's X25519 already returns an error
// itself for known low-order inputs, so this may be caught before ever
// reaching the explicit subtle.ConstantTimeCompare check in DeriveKeys.
// Both paths surface as exchange_key.ErrRequestInvalid, so the test accepts
// either, and also checks that no key material leaks out on failure.
func TestHkdfDeriver_DeriveKeys_RejectsLowOrderPeerKey(t *testing.T) {
	deriver := NewHkdfDeriver()
	var lowOrderPub [32]byte // all-zero

	keys, _, err := deriver.DeriveKeys(&lowOrderPub, []byte("info"))
	require.ErrorIs(t, err, exchange_key.ErrRequestInvalid)
	require.Equal(t, session.Keys{}, keys)
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
// not. The transcript salt is also held fixed (A's), so the only thing
// that differs between A and B is the ECDH secret itself. This isolates the
// peer-key's contribution without needing any internal access to
// DeriveKeys' ephemeral private key.
func TestHkdfDeriver_DeriveKeys_SecretIsBoundToIntendedPeer(t *testing.T) {
	deriver := NewHkdfDeriver()
	peerAPriv, peerAPub := genX25519KeyPair(t)
	peerBPriv, _ := genX25519KeyPair(t)
	info := []byte("info")

	keys, ephemeralPub, err := deriver.DeriveKeys(&peerAPub, info)
	require.NoError(t, err)

	salt := transcriptSalt(peerAPub, ephemeralPub)
	deriveAs := func(priv [32]byte) session.Keys {
		raw, err := curve25519.X25519(priv[:], ephemeralPub[:])
		require.NoError(t, err)
		return deriveExpectedKeys(t, raw, salt, info)
	}

	// The intended recipient (peer A, whose public key was passed to
	// DeriveKeys) recomputes the exact same keys DeriveKeys returned --
	// this mirrors MatchesManualECDH.
	require.Equal(t, deriveAs(peerAPriv), keys)

	// Peer B, holding a different private key, gets different keys from
	// that same ephemeral public key -- the shared secret is genuinely
	// bound to the intended recipient's private key, not just to the
	// ephemeral key, the salt, or the plaintext info string.
	requireEachDiffers(t, keys, deriveAs(peerBPriv))
}
