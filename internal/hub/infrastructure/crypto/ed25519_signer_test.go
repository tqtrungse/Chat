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
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/require"
)

func genEd25519KeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	return pub, priv
}

func TestEd25519Signature_Verify_Valid(t *testing.T) {
	pub, priv := genEd25519KeyPair(t)
	payload := []byte("hello, device")
	sig := ed25519.Sign(priv, payload)

	v := NewEd25519Signer()
	err := v.Verify(pub, payload, sig)
	require.NoError(t, err)
}

func TestEd25519Signature_Verify_TamperedPayload(t *testing.T) {
	pub, priv := genEd25519KeyPair(t)
	payload := []byte("hello, device")
	sig := ed25519.Sign(priv, payload)

	v := NewEd25519Signer()
	err := v.Verify(pub, []byte("hello, devicE"), sig)
	require.Error(t, err)
}

func TestEd25519Signature_Verify_TamperedSignature(t *testing.T) {
	pub, priv := genEd25519KeyPair(t)
	payload := []byte("hello, device")
	sig := ed25519.Sign(priv, payload)
	tampered := append([]byte(nil), sig...)
	tampered[0] ^= 0xFF

	v := NewEd25519Signer()
	err := v.Verify(pub, payload, tampered)
	require.Error(t, err)
}

func TestEd25519Signature_Verify_WrongKey(t *testing.T) {
	_, priv := genEd25519KeyPair(t)
	otherPub, _ := genEd25519KeyPair(t)
	payload := []byte("hello, device")
	sig := ed25519.Sign(priv, payload)

	v := NewEd25519Signer()
	err := v.Verify(otherPub, payload, sig)
	require.Error(t, err)
}

func TestEd25519Signature_Verify_EmptyPayload(t *testing.T) {
	pub, priv := genEd25519KeyPair(t)
	payload := []byte{}
	sig := ed25519.Sign(priv, payload)

	v := NewEd25519Signer()
	err := v.Verify(pub, payload, sig)
	require.NoError(t, err)
}

func TestEd25519Signature_Verify_SignatureFromDifferentPayload(t *testing.T) {
	pub, priv := genEd25519KeyPair(t)
	sigForA := ed25519.Sign(priv, []byte("payload-A"))

	v := NewEd25519Signer()
	err := v.Verify(pub, []byte("payload-B"), sigForA)
	require.Error(t, err)
}

// TestEd25519Signature_Verify_MalformedSignatureLengthReturnsError checks
// the companion case: unlike a malformed public key, ed25519.Verify does
// NOT panic on a wrong-length signature -- it returns false, which this
// wrapper correctly surfaces as an error.
func TestEd25519Signature_Verify_MalformedSignatureLengthReturnsError(t *testing.T) {
	pub, _ := genEd25519KeyPair(t)
	v := NewEd25519Signer()

	shortSig := []byte("not-a-valid-signature")
	err := v.Verify(pub, []byte("payload"), shortSig)
	require.Error(t, err)
}

// TestEd25519Signature_Verify_MalformedPublicKeyLength is the companion
// gap to the test above: unlike a wrong-length signature, ed25519.Verify
// PANICS on a wrong-length public key rather than returning false. This
// asserts the wrapper itself must not let that panic escape -- Verify
// should turn it into an ordinary error, same as every other verification
// failure in this file.
//
// If Verify doesn't already guard against this (a length check, or a
// recover, before calling ed25519.Verify), this test fails by panicking
// rather than by a normal assertion failure -- that panic trace IS the
// finding.
func TestEd25519Signature_Verify_MalformedPublicKeyLength(t *testing.T) {
	v := NewEd25519Signer()
	payload := []byte("payload")
	sig := make([]byte, ed25519.SignatureSize) // valid length; contents don't matter here

	tests := []struct {
		name string
		size int
	}{
		{"empty", 0},
		{"far too short", 10},
		{"one byte short", ed25519.PublicKeySize - 1},
		{"one byte long", ed25519.PublicKeySize + 1},
		{"far too long", 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			badPub := make(ed25519.PublicKey, tt.size)
			require.NotPanics(t, func() {
				err := v.Verify(badPub, payload, sig)
				require.Error(t, err)
			})
		})
	}
}
