/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
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
	signer := NewEd25519Signer()
	require.NoError(t, signer.Verify(pub, payload, sig))
}

func TestEd25519Signature_Verify_TamperedPayload(t *testing.T) {
	pub, priv := genEd25519KeyPair(t)
	payload := []byte("hello, device")
	sig := ed25519.Sign(priv, payload)
	signer := NewEd25519Signer()
	require.Error(t, signer.Verify(pub, []byte("hello, devicE"), sig))
}

func TestEd25519Signature_Verify_TamperedSignature(t *testing.T) {
	pub, priv := genEd25519KeyPair(t)
	payload := []byte("hello, device")
	sig := ed25519.Sign(priv, payload)
	tampered := append([]byte(nil), sig...)
	tampered[0] ^= 0xFF
	signer := NewEd25519Signer()
	require.Error(t, signer.Verify(pub, payload, tampered))
}

func TestEd25519Signature_Verify_WrongKey(t *testing.T) {
	_, priv := genEd25519KeyPair(t)
	otherPub, _ := genEd25519KeyPair(t)
	payload := []byte("hello, device")
	sig := ed25519.Sign(priv, payload)
	signer := NewEd25519Signer()
	require.Error(t, signer.Verify(otherPub, payload, sig))
}

func TestEd25519Signature_Verify_EmptyPayload(t *testing.T) {
	var payload []byte
	pub, priv := genEd25519KeyPair(t)
	sig := ed25519.Sign(priv, payload)
	signer := NewEd25519Signer()
	require.NoError(t, signer.Verify(pub, payload, sig))
}

func TestEd25519Signature_Verify_SignatureFromDifferentPayload(t *testing.T) {
	pub, priv := genEd25519KeyPair(t)
	sigForA := ed25519.Sign(priv, []byte("payload-A"))
	signer := NewEd25519Signer()
	require.Error(t, signer.Verify(pub, []byte("payload-B"), sigForA))
}

// TestEd25519Signature_Verify_MalformedSignatureLengthReturnsError checks that a
// wrong-length signature returns an error instead of panicking.
func TestEd25519Signature_Verify_MalformedSignatureLengthReturnsError(t *testing.T) {
	pub, _ := genEd25519KeyPair(t)
	signer := NewEd25519Signer()
	shortSig := []byte("not-a-valid-signature")
	require.Error(t, signer.Verify(pub, []byte("payload"), shortSig))
}

// TestEd25519Signature_Verify_MalformedPublicKeyLength is the companion
// gap to the test above: unlike a wrong-length signature, ed25519.Verify
// PANICS on a wrong-length public key rather than returning false. This
// asserts the wrapper itself must not let that panic escape -- Verify
// should turn it into an ordinary error, same as every other verification
// failure in this file.
//
// Unlike a wrong-length signature, ed25519.Verify panics on a wrong-length public key.
// Verify guards against it with a length check and returns ErrSignerInvalidPubKey.
func TestEd25519Signature_Verify_MalformedPublicKeyLength(t *testing.T) {
	signer := NewEd25519Signer()
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
		t.Run(
			tt.name,
			func(t *testing.T) {
				badPub := make(ed25519.PublicKey, tt.size)
				require.NotPanics(
					t,
					func() {
						err := signer.Verify(badPub, payload, sig)
						require.Error(t, err)
					},
				)
			},
		)
	}
}
