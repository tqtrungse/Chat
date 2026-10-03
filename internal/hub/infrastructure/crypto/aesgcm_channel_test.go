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
	"testing"

	"github.com/stretchr/testify/require"
)

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()

	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

func newKeyNonce(t *testing.T) (key [32]byte, nonce [12]byte) {
	t.Helper()

	copy(key[:], randomBytes(t, 32))
	copy(nonce[:], randomBytes(t, 12))
	return
}

func TestAesGcmChannel_SealOpen_RoundTrip(t *testing.T) {
	ch := NewAesGcmChannel()
	key, nonce := newKeyNonce(t)

	plain := []byte("the quick brown fox jumps over the lazy dog")
	aad := []byte("associated-data")

	// Seal writes into plain[:0] as its destination buffer, so it mutates
	// the caller's backing array. Keep an independent copy to compare
	// against after decryption.
	original := append([]byte(nil), plain...)

	cipher, err := ch.Seal(
		&key,
		&nonce,
		plain,
		aad,
	)
	require.NoError(t, err)
	require.Equal(t, len(original)+16, len(cipher))

	// Open similarly writes into cipher[:0], so decrypt from a fresh copy
	// rather than the buffer Seal already produced.
	cipherCopy := append([]byte(nil), cipher...)
	opened, err := ch.Open(
		&key,
		&nonce,
		cipherCopy,
		aad,
	)
	require.NoError(t, err)
	require.True(t, bytes.Equal(opened, original))
}

func TestAesGcmChannel_Seal_EmptyPlaintext(t *testing.T) {
	ch := NewAesGcmChannel()
	key, nonce := newKeyNonce(t)

	cipher, err := ch.Seal(
		&key,
		&nonce,
		[]byte{},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, cipher, 16)

	plain, err := ch.Open(
		&key,
		&nonce,
		cipher,
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, plain)
}

func TestAesGcmChannel_Open_WrongKey(t *testing.T) {
	ch := NewAesGcmChannel()
	key, nonce := newKeyNonce(t)
	wrongKey, _ := newKeyNonce(t)
	plain := []byte("secret message")

	cipher, err := ch.Seal(
		&key,
		&nonce,
		append([]byte(nil), plain...),
		nil,
	)
	require.NoError(t, err)

	_, err = ch.Open(
		&wrongKey,
		&nonce,
		append([]byte(nil), cipher...),
		nil,
	)
	require.Error(t, err)
}

func TestAesGcmChannel_Open_WrongNonce(t *testing.T) {
	ch := NewAesGcmChannel()
	key, nonce := newKeyNonce(t)
	_, wrongNonce := newKeyNonce(t)

	plain := []byte("secret message")
	cipher, err := ch.Seal(
		&key,
		&nonce,
		append([]byte(nil), plain...),
		nil,
	)
	require.NoError(t, err)

	_, err = ch.Open(
		&key,
		&wrongNonce,
		append([]byte(nil), cipher...),
		nil,
	)
	require.Error(t, err)
}

func TestAesGcmChannel_Open_TamperedCiphertext(t *testing.T) {
	ch := NewAesGcmChannel()
	key, nonce := newKeyNonce(t)

	plain := []byte("secret message")
	cipher, err := ch.Seal(
		&key,
		&nonce,
		append([]byte(nil), plain...),
		nil,
	)
	require.NoError(t, err)

	tampered := append([]byte(nil), cipher...)
	tampered[0] ^= 0xFF

	_, err = ch.Open(
		&key,
		&nonce,
		tampered,
		nil,
	)
	require.Error(t, err)
}

func TestAesGcmChannel_Open_TamperedTag(t *testing.T) {
	ch := NewAesGcmChannel()
	key, nonce := newKeyNonce(t)

	plain := []byte("secret message")
	cipher, err := ch.Seal(
		&key,
		&nonce,
		append([]byte(nil), plain...),
		nil,
	)
	require.NoError(t, err)

	tampered := append([]byte(nil), cipher...)
	tampered[len(tampered)-1] ^= 0xFF // flip a bit in the GCM tag itself

	_, err = ch.Open(
		&key,
		&nonce,
		tampered,
		nil,
	)
	require.Error(t, err)
}

func TestAesGcmChannel_Open_WrongAAD(t *testing.T) {
	ch := NewAesGcmChannel()
	key, nonce := newKeyNonce(t)

	plain := []byte("secret message")
	cipher, err := ch.Seal(
		&key,
		&nonce,
		append([]byte(nil), plain...),
		[]byte("aad-1"),
	)
	require.NoError(t, err)

	_, err = ch.Open(
		&key,
		&nonce,
		append([]byte(nil), cipher...),
		[]byte("aad-2"),
	)
	require.Error(t, err)
}

func TestAesGcmChannel_Open_TruncatedCiphertext(t *testing.T) {
	ch := NewAesGcmChannel()
	key, nonce := newKeyNonce(t)

	plain := []byte("secret message")
	cipher, err := ch.Seal(
		&key,
		&nonce,
		append([]byte(nil), plain...),
		nil,
	)
	require.NoError(t, err)

	truncated := cipher[:len(cipher)-1]
	_, err = ch.Open(
		&key,
		&nonce,
		truncated,
		nil,
	)
	require.Error(t, err)
}

func TestAesGcmChannel_Size(t *testing.T) {
	ch := NewAesGcmChannel()
	cases := []int{0, 1, 16, 1024, 65535}
	for _, plainSize := range cases {
		got := ch.Size(plainSize)
		want := plainSize + 16
		require.Equal(t, want, got)
	}
}

func TestAesGcmChannel_IsValid(t *testing.T) {
	ch := NewAesGcmChannel()
	cases := []struct {
		size int
		want bool
	}{
		{0, false},
		{15, false},
		{16, false}, // boundary: must be strictly greater than the tag size
		{17, true},
		{1024, true},
	}
	for _, c := range cases {
		got := ch.IsValid(c.size)
		require.Equal(t, c.want, got)
	}
}

// TestAesGcmChannel_DifferentNoncesProduceDifferentCiphertext guards against
// nonce-reuse-style regressions: the same key+plaintext must yield different
// ciphertext under different nonces.
func TestAesGcmChannel_DifferentNoncesProduceDifferentCiphertext(t *testing.T) {
	ch := NewAesGcmChannel()
	key, nonce1 := newKeyNonce(t)
	_, nonce2 := newKeyNonce(t)

	plain := []byte("same plaintext every time")

	c1, err := ch.Seal(
		&key,
		&nonce1,
		append([]byte(nil), plain...),
		nil,
	)
	require.NoError(t, err)

	c2, err := ch.Seal(
		&key,
		&nonce2,
		append([]byte(nil), plain...),
		nil,
	)
	require.NoError(t, err)
	require.False(t, bytes.Equal(c1, c2))
}
