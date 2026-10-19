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

package activation

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTranscript_UsesUnambiguousLengthPrefixes(t *testing.T) {
	token, ticket, sign := []byte("ab"), []byte("c"), []byte("d")
	one := Transcript(token, ticket, sign)
	two := Transcript([]byte("a"), []byte("bc"), []byte("d"))

	require.NotEqual(t, one, two)

	// The uint32 field lengths are encoded little-endian.
	encoded := append([]byte("xxx/hub/activate/v1\x00"), 2, 0, 0, 0)
	encoded = append(encoded, token...)
	encoded = append(encoded, 1, 0, 0, 0)
	encoded = append(encoded, ticket...)
	encoded = append(encoded, 1, 0, 0, 0)
	encoded = append(encoded, sign...)
	require.Equal(t, sha256.Sum256(encoded), one)
}

func TestTranscript_BindsEveryFieldWithoutMutatingInputs(t *testing.T) {
	token, ticket, signature := []byte("token"), []byte("ticket"), []byte("signature")
	beforeToken, beforeTicket, beforeSignature := bytes.Clone(token), bytes.Clone(ticket), bytes.Clone(signature)
	original := Transcript(token, ticket, signature)

	require.NotEqual(t, original, Transcript([]byte("other"), ticket, signature))
	require.NotEqual(t, original, Transcript(token, []byte("other"), signature))
	require.NotEqual(t, original, Transcript(token, ticket, []byte("other")))
	require.NotEqual(t, original, Transcript(ticket, token, signature))
	require.Equal(t, original, Transcript(token, ticket, signature))
	require.Equal(t, beforeToken, token)
	require.Equal(t, beforeTicket, ticket)
	require.Equal(t, beforeSignature, signature)
}

func TestTranscript_EmptyFields(t *testing.T) {
	nilFields := Transcript(nil, nil, nil)
	emptyFields := Transcript([]byte{}, []byte{}, []byte{})
	require.Equal(t, nilFields, emptyFields)

	// Even empty fields contribute their three uint32 little-endian lengths.
	encoded := append([]byte("xxx/hub/activate/v1\x00"), make([]byte, 12)...)
	require.Equal(t, sha256.Sum256(encoded), nilFields)
}

func TestProof_BindsKeyTranscriptAndNonce(t *testing.T) {
	var keyA, keyB, nonceA, nonceB [32]byte
	for i := range keyA {
		keyA[i] = byte(i + 1)
		keyB[i] = byte(i + 2)
		nonceA[i] = byte(i + 3)
		nonceB[i] = byte(i + 4)
	}
	transcriptA := Transcript([]byte("token"), []byte("ticket"), []byte("signature"))
	transcriptB := Transcript([]byte("other"), []byte("ticket"), []byte("signature"))

	proof := Proof(&keyA, transcriptA, nonceA)
	sameProof := Proof(&keyA, transcriptA, nonceA)
	wrongKeyProof := Proof(&keyB, transcriptA, nonceA)
	wrongTranscriptProof := Proof(&keyA, transcriptB, nonceA)
	wrongNonceProof := Proof(&keyA, transcriptA, nonceB)
	require.Len(t, proof, 32)
	require.True(t, hmac.Equal(proof[:], sameProof[:]))
	require.False(t, hmac.Equal(proof[:], wrongKeyProof[:]))
	require.False(t, hmac.Equal(proof[:], wrongTranscriptProof[:]))
	require.False(t, hmac.Equal(proof[:], wrongNonceProof[:]))

	// Pin the domain and field order against an independent HMAC assembly.
	mac := hmac.New(sha256.New, keyA[:])
	_, _ = mac.Write([]byte("xxx/hub/activate-proof/v1\x00"))
	_, _ = mac.Write(transcriptA[:])
	_, _ = mac.Write(nonceA[:])
	require.Equal(t, mac.Sum(nil), proof[:])
}

func TestProof_DoesNotMutateInputs(t *testing.T) {
	var key, transcript, nonce [32]byte
	for i := range key {
		key[i], transcript[i], nonce[i] = byte(i+1), byte(i+2), byte(i+3)
	}
	beforeKey, beforeTranscript, beforeNonce := key, transcript, nonce
	_ = Proof(&key, transcript, nonce)
	require.Equal(t, beforeKey, key)
	require.Equal(t, beforeTranscript, transcript)
	require.Equal(t, beforeNonce, nonce)
}
