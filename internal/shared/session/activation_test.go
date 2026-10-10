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
	"crypto/hmac"
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActivationTranscriptUsesUnambiguousLittleEndianLengthPrefixes(t *testing.T) {
	token, ticket, sign := []byte("ab"), []byte("c"), []byte("d")
	one := ActivationTranscript(token, ticket, sign)
	two := ActivationTranscript([]byte("a"), []byte("bc"), []byte("d"))

	require.NotEqual(t, one, two)

	// The uint32 field lengths are encoded little-endian.
	encoded := append([]byte(activationTranscriptDomain), 2, 0, 0, 0)
	encoded = append(encoded, token...)
	encoded = append(encoded, 1, 0, 0, 0)
	encoded = append(encoded, ticket...)
	encoded = append(encoded, 1, 0, 0, 0)
	encoded = append(encoded, sign...)
	require.Equal(t, sha256.Sum256(encoded), one)
}

func TestActivationProofBindsKeyTranscriptAndNonce(t *testing.T) {
	var keyA, keyB, nonceA, nonceB [32]byte
	for i := range keyA {
		keyA[i] = byte(i + 1)
		keyB[i] = byte(i + 2)
		nonceA[i] = byte(i + 3)
		nonceB[i] = byte(i + 4)
	}
	transcriptA := ActivationTranscript([]byte("token"), []byte("ticket"), []byte("signature"))
	transcriptB := ActivationTranscript([]byte("other"), []byte("ticket"), []byte("signature"))

	proof := ActivationProof(&keyA, transcriptA, nonceA)
	sameProof := ActivationProof(&keyA, transcriptA, nonceA)
	wrongKeyProof := ActivationProof(&keyB, transcriptA, nonceA)
	wrongTranscriptProof := ActivationProof(&keyA, transcriptB, nonceA)
	wrongNonceProof := ActivationProof(&keyA, transcriptA, nonceB)
	require.Len(t, proof, 32)
	require.True(t, hmac.Equal(proof[:], sameProof[:]))
	require.False(t, hmac.Equal(proof[:], wrongKeyProof[:]))
	require.False(t, hmac.Equal(proof[:], wrongTranscriptProof[:]))
	require.False(t, hmac.Equal(proof[:], wrongNonceProof[:]))
}
