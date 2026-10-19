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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"hash"
)

const (
	transcriptDomain = "xxx/hub/activate/v1\x00"
	proofDomain      = "xxx/hub/activate-proof/v1\x00"
)

// Transcript binds the proof to the activation request fields.
// Lengths use little-endian uint32 to match the rest of this protocol.
func Transcript(token, ticket, sign []byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte(transcriptDomain))
	writeField(h, token)
	writeField(h, ticket)
	writeField(h, sign)

	var transcript [32]byte
	copy(transcript[:], h.Sum(nil))
	return transcript
}

// Proof computes a connection-bound proof. The caller must keep the
// nonce fresh and accept it only once on the connection that issued it.
func Proof(key *[32]byte, transcript, nonce [32]byte) [32]byte {
	mac := hmac.New(sha256.New, key[:])
	_, _ = mac.Write([]byte(proofDomain))
	_, _ = mac.Write(transcript[:])
	_, _ = mac.Write(nonce[:])

	var proof [32]byte
	copy(proof[:], mac.Sum(nil))
	return proof
}

func writeField(h hash.Hash, field []byte) {
	var size [4]byte
	binary.LittleEndian.PutUint32(size[:], uint32(len(field)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(field)
}
