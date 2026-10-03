/*
 * Copyright 2026 tqtrungse@gmail.com. All rights reserved.
 * Use of this pub code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package protocol

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
)

// mockTagSize is the authentication tag length produced by
// mockSecureChannel.
const mockTagSize = 16

// mockSecureChannel is a minimal SecureChannel test double: an XOR
// keystream for "encryption" plus an HMAC-SHA256 tag over
// (nonce || ciphertext || aad) for authentication. It is not real
// encryption -- it exists only so tests in package protocol can exercise
// Encoder/Decoder's error paths (tamper detection, wrong key, cipher-size
// validation) without depending on a real SecureChannel implementation.
//
// Seal and Open reuse plain's/cipher's storage for their output, exactly
// as SecureChannel documents -- Encoder.EncodeS relies on that in-place
// contract to avoid a second allocation (it pre-sizes its buffer via
// Size and expects Seal to extend the plaintext slice into ciphertext
// within that same backing array, not return an unrelated one).
type mockSecureChannel struct{}

func NewMockSecureChannel() SecureChannel {
	return mockSecureChannel{}
}

// keystream derives n pseudo-random bytes from key and nonce by
// concatenating successive HMAC-SHA256 blocks over (nonce || counter).
func (mockSecureChannel) keystream(key *[32]byte, nonce *[12]byte, n int) []byte {
	ks := make([]byte, 0, n+sha256.Size)
	for counter := 0; len(ks) < n; counter++ {
		h := hmac.New(sha256.New, key[:])
		h.Write(nonce[:])
		h.Write([]byte{byte(counter)})
		ks = h.Sum(ks)
	}
	return ks[:n]
}

// tag authenticates the ciphertext together with the associated data, so
// tampering either one is detectable, mirroring a real AEAD tag.
func (m mockSecureChannel) tag(key *[32]byte, nonce *[12]byte, cipher []byte, aad []byte) []byte {
	h := hmac.New(sha256.New, key[:])
	h.Write(nonce[:])
	h.Write(cipher)
	h.Write(aad)
	return h.Sum(nil)[:mockTagSize]
}

func (m mockSecureChannel) Seal(
	key *[32]byte,
	nonce *[12]byte,
	plain []byte,
	aad []byte,
) ([]byte, error) {
	// Encrypt in place: plain becomes the ciphertext.
	ks := m.keystream(key, nonce, len(plain))
	for i := range plain {
		plain[i] ^= ks[i]
	}
	// append onto plain's own backing array -- callers that sized plain's
	// capacity via Size (as Encoder.EncodeS does) get the tag written
	// right after the ciphertext with no new allocation.
	return append(plain, m.tag(key, nonce, plain, aad)...), nil
}

func (m mockSecureChannel) Open(
	key *[32]byte,
	nonce *[12]byte,
	cipher []byte,
	aad []byte,
) ([]byte, error) {
	if len(cipher) < mockTagSize {
		return nil, errors.New("secureChannelMock: ciphertext shorter than tag")
	}

	ctLen := len(cipher) - mockTagSize
	ct, gotTag := cipher[:ctLen], cipher[ctLen:]
	if !hmac.Equal(gotTag, m.tag(key, nonce, ct, aad)) {
		return nil, errors.New("secureChannelMock: authentication failed")
	}

	// Decrypt in place: ct (a sub-slice of cipher) becomes the plaintext.
	ks := m.keystream(key, nonce, ctLen)
	for i := range ct {
		ct[i] ^= ks[i]
	}
	return ct, nil
}

func (mockSecureChannel) Size(plainSize int) int {
	return plainSize + mockTagSize
}

func (mockSecureChannel) IsValid(cipherSize int) bool {
	return cipherSize >= mockTagSize
}
