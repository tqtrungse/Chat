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

package protocol

import (
	"crypto/aes"
	"crypto/cipher"
	"testing"

	"xxx/internal/hub/protocol/mocks"

	"go.uber.org/mock/gomock"
)

// newSecureChannelHelperTest configures the generated mock with AES-GCM behavior so
// protocol tests can exercise framing and AAD while keeping SecureChannel as
// an injected dependency.
func newSecureChannelHelperTest(t *testing.T) *mocks.MockSecureChannel {
	t.Helper()

	const authTagSize = 16 // AES-GCM authentication tag size.

	ch := mocks.NewMockSecureChannel(gomock.NewController(t))
	ch.EXPECT().
		Size(gomock.Any()).
		AnyTimes().
		DoAndReturn(
			func(plainSize int) int {
				return plainSize + authTagSize
			},
		)
	ch.EXPECT().
		IsValid(gomock.Any()).
		AnyTimes().
		DoAndReturn(
			func(cipherSize int) bool {
				return cipherSize > authTagSize
			},
		)
	ch.EXPECT().
		Seal(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		AnyTimes().
		DoAndReturn(
			func(key *[32]byte, nonce *[12]byte, plain, aad []byte) ([]byte, error) {
				aead, err := testAEAD(key)
				if err != nil {
					return nil, err
				}
				// SecureChannel.Seal promises to reuse the plaintext backing array.
				return aead.Seal(plain[:0], nonce[:], plain, aad), nil
			},
		)
	ch.EXPECT().
		Open(
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).
		AnyTimes().
		DoAndReturn(
			func(key *[32]byte, nonce *[12]byte, ciphertext, aad []byte) ([]byte, error) {
				aead, err := testAEAD(key)
				if err != nil {
					return nil, err
				}
				// Match the production channel's in-place decryption behavior too.
				return aead.Open(ciphertext[:0], nonce[:], ciphertext, aad)
			},
		)
	return ch
}

func testAEAD(key *[32]byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
