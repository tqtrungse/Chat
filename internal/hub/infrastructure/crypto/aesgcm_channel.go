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
	"crypto/aes"
	crypto "crypto/cipher"

	"xxx/internal/hub/protocol"
)

const authTagSize = 16

type aesGcmChannel struct{}

func NewAesGcmChannel() protocol.SecureChannel {
	return new(aesGcmChannel)
}

func (a *aesGcmChannel) Seal(key *[32]byte, nonce *[12]byte, plain []byte, aad []byte) ([]byte, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	aead, err := crypto.NewGCM(block)
	if err != nil {
		return nil, err
	}

	cipher := aead.Seal(plain[:0], nonce[:], plain, aad)
	return cipher, nil
}

func (a *aesGcmChannel) Open(key *[32]byte, nonce *[12]byte, cipher []byte, aad []byte) ([]byte, error) {
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	aead, err := crypto.NewGCM(block)
	if err != nil {
		return nil, err
	}

	return aead.Open(cipher[:0], nonce[:], cipher, aad)
}

func (a *aesGcmChannel) Size(plainSize int) int {
	// gcmTagSize = 16
	return plainSize + authTagSize
}

func (a *aesGcmChannel) IsValid(cipherSize int) bool {
	return cipherSize > authTagSize
}
