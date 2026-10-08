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
	"crypto/subtle"
	"fmt"
	"io"

	"xxx/internal/hub/application/exchange_key"

	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/hkdf"
)

var zero [32]byte

type hkdfDeriver struct{}

func NewHkdfDeriver() exchange_key.KeyDeriver {
	return new(hkdfDeriver)
}

func (hd *hkdfDeriver) DeriveKeys(
	peerPub *[32]byte,
	info []byte,
) (
	secretKey [32]byte,
	hmacKey [32]byte,
	pubKey [32]byte,
	err error,
) {
	// Implement of rand.Read will panic if there is any error.
	// So we don't need check error.
	var privateKey [32]byte
	_, _ = rand.Read(privateKey[:])
	defer clear(privateKey[:])

	pub, err := curve25519.X25519(privateKey[:], curve25519.Basepoint)
	if err != nil {
		return
	}
	copy(pubKey[:], pub)

	// PrivateKey, PublicKey, RawSecret and SecretKey are 32 bytes.
	rawSecret, err := curve25519.X25519(privateKey[:], peerPub[:])
	if err != nil {
		err = fmt.Errorf("%w: %v", exchange_key.ErrRequestInvalid, err)
		return
	}
	defer clear(rawSecret)

	if subtle.ConstantTimeCompare(rawSecret, zero[:]) == 1 {
		err = exchange_key.ErrRequestInvalid
		return
	}

	secretKey, err = derive32(rawSecret, withLabel(info, "encryption"))
	if err == nil {
		hmacKey, err = derive32(rawSecret, withLabel(info, "packet-hmac"))
	}
	return
}

func derive32(secret, info []byte) (key [32]byte, err error) {
	_, err = io.ReadFull(
		hkdf.New(sha256.New, secret, nil, info),
		key[:],
	)
	return key, err
}

func withLabel(base []byte, label string) []byte {
	info := make([]byte, len(base)+len(label)+1)
	info[len(base)] = '/'
	copy(info, base)
	copy(info[len(base)+1:], label)
	return info
}
