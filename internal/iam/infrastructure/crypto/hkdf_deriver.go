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

package crypto

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"io"

	"xxx/internal/iam/application/exchange_key"

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
) (derived exchange_key.DerivedKeys, pubKey [32]byte, err error) {
	//Don't need to check error because crypto/rand will panic if it error.
	var privateKey [32]byte
	_, _ = rand.Read(privateKey[:])
	defer clear(privateKey[:])

	pub, err := curve25519.X25519(privateKey[:], curve25519.Basepoint)
	if err != nil {
		return
	}
	copy(pubKey[:], pub)

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

	// transcript = clientPub || serverPub (fixed 64 bytes, unambiguous)
	var salt [64]byte
	copy(salt[:32], peerPub[:])
	copy(salt[32:], pubKey[:])

	prk := hkdf.Extract(sha256.New, rawSecret, salt[:])
	defer clear(prk)

	outputs := []struct {
		label string
		dst   []byte
	}{
		{"c2s/enc", derived.Session.RecvEnc[:]},
		{"s2c/enc", derived.Session.SendEnc[:]},
		{"c2s/mac", derived.Session.RecvMac[:]},
		{"s2c/mac", derived.Session.SendMac[:]},
		{"c2s/act", derived.Activation[:]},
	}

	for _, out := range outputs {
		if _, err = io.ReadFull(
			hkdf.Expand(sha256.New, prk, withLabel(info, out.label)),
			out.dst,
		); err != nil {
			derived = exchange_key.DerivedKeys{}
			return
		}
	}
	return
}

func withLabel(base []byte, label string) []byte {
	info := make([]byte, len(base)+len(label)+1)
	info[len(base)] = '/'
	copy(info, base)
	copy(info[len(base)+1:], label)
	return info
}
