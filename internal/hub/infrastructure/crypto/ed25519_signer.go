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
	"crypto/ed25519"
	"errors"

	"xxx/internal/hub/connection"
)

var (
	ErrSignerInvalidPubKey = errors.New("invalid public key length")
	ErrSignerVerify        = errors.New("signature verification failed")
)

type ed25519Signer struct{}

func NewEd25519Signer() connection.Signer {
	return new(ed25519Signer)
}

func (v *ed25519Signer) Verify(identityPub, payload, sign []byte) error {
	if len(identityPub) != ed25519.PublicKeySize {
		return ErrSignerInvalidPubKey
	}
	if !ed25519.Verify(identityPub, payload, sign) {
		return ErrSignerVerify
	}
	return nil
}
