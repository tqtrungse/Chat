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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package protocol

import (
	"crypto/sha256"
	"io"

	sharedsession "xxx/internal/shared/session"

	"golang.org/x/crypto/hkdf"
)

const connKeyDomain = "xxx/hub/conn/v1/"

// DeriveConnKeys creates directional keys bound to one activation challenge.
// The labels and info encoding are part of the wire protocol and must match
// the client implementation exactly.
func DeriveConnKeys(base *sharedsession.Keys, nonce [32]byte) (out sharedsession.Keys, err error) {
	if base == nil {
		return sharedsession.Keys{}, ErrConnKeyBaseNil
	}

	if out.RecvEnc, err = deriveConnKey(&base.RecvEnc, "c2s/enc", nonce); err != nil {
		out.Clear()
		return sharedsession.Keys{}, err
	}
	if out.SendEnc, err = deriveConnKey(&base.SendEnc, "s2c/enc", nonce); err != nil {
		out.Clear()
		return sharedsession.Keys{}, err
	}
	if out.RecvMac, err = deriveConnKey(&base.RecvMac, "c2s/mac", nonce); err != nil {
		out.Clear()
		return sharedsession.Keys{}, err
	}
	if out.SendMac, err = deriveConnKey(&base.SendMac, "s2c/mac", nonce); err != nil {
		out.Clear()
		return sharedsession.Keys{}, err
	}
	return out, nil
}

func deriveConnKey(base *[32]byte, label string, nonce [32]byte) (out [32]byte, err error) {
	info := make([]byte, 0, len(connKeyDomain)+len(label)+1+len(nonce))
	info = append(info, connKeyDomain...)
	info = append(info, label...)
	info = append(info, 0)
	info = append(info, nonce[:]...)
	defer clear(info)

	_, err = io.ReadFull(hkdf.Expand(sha256.New, base[:], info), out[:])
	if err != nil {
		clear(out[:])
	}
	return out, err
}
