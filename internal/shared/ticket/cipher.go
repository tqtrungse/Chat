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

package ticket

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
)

const (
	plainSize = 8 + 8 + 6*32            // 208
	header    = 1 + 12                  // keyID | nonce
	size      = header + plainSize + 16 // 237
)

type Sealer struct {
	curID byte
	aeads map[byte]cipher.AEAD // read-only after construction
}

// NewSealer keys maps keyID -> 32-byte AES key. Keep the previous key in
// the map while rotating so in-flight tickets (TTL ~20s) still open.
func NewSealer(curID byte, keys map[byte][]byte) (*Sealer, error) {
	s := &Sealer{
		curID: curID,
		aeads: make(map[byte]cipher.AEAD, len(keys)),
	}

	for id, k := range keys {
		if len(k) != 32 {
			return nil, fmt.Errorf("ticket key %d: want 32 bytes, got %d", id, len(k))
		}

		block, err := aes.NewCipher(k)
		if err != nil {
			return nil, err
		}

		if s.aeads[id], err = cipher.NewGCM(block); err != nil {
			return nil, err
		}
	}

	if _, ok := s.aeads[curID]; !ok {
		return nil, fmt.Errorf("ticket current key %d not configured", curID)
	}

	return s, nil
}

func (s *Sealer) Seal(t *Ticket) ([]byte, error) {
	if t == nil {
		return nil, ErrTicketInvalid
	}

	var pt [plainSize]byte
	marshal(pt[:], t)
	defer clear(pt[:])

	out := make([]byte, header, size)
	out[0] = s.curID
	if _, err := rand.Read(out[1:header]); err != nil {
		return nil, err
	}
	return s.aeads[s.curID].Seal(out, out[1:header], pt[:], aad(t.DeviceID)), nil
}

func (s *Sealer) Open(deviceID uint64, ticket []byte) (*Ticket, error) {
	if len(ticket) != size {
		return nil, ErrTicketInvalid
	}

	aead, ok := s.aeads[ticket[0]]
	if !ok {
		return nil, ErrTicketInvalid
	}

	// dst=nil: never mutates the caller's buffer (it aliases the nio read buffer).
	pt, err := aead.Open(nil, ticket[1:header], ticket[header:], aad(deviceID))
	if err != nil {
		return nil, ErrTicketInvalid
	}
	defer clear(pt)

	t := unmarshal(pt)
	if t.DeviceID != deviceID {
		t.Clear()
		return nil, ErrTicketInvalid
	}
	return t, nil
}
