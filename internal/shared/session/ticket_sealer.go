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
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	ticketPlainSize = 8 + 8 + 6*32                        // 208
	ticketHeader    = 1 + 12                              // keyID | nonce
	ticketSize      = ticketHeader + ticketPlainSize + 16 // 237
	ticketMaxSkew   = 5 * time.Second
)

var (
	ErrTicketInvalid = errors.New("invalid ticket")
	ErrTicketExpired = errors.New("ticket expired")
)

type TicketSealer struct {
	curID byte
	aeads map[byte]cipher.AEAD // read-only after construction
}

// NewTicketSealer keys maps keyID -> 32-byte AES key. Keep the previous key in
// the map while rotating so in-flight tickets (TTL ~20s) still open.
func NewTicketSealer(curID byte, keys map[byte][]byte) (*TicketSealer, error) {
	s := &TicketSealer{curID: curID, aeads: make(map[byte]cipher.AEAD, len(keys))}
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

// ParseTicketKeys parses entries like "1:<base64 32 bytes>".
func ParseTicketKeys(entries []string) (map[byte][]byte, error) {
	out := make(map[byte][]byte, len(entries))
	for _, e := range entries {
		idStr, b64, ok := strings.Cut(strings.TrimSpace(e), ":")
		if !ok {
			return nil, fmt.Errorf("bad ticket key entry")
		}
		id, err := strconv.ParseUint(idStr, 10, 8)
		if err != nil {
			return nil, err
		}
		k, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return nil, err
		}
		out[byte(id)] = k
	}
	return out, nil
}

func (s *TicketSealer) Seal(t *Ticket) ([]byte, error) {
	var pt [ticketPlainSize]byte
	marshalTicket(pt[:], t)
	defer clear(pt[:])

	out := make([]byte, ticketHeader, ticketSize)
	out[0] = s.curID
	if _, err := rand.Read(out[1:ticketHeader]); err != nil {
		return nil, err
	}
	return s.aeads[s.curID].Seal(out, out[1:ticketHeader], pt[:], ticketAAD(t.DeviceID)), nil
}

func (s *TicketSealer) Open(deviceID uint64, ticket []byte) (*Ticket, error) {
	if len(ticket) != ticketSize {
		return nil, ErrTicketInvalid
	}
	aead, ok := s.aeads[ticket[0]]
	if !ok {
		return nil, ErrTicketInvalid
	}
	// dst=nil: never mutates the caller's buffer (it aliases the nio read buffer).
	pt, err := aead.Open(nil, ticket[1:ticketHeader], ticket[ticketHeader:], ticketAAD(deviceID))
	if err != nil {
		return nil, ErrTicketInvalid
	}
	defer clear(pt)

	t := unmarshalTicket(pt)
	if t.DeviceID != deviceID {
		t.Clear()
		return nil, ErrTicketInvalid
	}
	if time.Now().Unix() > t.ExpiresAt+int64(ticketMaxSkew.Seconds()) {
		t.Clear()
		return nil, ErrTicketExpired
	}
	return t, nil
}

func ticketAAD(deviceID uint64) []byte {
	return binary.LittleEndian.AppendUint64(nil, deviceID)
}

func marshalTicket(b []byte, t *Ticket) {
	binary.LittleEndian.PutUint64(b[0:8], t.DeviceID)
	binary.LittleEndian.PutUint64(b[8:16], uint64(t.ExpiresAt))

	copy(b[16:48], t.Keys.RecvEnc[:])
	copy(b[48:80], t.Keys.SendEnc[:])
	copy(b[80:112], t.Keys.RecvMac[:])
	copy(b[112:144], t.Keys.SendMac[:])
	copy(b[144:176], t.ActivationKey[:])
	copy(b[176:208], t.IdentityPub[:])
}

func unmarshalTicket(b []byte) *Ticket {
	t := &Ticket{
		DeviceID:  binary.LittleEndian.Uint64(b[0:]),
		ExpiresAt: int64(binary.LittleEndian.Uint64(b[8:])),
	}
	copy(t.Keys.RecvEnc[:], b[16:48])
	copy(t.Keys.SendEnc[:], b[48:80])
	copy(t.Keys.RecvMac[:], b[80:112])
	copy(t.Keys.SendMac[:], b[112:144])
	copy(t.ActivationKey[:], b[144:176])
	copy(t.IdentityPub[:], b[176:208])
	return t
}
