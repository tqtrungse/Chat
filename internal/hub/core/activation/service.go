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
	"encoding/binary"
	"time"

	sharedevice "xxx/internal/shared/device"
	sharedticket "xxx/internal/shared/ticket"
)

const tokenSize = 8

type Service struct {
	signer         Signer
	ticketVerifier TicketVerifier
}

func NewService(signer Signer, ticketVerifier TicketVerifier) *Service {
	return &Service{
		signer:         signer,
		ticketVerifier: ticketVerifier,
	}
}

// Prepare opens the sealed ticket and verifies the device
// signature, but does not register the connection. The caller must clear the
// returned ticket and prove possession of its activation key before
// activating.
func (s *Service) Prepare(token, ticket, sign []byte) (*sharedticket.Ticket, error) {
	if len(token) != tokenSize {
		return nil, ErrInvalid
	}

	deviceID := binary.LittleEndian.Uint64(token)
	t, err := s.ticketVerifier.Open(sharedevice.ID(deviceID), ticket)
	if err != nil {
		return nil, err
	}

	// The signature authenticates the device identity. Freshness is provided
	// separately by the per-connection challenge-response.
	if err = s.signer.Verify(t.IdentityPub[:], ticket, sign); err != nil {
		t.Clear()
		return nil, err
	}
	return t, nil
}

func (s *Service) CheckExpiry(ticket *sharedticket.Ticket, now time.Time) error {
	return s.ticketVerifier.CheckExpiry(ticket, now)
}
