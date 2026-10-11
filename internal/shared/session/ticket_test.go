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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTicket_Clear_ErasesAllKeyMaterial(t *testing.T) {
	ticket := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
	deviceID, expiresAt := ticket.DeviceID, ticket.ExpiresAt
	ticket.Clear()

	require.Equal(t, Keys{}, ticket.Keys)
	require.Equal(t, [32]byte{}, ticket.ActivationKey)
	require.Equal(t, [32]byte{}, ticket.IdentityPub)
	require.Equal(t, deviceID, ticket.DeviceID)
	require.Equal(t, expiresAt, ticket.ExpiresAt)

	// Repeated cleanup and cleanup of a zero ticket must be safe.
	require.NotPanics(t, ticket.Clear)
	require.NotPanics(t, (&Ticket{}).Clear)
}

func TestTicket_Clear_NilReceiver(t *testing.T) {
	var ticket *Ticket
	require.NotPanics(t, ticket.Clear)
}

func TestTicket_Clear_DoesNotEraseCopiedSessionKeys(t *testing.T) {
	ticket := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
	copiedKeys := ticket.Keys
	expectedKeys := copiedKeys
	require.NotEqual(t, Keys{}, copiedKeys)

	ticket.Clear()
	require.Equal(t, Keys{}, ticket.Keys)
	require.Equal(t, expectedKeys, copiedKeys,
		"a session owns the directional keys copied by value")
	clear(copiedKeys.RecvEnc[:])
	clear(copiedKeys.SendEnc[:])
	clear(copiedKeys.RecvMac[:])
	clear(copiedKeys.SendMac[:])
}
