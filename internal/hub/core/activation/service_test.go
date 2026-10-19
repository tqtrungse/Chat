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
	"errors"
	"testing"
	"time"

	"xxx/internal/hub/core/activation/mocks"
	shareddevice "xxx/internal/shared/device"
	sharedticket "xxx/internal/shared/ticket"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

const testDeviceID shareddevice.ID = 0x12A

func testTicket(deviceID shareddevice.ID) *sharedticket.Ticket {
	ticket := &sharedticket.Ticket{
		DeviceID:  deviceID,
		ExpiresAt: time.Now().Add(time.Minute).Unix(),
	}
	for i := range ticket.Keys.RecvEnc {
		ticket.Keys.RecvEnc[i] = byte(i + 1)
		ticket.Keys.SendEnc[i] = byte(i + 2)
		ticket.Keys.RecvMac[i] = byte(i + 3)
		ticket.Keys.SendMac[i] = byte(i + 4)
		ticket.ActivationKey[i] = byte(i + 5)
		ticket.IdentityPub[i] = byte(i + 6)
	}
	return ticket
}

func newTestToken(deviceID shareddevice.ID) []byte {
	token := make([]byte, tokenSize)
	binary.LittleEndian.PutUint64(token, deviceID.Uint64())
	return token
}

type testEnv struct {
	svc      *Service
	signer   *mocks.MockSigner
	verifier *mocks.MockTicketVerifier
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	ctrl := gomock.NewController(t)
	signer := mocks.NewMockSigner(ctrl)
	verifier := mocks.NewMockTicketVerifier(ctrl)
	return &testEnv{
		svc:      NewService(signer, verifier),
		signer:   signer,
		verifier: verifier,
	}
}

func TestActivation_PrepareValidatesTokenBeforeOpeningTicket(t *testing.T) {
	env := newTestEnv(t)
	got, err := env.svc.Prepare(
		[]byte{1},
		[]byte("ticket"),
		[]byte("sign"),
	)

	require.ErrorIs(t, err, ErrInvalid)
	require.Nil(t, got)
}

func TestActivation_PrepareOpensAndVerifiesTicket(t *testing.T) {
	env := newTestEnv(t)
	ticket := testTicket(testDeviceID)
	ticketBlob := []byte("ticket envelope")
	signature := []byte("device signature")

	env.verifier.
		EXPECT().
		Open(
			testDeviceID,
			gomock.Eq(ticketBlob),
		).
		Return(ticket, nil).
		Times(1)

	env.signer.
		EXPECT().
		Verify(
			gomock.Eq(ticket.IdentityPub[:]),
			gomock.Eq(ticketBlob), // The signed payload is the ticket blob
			gomock.Eq(signature),
		).
		Return(nil).
		Times(1)

	got, err := env.svc.Prepare(
		newTestToken(testDeviceID),
		ticketBlob,
		signature,
	)

	require.NoError(t, err)
	require.Same(t, ticket, got)
	require.NotZero(t, got.Keys, "the ticket returned to the caller must still have the original key.")
}

func TestActivation_PrepareClearsTicketWhenSignatureFails(t *testing.T) {
	env := newTestEnv(t)
	wantErr := errors.New("bad device signature")
	ticket := testTicket(testDeviceID)
	ticketBlob := []byte("ticket")

	env.verifier.
		EXPECT().
		Open(
			testDeviceID,
			gomock.Eq(ticketBlob),
		).
		Return(ticket, nil).
		Times(1)

	env.signer.
		EXPECT().
		Verify(
			gomock.Any(),
			gomock.Eq(ticketBlob),
			gomock.Eq([]byte("bad")),
		).
		Return(wantErr).
		Times(1)

	got, err := env.svc.Prepare(
		newTestToken(testDeviceID),
		ticketBlob,
		[]byte("bad"),
	)

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, got)
	require.Zero(t, ticket.Keys)
	require.Zero(t, ticket.ActivationKey)
	require.Zero(t, ticket.IdentityPub)
	require.Equal(t, testDeviceID, ticket.DeviceID)
	require.NotZero(t, ticket.ExpiresAt)
}
