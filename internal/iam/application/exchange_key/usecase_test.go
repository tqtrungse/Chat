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

package exchange_key_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"xxx/api/iam/v1/json"
	"xxx/internal/iam/application/exchange_key"
	"xxx/internal/iam/application/exchange_key/mocks"
	"xxx/internal/iam/domain/device"
	shareddevice "xxx/internal/shared/device"
	sharedsession "xxx/internal/shared/session"
	sharedticket "xxx/internal/shared/ticket"

	"xxx/pkg/database/sql"
	dbmocks "xxx/pkg/database/sql/mocks"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

const testExchangeDeviceID uint64 = 0x12A

type usecaseTestDeps struct {
	uc      exchange_key.Usecase
	db      *dbmocks.MockDB
	reader  *mocks.MockDeviceMetaReader
	deriver *mocks.MockKeyDeriver
	issuer  *mocks.MockTicketIssuer
}

func newUsecaseTestDeps(t *testing.T) usecaseTestDeps {
	t.Helper()

	ctrl := gomock.NewController(t)
	db := dbmocks.NewMockDB(ctrl)
	reader := mocks.NewMockDeviceMetaReader(ctrl)
	deriver := mocks.NewMockKeyDeriver(ctrl)
	issuer := mocks.NewMockTicketIssuer(ctrl)

	return usecaseTestDeps{
		uc:      exchange_key.New(db, deriver, reader, issuer),
		db:      db,
		reader:  reader,
		deriver: deriver,
		issuer:  issuer,
	}
}

func expectDeviceLookup(
	t *testing.T,
	deps usecaseTestDeps,
	deviceID uint64,
	meta *exchange_key.DeviceMeta,
	findErr error,
) {
	t.Helper()

	deps.db.EXPECT().WithDB(gomock.Any(), sql.TxSourcePrimary, gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ sql.TxSource, fn func(context.Context) error) error {
			return fn(ctx)
		},
	)
	deps.reader.EXPECT().FindDeviceMeta(gomock.Any(), shareddevice.ID(deviceID)).Return(meta, findErr)
}

func validExchangeRequest() json.ExchangeKeyReq {
	clientPubKey := make([]byte, 32)
	for i := range clientPubKey {
		clientPubKey[i] = byte(i + 1)
	}
	return json.ExchangeKeyReq{
		DeviceID:     testExchangeDeviceID,
		ClientPubKey: clientPubKey,
	}
}

func activeDeviceMeta(subject string) *exchange_key.DeviceMeta {
	identityPub := make([]byte, 32)
	for i := range identityPub {
		identityPub[i] = byte(0xA0 + i)
	}
	return &exchange_key.DeviceMeta{
		ExternalID:  subject,
		State:       device.StateActive,
		IdentityPub: identityPub,
	}
}

func derivedKeysForTest() exchange_key.DerivedKeys {
	var keys exchange_key.DerivedKeys
	for i := range 32 {
		keys.Session.RecvEnc[i] = byte(i + 1)
		keys.Session.SendEnc[i] = byte(i + 33)
		keys.Session.RecvMac[i] = byte(i + 65)
		keys.Session.SendMac[i] = byte(i + 97)
		keys.Activation[i] = byte(i + 129)
	}
	return keys
}

func requireKeysEqual(t *testing.T, expected, actual sharedsession.Keys) {
	t.Helper()
	require.Equal(t, expected.RecvEnc, actual.RecvEnc)
	require.Equal(t, expected.SendEnc, actual.SendEnc)
	require.Equal(t, expected.RecvMac, actual.RecvMac)
	require.Equal(t, expected.SendMac, actual.SendMac)
}

func requireKeysCleared(t *testing.T, actual sharedsession.Keys) {
	t.Helper()
	require.Equal(t, [32]byte{}, actual.RecvEnc)
	require.Equal(t, [32]byte{}, actual.SendEnc)
	require.Equal(t, [32]byte{}, actual.RecvMac)
	require.Equal(t, [32]byte{}, actual.SendMac)
}

func TestExchange_Success(t *testing.T) {
	const subject = "auth0|user-123"
	ctx := context.Background()
	deps := newUsecaseTestDeps(t)
	req := validExchangeRequest()
	meta := activeDeviceMeta(subject)
	expectedDerived := derivedKeysForTest()
	var clientPub [32]byte
	copy(clientPub[:], req.ClientPubKey)
	var serverPub [32]byte
	for i := range serverPub {
		serverPub[i] = byte(0xF0 - i)
	}
	sealedTicket := []byte("sealed-ticket-envelope")
	var sealed *sharedticket.Ticket
	startedAt := time.Now()

	expectDeviceLookup(t, deps, req.DeviceID, meta, nil)
	const derivationDomain = "xxx/iam/exchange-key/v1"
	expectedInfo := make([]byte, 0, len(derivationDomain)+8)
	expectedInfo = append(expectedInfo, derivationDomain...)
	expectedInfo = binary.LittleEndian.AppendUint64(expectedInfo, req.DeviceID)
	deps.deriver.EXPECT().DeriveKeys(&clientPub, expectedInfo).Return(expectedDerived, serverPub, nil)
	deps.issuer.EXPECT().Seal(gomock.Any()).DoAndReturn(func(ticket *sharedticket.Ticket) ([]byte, error) {
		sealed = ticket
		require.Equal(t, req.DeviceID, ticket.DeviceID)
		require.GreaterOrEqual(t, ticket.ExpiresAt, startedAt.Add(15*time.Second).Add(-time.Second).Unix())
		require.LessOrEqual(t, ticket.ExpiresAt, time.Now().Add(15*time.Second).Unix())
		requireKeysEqual(t, expectedDerived.Session, ticket.Keys)
		require.Equal(t, expectedDerived.Activation, ticket.ActivationKey)
		require.Equal(t, meta.IdentityPub, ticket.IdentityPub[:])
		return bytes.Clone(sealedTicket), nil
	})

	resp, err := deps.uc.Exchange(ctx, subject, req)

	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, serverPub, resp.ServerPubKey)
	require.Equal(t, sealedTicket, resp.Ticket)
	require.NotNil(t, sealed)
	requireKeysCleared(t, sealed.Keys)
	require.Equal(t, [32]byte{}, sealed.ActivationKey)
}

func TestExchange_RejectsInvalidRequest(t *testing.T) {
	valid := validExchangeRequest()
	tests := []struct {
		name    string
		subject string
		req     json.ExchangeKeyReq
	}{
		{name: "zero device id", subject: "user", req: json.ExchangeKeyReq{ClientPubKey: valid.ClientPubKey}},
		{name: "empty client public key", subject: "user", req: json.ExchangeKeyReq{DeviceID: valid.DeviceID}},
		{name: "short client public key", subject: "user", req: json.ExchangeKeyReq{DeviceID: valid.DeviceID, ClientPubKey: valid.ClientPubKey[:31]}},
		{name: "long client public key", subject: "user", req: json.ExchangeKeyReq{DeviceID: valid.DeviceID, ClientPubKey: append(bytes.Clone(valid.ClientPubKey), 0)}},
		{name: "empty subject", req: valid},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := newUsecaseTestDeps(t)
			resp, err := deps.uc.Exchange(context.Background(), tt.subject, tt.req)
			require.ErrorIs(t, err, exchange_key.ErrRequestInvalid)
			require.Nil(t, resp)
		})
	}
}

func TestExchange_RejectsMissingOrForeignDevice(t *testing.T) {
	const subject = "user-123"
	tests := []struct {
		name string
		meta *exchange_key.DeviceMeta
	}{
		{name: "missing device"},
		{name: "device belongs to another subject", meta: activeDeviceMeta("another-user")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := newUsecaseTestDeps(t)
			req := validExchangeRequest()
			expectDeviceLookup(t, deps, req.DeviceID, tt.meta, nil)

			resp, err := deps.uc.Exchange(context.Background(), subject, req)

			require.ErrorIs(t, err, exchange_key.ErrDeviceNotFound)
			require.Nil(t, resp)
		})
	}
}

func TestExchange_RejectsInactiveDevice(t *testing.T) {
	deps := newUsecaseTestDeps(t)
	req := validExchangeRequest()
	meta := activeDeviceMeta("user-123")
	meta.State = device.StateLocked
	expectDeviceLookup(t, deps, req.DeviceID, meta, nil)

	resp, err := deps.uc.Exchange(context.Background(), "user-123", req)

	require.ErrorIs(t, err, exchange_key.ErrDeviceUnactive)
	require.Nil(t, resp)
}

func TestExchange_PropagatesDatabaseAndReaderErrors(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, usecaseTestDeps, error)
	}{
		{
			name: "database error",
			setup: func(_ *testing.T, deps usecaseTestDeps, wantErr error) {
				deps.db.EXPECT().WithDB(gomock.Any(), sql.TxSourcePrimary, gomock.Any()).Return(wantErr)
			},
		},
		{
			name: "device reader error",
			setup: func(t *testing.T, deps usecaseTestDeps, wantErr error) {
				expectDeviceLookup(t, deps, testExchangeDeviceID, nil, wantErr)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deps := newUsecaseTestDeps(t)
			wantErr := errors.New("dependency failed")
			tt.setup(t, deps, wantErr)

			resp, err := deps.uc.Exchange(context.Background(), "user-123", validExchangeRequest())

			require.ErrorIs(t, err, wantErr)
			require.Nil(t, resp)
		})
	}
}

func TestExchange_PropagatesKeyDerivationError(t *testing.T) {
	deps := newUsecaseTestDeps(t)
	req := validExchangeRequest()
	expectDeviceLookup(t, deps, req.DeviceID, activeDeviceMeta("user-123"), nil)
	wantErr := errors.New("key derivation failed")
	deps.deriver.EXPECT().DeriveKeys(gomock.Any(), gomock.Any()).Return(exchange_key.DerivedKeys{}, [32]byte{}, wantErr)

	resp, err := deps.uc.Exchange(context.Background(), "user-123", req)

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, resp)
}

func TestExchange_PropagatesTicketSealErrorAndClearsKeys(t *testing.T) {
	deps := newUsecaseTestDeps(t)
	req := validExchangeRequest()
	expectedDerived := derivedKeysForTest()
	expectDeviceLookup(t, deps, req.DeviceID, activeDeviceMeta("user-123"), nil)
	deps.deriver.EXPECT().DeriveKeys(gomock.Any(), gomock.Any()).Return(expectedDerived, [32]byte{}, nil)
	wantErr := errors.New("ticket seal failed")
	var sealed *sharedticket.Ticket
	deps.issuer.EXPECT().Seal(gomock.Any()).DoAndReturn(func(ticket *sharedticket.Ticket) ([]byte, error) {
		sealed = ticket
		return nil, wantErr
	})

	resp, err := deps.uc.Exchange(context.Background(), "user-123", req)

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, resp)
	require.NotNil(t, sealed)
	requireKeysCleared(t, sealed.Keys)
	require.Equal(t, [32]byte{}, sealed.ActivationKey)
}
