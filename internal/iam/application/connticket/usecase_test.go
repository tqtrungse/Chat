/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connticket_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	api "xxx/api/iam/v2/json"
	"xxx/internal/iam/application/connticket"
	"xxx/internal/iam/application/connticket/mocks"
	"xxx/internal/shared/device"
	"xxx/internal/shared/hubendpoint"
	"xxx/internal/shared/ticket"
	"xxx/internal/shared/ticket/tickettest"

	"xxx/pkg/database/sql"
	dbmocks "xxx/pkg/database/sql/mocks"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestIssueAuthorizesDeviceAndSignsMatchingDescriptor(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	db := dbmocks.NewMockDB(ctrl)
	reader := mocks.NewMockDeviceMetaReader(ctrl)
	signer := mocks.NewMockTicketSigner(ctrl)
	pub := tickettest.HubPublic()
	hub := hubendpoint.Descriptor{
		Address:   "hub.example.com:9000",
		Audience:  "hub.chat",
		KeyID:     7,
		PublicKey: bytes.Clone(pub[:]),
		Protocol:  hubendpoint.Protocol,
		Version:   hubendpoint.WireVersion,
	}
	uc, err := connticket.New(db, reader, signer, hub)
	require.NoError(t, err)
	clear(hub.PublicKey[:cap(hub.PublicKey)])

	marker := new(int)
	dbCtx := context.WithValue(t.Context(), marker, "db")
	lookupErr := errors.New("database unavailable")
	signErr := errors.New("sign failed")

	type testCase struct {
		name       string
		want       error
		setupMocks func(*dbmocks.MockDB, *mocks.MockDeviceMetaReader, *mocks.MockTicketSigner)
	}

	expectLookup := func(db *dbmocks.MockDB, reader *mocks.MockDeviceMetaReader, meta *connticket.DeviceMeta, lookupErr error) {
		db.
			EXPECT().
			WithDB(
				gomock.Any(),
				sql.TxSourcePrimary,
				gomock.Any(),
			).
			DoAndReturn(
				func(_ context.Context, _ sql.TxSource, fn func(context.Context) error) error {
					return fn(dbCtx)
				},
			)

		reader.
			EXPECT().
			FindDeviceMeta(
				dbCtx,
				device.ID(42),
			).
			Return(
				meta,
				lookupErr,
			)
	}

	expectSign := func(signer *mocks.MockTicketSigner, signErr error) {
		signer.
			EXPECT().
			Sign(gomock.Any()).
			DoAndReturn(
				func(v *ticket.Info) ([]byte, error) {
					require.Equal(t, pub, v.HubPublicKey)
					require.Equal(t, device.ID(42), v.DeviceID)
					require.Equal(t, tickettest.New().IdentityPub, v.IdentityPub)
					require.Equal(t, hub.Audience, v.Audience)
					require.Equal(t, hub.KeyID, v.HubKeyID)
					require.Equal(t, hub.Version, v.WireVersion)
					require.NotEqual(t, [ticket.IDSize]byte{}, v.TicketID)
					require.Positive(t, v.IssuedAt)
					require.Equal(t, int64(ticket.MaxTTL/time.Second), v.ExpiresAt-v.IssuedAt)
					return []byte("signed"), signErr
				},
			)
	}

	activeDevice := func() *connticket.DeviceMeta {
		return &connticket.DeviceMeta{
			ExternalID:  "user",
			State:       device.StateActive,
			IdentityPub: tickettest.New().IdentityPub[:],
		}
	}

	tcs := []testCase{
		{
			name: "valid",
			setupMocks: func(db *dbmocks.MockDB, reader *mocks.MockDeviceMetaReader, signer *mocks.MockTicketSigner) {
				expectLookup(db, reader, activeDevice(), nil)
				expectSign(signer, nil)
			},
		},
		{
			name: "not found", want: connticket.ErrDeviceNotFound,
			setupMocks: func(db *dbmocks.MockDB, reader *mocks.MockDeviceMetaReader, _ *mocks.MockTicketSigner) {
				expectLookup(db, reader, nil, nil)
			},
		},
		{
			name: "not owner", want: connticket.ErrDeviceNotFound,
			setupMocks: func(db *dbmocks.MockDB, reader *mocks.MockDeviceMetaReader, _ *mocks.MockTicketSigner) {
				meta := activeDevice()
				meta.ExternalID = "other"
				expectLookup(db, reader, meta, nil)
			},
		},
		{
			name: "inactive", want: connticket.ErrDeviceUnactive,
			setupMocks: func(db *dbmocks.MockDB, reader *mocks.MockDeviceMetaReader, _ *mocks.MockTicketSigner) {
				meta := activeDevice()
				meta.State = 0
				expectLookup(db, reader, meta, nil)
			},
		},
		{
			name: "invalid identity", want: connticket.ErrIdentityPubInvalid,
			setupMocks: func(db *dbmocks.MockDB, reader *mocks.MockDeviceMetaReader, _ *mocks.MockTicketSigner) {
				meta := activeDevice()
				meta.IdentityPub = nil
				expectLookup(db, reader, meta, nil)
			},
		},
		{
			name: "DB error", want: lookupErr,
			setupMocks: func(db *dbmocks.MockDB, reader *mocks.MockDeviceMetaReader, _ *mocks.MockTicketSigner) {
				expectLookup(db, reader, nil, lookupErr)
			},
		},
		{
			name: "signer error", want: signErr,
			setupMocks: func(db *dbmocks.MockDB, reader *mocks.MockDeviceMetaReader, signer *mocks.MockTicketSigner) {
				expectLookup(db, reader, activeDevice(), nil)
				expectSign(signer, signErr)
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setupMocks != nil {
				tc.setupMocks(db, reader, signer)
			}

			resp, err := uc.Issue(t.Context(), "user", api.ConnectionTicketReq{DeviceID: "42"})
			if tc.want != nil {
				require.ErrorIs(t, err, tc.want)
				require.Nil(t, resp)
				return
			}

			require.NoError(t, err)
			require.Equal(t, []byte("signed"), resp.Ticket)
			require.Equal(t, pub[:], resp.Hub.PublicKey)
		})
	}
}

func TestIssueRejectsInvalidRequestBeforeDB(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	db := dbmocks.NewMockDB(ctrl)
	reader := mocks.NewMockDeviceMetaReader(ctrl)
	signer := mocks.NewMockTicketSigner(ctrl)
	pub := tickettest.HubPublic()
	hub := hubendpoint.Descriptor{
		Address:   "hub.example.com:9000",
		Audience:  "hub.chat",
		KeyID:     7,
		PublicKey: pub[:],
		Protocol:  hubendpoint.Protocol,
		Version:   hubendpoint.WireVersion,
	}
	uc, err := connticket.New(db, reader, signer, hub)
	require.NoError(t, err)

	type testCase struct {
		name     string
		subject  string
		deviceID string
	}
	tcs := []testCase{
		{name: "empty device ID", subject: "user"},
		{name: "zero device ID", subject: "user", deviceID: "0"},
		{name: "negative device ID", subject: "user", deviceID: "-1"},
		{name: "non-numeric device ID", subject: "user", deviceID: "x"},
		{name: "overflow device ID", subject: "user", deviceID: "18446744073709551616"},
		{name: "empty subject", deviceID: "42"},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := uc.Issue(
				t.Context(),
				tc.subject,
				api.ConnectionTicketReq{
					DeviceID: tc.deviceID,
				},
			)
			require.ErrorIs(t, err, connticket.ErrRequestInvalid)
			require.Nil(t, resp)
		})
	}
}

func TestNewRejectsInvalidDescriptor(t *testing.T) {
	_, err := connticket.New(
		nil,
		nil,
		nil,
		hubendpoint.Descriptor{},
	)
	require.Error(t, err)
}
