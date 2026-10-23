/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package activation_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"xxx/internal/hub/core/activation"
	"xxx/internal/hub/core/activation/mocks"
	"xxx/internal/hub/core/auth"
	"xxx/internal/hub/core/transport"
	"xxx/internal/shared/ticket"
	"xxx/internal/shared/ticket/tickettest"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestVerifyAuthorization(t *testing.T) {
	verifierErr := errors.New("ticket rejected")
	signatureErr := errors.New("proof rejected")

	type fixture struct {
		claims    *ticket.Info
		raw       []byte
		signature []byte
		binding   []byte
		challenge []byte
		message   []byte
		hub       transport.Identity
	}

	expectTicket := func(verifier *mocks.MockTicketVerifier, f *fixture) *gomock.Call {
		return verifier.
			EXPECT().
			Verify(f.raw, f.hub).
			Return(f.claims, nil)
	}

	expectProof := func(signer *mocks.MockSigner, f *fixture, err error) *gomock.Call {
		return signer.
			EXPECT().
			Verify(
				f.claims.IdentityPub[:],
				f.message,
				f.signature,
			).
			Return(err)
	}

	invalidProofSetup := func(verifier *mocks.MockTicketVerifier, _ *mocks.MockSigner, f *fixture) {
		expectTicket(verifier, f)
	}

	type testCase struct {
		name       string
		want       error
		mutate     func(*fixture)
		setupMocks func(*mocks.MockTicketVerifier, *mocks.MockSigner, *fixture)
	}

	tcs := []testCase{
		{
			name: "valid",
			setupMocks: func(verifier *mocks.MockTicketVerifier, signer *mocks.MockSigner, f *fixture) {
				gomock.InOrder(
					expectTicket(verifier, f),
					expectProof(signer, f, nil),
				)
			},
		},
		{
			name: "verifier error",
			want: verifierErr,
			setupMocks: func(verifier *mocks.MockTicketVerifier, _ *mocks.MockSigner, f *fixture) {
				verifier.
					EXPECT().
					Verify(f.raw, f.hub).
					Return(nil, verifierErr)
			},
		},
		{
			name:   "invalid binding",
			want:   auth.ErrDeviceProofInvalid,
			mutate: func(f *fixture) { f.binding = nil }, setupMocks: invalidProofSetup,
		},
		{
			name:   "invalid challenge",
			want:   auth.ErrDeviceProofInvalid,
			mutate: func(f *fixture) { f.challenge = nil }, setupMocks: invalidProofSetup,
		},
		{
			name: "empty ticket", want: auth.ErrDeviceProofInvalid,
			mutate: func(f *fixture) { f.raw = nil }, setupMocks: invalidProofSetup,
		},
		{
			name: "signature error",
			want: signatureErr,
			setupMocks: func(verifier *mocks.MockTicketVerifier, signer *mocks.MockSigner, f *fixture) {
				gomock.InOrder(
					expectTicket(verifier, f),
					expectProof(signer, f, signatureErr),
				)
			},
		},
	}

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	verifier := mocks.NewMockTicketVerifier(ctrl)
	signer := mocks.NewMockSigner(ctrl)
	svc := activation.NewService(signer, verifier)

	for _, tc := range tcs {
		t.Run(
			tc.name,
			func(t *testing.T) {
				f := fixture{
					claims:    tickettest.New(),
					binding:   bytes.Repeat([]byte{1}, auth.ChannelBindingSize),
					challenge: bytes.Repeat([]byte{2}, auth.ChallengeSize),
				}
				f.raw, f.signature = tickettest.Authorization()
				f.hub = transport.Identity{
					Audience: f.claims.Audience, KeyID: f.claims.HubKeyID,
					PublicKey: f.claims.HubPublicKey, Version: f.claims.WireVersion,
				}

				// Use the core proof contract as the expected input to the signer port.
				message, err := auth.SigningMessage(f.binding, f.raw, f.challenge)
				t.Cleanup(func() {
					clear(message[:cap(message)])
					slicepool.Put(message)
				})
				require.NoError(t, err)
				f.message = message

				if tc.mutate != nil {
					tc.mutate(&f)
				}
				tc.setupMocks(verifier, signer, &f)

				out, err := svc.VerifyAuthorization(
					f.raw,
					f.signature,
					f.binding,
					f.challenge,
					f.hub,
				)
				if tc.want != nil {
					require.ErrorIs(t, err, tc.want)
					if tc.name == "signature error" {
						require.ErrorIs(t, err, auth.ErrDeviceProofInvalid)
					}
					require.Nil(t, out)
					return
				}
				require.NoError(t, err)
				require.Same(t, f.claims, out)
			},
		)
	}
}

func TestCheckExpiryDelegatesToVerifier(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	verifier := mocks.NewMockTicketVerifier(ctrl)
	signer := mocks.NewMockSigner(ctrl)
	svc := activation.NewService(signer, verifier)
	claims := tickettest.New()

	for _, want := range []error{
		nil,
		errors.New("expired"),
	} {
		name := "valid"
		if want != nil {
			name = "expired"
		}

		t.Run(name, func(t *testing.T) {
			now := time.Unix(claims.ExpiresAt, 0)

			verifier.
				EXPECT().
				CheckExpiry(claims, now).
				Return(want)

			require.ErrorIs(t, svc.CheckExpiry(claims, now), want)
		})
	}
}
