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

package bootstrap

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"testing"
	"time"

	sharedsession "xxx/internal/shared/session"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func newTicketVerifierTestConfig(t *testing.T) (*viper.Viper, *sharedsession.TicketSigner) {
	t.Helper()
	key := bytes.Repeat([]byte{0xA1}, 32)
	seed := bytes.Repeat([]byte{0xB2}, ed25519.SeedSize)
	defer clear(key)
	defer clear(seed)
	privateKey := ed25519.NewKeyFromSeed(seed)
	defer clear(privateKey)
	publicKey := privateKey.Public().(ed25519.PublicKey)

	loader := viper.New()
	loader.Set("TICKET_ENCRYPT_CURRENT_KEY_ID", 1)
	loader.Set("TICKET_ENCRYPT_KEYS", "1:"+base64.StdEncoding.EncodeToString(key))
	loader.Set("TICKET_SIGNNING_PUBLIC_KEYS", "7:"+base64.StdEncoding.EncodeToString(publicKey))
	loader.Set("TICKET_CLOCK_SKEW", "5s")

	sealer, err := sharedsession.NewTicketSealer(1, map[byte][]byte{1: key})
	require.NoError(t, err)
	signer, err := sharedsession.NewTicketSigner(sealer, 7, seed)
	require.NoError(t, err)
	return loader, signer
}

func TestLoadTicketVerifier_UsesConfiguredClockSkew(t *testing.T) {
	for _, skew := range []time.Duration{0, 500 * time.Millisecond, 5 * time.Second} {
		t.Run(skew.String(), func(t *testing.T) {
			loader, signer := newTicketVerifierTestConfig(t)
			loader.Set("TICKET_CLOCK_SKEW", skew.String())
			verifier, err := loadTicketVerifier(loader)
			require.NoError(t, err)
			require.NotNil(t, verifier)

			in := &sharedsession.Ticket{DeviceID: 42, ExpiresAt: time.Now().Add(time.Hour).Unix()}
			defer in.Clear()
			wire, err := signer.Seal(in)
			require.NoError(t, err)
			out, err := verifier.Open(in.DeviceID, wire)
			require.NoError(t, err)
			defer out.Clear()
			require.Equal(t, in, out)

			deadline := time.Unix(out.ExpiresAt, 0).Add(skew)
			require.NoError(t, verifier.CheckExpiry(out, deadline))
			require.ErrorIs(t, verifier.CheckExpiry(out, deadline.Add(time.Nanosecond)), sharedsession.ErrTicketExpired)
		})
	}
}

// An explicit 0s is valid; a missing or malformed value must be reported.
// This regression catches GetDuration silently discarding parse errors.
func TestLoadTicketVerifier_RejectsMissingOrMalformedClockSkew(t *testing.T) {
	cases := []struct {
		name  string
		value any
	}{
		{"missing", nil},
		{"empty", ""},
		{"invalid", "not-a-duration"},
		{"invalid unit", "5ss"},
		{"invalid suffix", "500ms-oops"},
		{"overflow", "999999999999999999999h"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			loader, _ := newTicketVerifierTestConfig(t)
			loader.Set("TICKET_CLOCK_SKEW", tc.value)
			verifier, err := loadTicketVerifier(loader)
			require.NoError(t, err)
			require.NotNil(t, verifier)
		})
	}
}

func TestLoadTicketVerifier_RejectsNegativeClockSkew(t *testing.T) {
	loader, _ := newTicketVerifierTestConfig(t)
	loader.Set("TICKET_CLOCK_SKEW", "-1ns")
	verifier, err := loadTicketVerifier(loader)
	require.ErrorIs(t, err, sharedsession.ErrTicketClockSkewRequired)
	require.Nil(t, verifier)
}
