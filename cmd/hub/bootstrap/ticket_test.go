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

package bootstrap

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"testing"
	"time"

	hubcrypto "xxx/internal/hub/infrastructure/crypto"
	sharedticket "xxx/internal/shared/ticket"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func newHubTicketTestConfig(t *testing.T) (*viper.Viper, *sharedticket.Sealer, ed25519.PrivateKey) {
	t.Helper()

	aesKey := bytes.Repeat([]byte{0xA1}, 32)
	seed := bytes.Repeat([]byte{0xB2}, ed25519.SeedSize)
	privateKey := ed25519.NewKeyFromSeed(seed)
	clear(seed)
	t.Cleanup(func() {
		clear(aesKey)
		clear(privateKey)
	})

	sealer, err := sharedticket.NewSealer(1, map[byte][]byte{1: aesKey})
	require.NoError(t, err)
	loader := viper.New()
	loader.Set("TICKET_ENCRYPT_CURRENT_KEY_ID", 1)
	loader.Set("TICKET_ENCRYPT_KEYS", "1:"+base64.StdEncoding.EncodeToString(aesKey))
	loader.Set("TICKET_SIGNNING_PUBLIC_KEYS", "7:"+base64.StdEncoding.EncodeToString(privateKey.Public().(ed25519.PublicKey)))
	loader.Set("TICKET_CLOCK_SKEW", "5s")
	return loader, sealer, privateKey
}

func signHubTicketTestEnvelope(t *testing.T, sealer *sharedticket.Sealer, privateKey ed25519.PrivateKey, keyID byte, in *sharedticket.Ticket) []byte {
	t.Helper()

	sealed, err := sealer.Seal(in)
	require.NoError(t, err)
	unsigned := make([]byte, sharedticket.SignedUnsignedSize)
	unsigned[0], unsigned[1] = sharedticket.SignedVersion, keyID
	copy(unsigned[sharedticket.SignedHeaderSize:], sealed)
	message := append([]byte(sharedticket.SigningDomain), unsigned...)
	signature := ed25519.Sign(privateKey, message)
	wire := append(unsigned, signature...)
	require.Len(t, wire, sharedticket.SignedSize)
	return wire
}

func TestParseTicketSigningPublicKeys(t *testing.T) {
	key0 := bytes.Repeat([]byte{0x10}, ed25519.PublicKeySize)
	key255 := bytes.Repeat([]byte{0x20}, ed25519.PublicKeySize)
	got, err := parseTicketSigningPublicKeys([]string{
		" 0:" + base64.StdEncoding.EncodeToString(key0) + " ",
		" 255:" + base64.StdEncoding.EncodeToString(key255) + "\n",
	})
	require.NoError(t, err)
	require.Equal(t, ed25519.PublicKey(key0), got[0])
	require.Equal(t, ed25519.PublicKey(key255), got[255])

	for _, tc := range []struct {
		name    string
		entries []string
	}{
		{"no entries", nil},
		{"empty entry", []string{""}},
		{"missing separator", []string{"not-an-entry"}},
		{"invalid ID", []string{"x:" + base64.StdEncoding.EncodeToString(key0)}},
		{"negative ID", []string{"-1:" + base64.StdEncoding.EncodeToString(key0)}},
		{"ID overflow", []string{"256:" + base64.StdEncoding.EncodeToString(key0)}},
		{"invalid base64", []string{"1:***"}},
		{"wrong key size", []string{"1:" + base64.StdEncoding.EncodeToString(make([]byte, ed25519.PublicKeySize-1))}},
		{"duplicate ID", []string{
			"1:" + base64.StdEncoding.EncodeToString(key0),
			"01:" + base64.StdEncoding.EncodeToString(key255),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keys, err := parseTicketSigningPublicKeys(tc.entries)
			require.Error(t, err)
			require.Nil(t, keys)
		})
	}
}

func TestLoadTicketVerifier_RoundTripAndConfiguredClockSkew(t *testing.T) {
	loader, sealer, privateKey := newHubTicketTestConfig(t)
	verifier, err := loadTicketVerifier(loader)
	require.NoError(t, err)
	require.NotNil(t, verifier)

	in := &sharedticket.Ticket{DeviceID: 42, ExpiresAt: time.Now().Add(time.Hour).Unix()}
	defer in.Clear()
	wire := signHubTicketTestEnvelope(t, sealer, privateKey, 7, in)
	before := bytes.Clone(wire)

	out, err := verifier.Open(in.DeviceID, wire)
	require.NoError(t, err)
	defer out.Clear()
	require.Equal(t, in, out)
	require.Equal(t, before, wire, "verification must not modify the input buffer")

	deadline := time.Unix(out.ExpiresAt, 0).Add(5 * time.Second)
	require.NoError(t, verifier.CheckExpiry(out, deadline))
	require.ErrorIs(t, verifier.CheckExpiry(out, deadline.Add(time.Nanosecond)), hubcrypto.ErrTicketExpired)
}

func TestLoadTicketVerifier_RejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name  string
		key   string
		value any
	}{
		{"missing clock skew", "TICKET_CLOCK_SKEW", nil},
		{"empty clock skew", "TICKET_CLOCK_SKEW", ""},
		{"malformed clock skew", "TICKET_CLOCK_SKEW", "not-a-duration"},
		{"negative clock skew", "TICKET_CLOCK_SKEW", "-1ns"},
		{"zero AES key ID", "TICKET_ENCRYPT_CURRENT_KEY_ID", 0},
		{"missing public key", "TICKET_SIGNNING_PUBLIC_KEYS", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loader, _, _ := newHubTicketTestConfig(t)
			loader.Set(tc.key, tc.value)
			verifier, err := loadTicketVerifier(loader)
			require.Error(t, err)
			require.Nil(t, verifier)
		})
	}

	loader, _, _ := newHubTicketTestConfig(t)
	loader.Set("TICKET_SIGNNING_PUBLIC_KEYS", "7:"+base64.StdEncoding.EncodeToString(make([]byte, ed25519.PublicKeySize-1)))
	verifier, err := loadTicketVerifier(loader)
	require.Error(t, err)
	require.Nil(t, verifier)
}
