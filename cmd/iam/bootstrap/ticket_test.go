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

	sharedticket "xxx/internal/shared/ticket"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func newIAMTicketTestConfig(t *testing.T) (*viper.Viper, []byte, []byte) {
	t.Helper()

	aesKey := bytes.Repeat([]byte{0xA1}, 32)
	seed := bytes.Repeat([]byte{0xB2}, ed25519.SeedSize)
	loader := viper.New()
	loader.Set("TICKET_ENCRYPT_CURRENT_KEY_ID", 3)
	loader.Set("TICKET_ENCRYPT_KEYS", "3:"+base64.StdEncoding.EncodeToString(aesKey))
	loader.Set("TICKET_SIGNING_KEY_ID", 7)
	loader.Set("TICKET_SIGNING_PRIVATE_KEY", base64.StdEncoding.EncodeToString(seed))
	t.Cleanup(func() {
		clear(aesKey)
		clear(seed)
	})
	return loader, aesKey, seed
}

func TestParseTicketSigningPrivate(t *testing.T) {
	seed := bytes.Repeat([]byte{0xB2}, ed25519.SeedSize)
	got, err := parseTicketSigningPrivate(" \n" + base64.StdEncoding.EncodeToString(seed) + " \t")
	require.NoError(t, err)
	require.Equal(t, seed, got)
	clear(got)

	for _, value := range []string{"", "not-base64", base64.StdEncoding.EncodeToString(make([]byte, 31)), base64.StdEncoding.EncodeToString(make([]byte, 33)), base64.StdEncoding.EncodeToString(make([]byte, ed25519.PrivateKeySize))} {
		got, err := parseTicketSigningPrivate(value)
		require.Error(t, err)
		require.Nil(t, got)
	}
}

func TestLoadTicketSigner_SignsAndEncryptsTicket(t *testing.T) {
	loader, aesKey, seed := newIAMTicketTestConfig(t)
	signer, err := loadTicketSigner(loader)
	require.NoError(t, err)
	require.NotNil(t, signer)

	in := &sharedticket.Ticket{DeviceID: 42, ExpiresAt: time.Now().Add(time.Minute).Unix()}
	for i := range in.Keys.RecvEnc {
		in.Keys.RecvEnc[i] = 0x11
		in.Keys.SendEnc[i] = 0x22
		in.Keys.RecvMac[i] = 0x33
		in.Keys.SendMac[i] = 0x44
		in.ActivationKey[i] = 0x55
		in.IdentityPub[i] = 0x66
	}
	defer in.Clear()
	wire, err := signer.Seal(in)
	require.NoError(t, err)
	require.Len(t, wire, 303)
	require.Equal(t, byte(1), wire[0])
	require.Equal(t, byte(7), wire[1])
	require.Equal(t, byte(3), wire[2])

	privateKey := ed25519.NewKeyFromSeed(seed)
	defer clear(privateKey)
	message := append([]byte("xxx/iam/ticket-signature/v1\x00"), wire[:239]...)
	require.True(t, ed25519.Verify(privateKey.Public().(ed25519.PublicKey), message, wire[239:]))

	sealer, err := sharedticket.NewSealer(3, map[byte][]byte{3: aesKey})
	require.NoError(t, err)
	out, err := sealer.Open(in.DeviceID, wire[2:239])
	require.NoError(t, err)
	defer out.Clear()
	require.Equal(t, in, out)
}

func TestLoadTicketSigner_RejectsInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name  string
		key   string
		value any
	}{
		{"missing encryption keys", "TICKET_ENCRYPT_KEYS", ""},
		{"zero encryption key ID", "TICKET_ENCRYPT_CURRENT_KEY_ID", 0},
		{"zero signing key ID", "TICKET_SIGNING_KEY_ID", 0},
		{"invalid signing seed", "TICKET_SIGNING_PRIVATE_KEY", "not-base64"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loader, _, _ := newIAMTicketTestConfig(t)
			loader.Set(tc.key, tc.value)
			signer, err := loadTicketSigner(loader)
			require.Error(t, err)
			require.Nil(t, signer)
		})
	}
}
