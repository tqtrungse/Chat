/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package bootstrap

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"testing"
	"time"

	"xxx/internal/hub/core/transport"
	"xxx/internal/hub/infrastructure/crypto"
	"xxx/internal/shared/ticket/tickettest"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestParseTicketSigningPublicKeys(t *testing.T) {
	key0 := bytes.Repeat([]byte{0x10}, ed25519.PublicKeySize)
	key255 := bytes.Repeat([]byte{0x20}, ed25519.PublicKeySize)
	got, err := parseTicketSigningPublicKeys(
		[]string{
			" 0:" + base64.StdEncoding.EncodeToString(key0) + " ",
			" 255:" + base64.StdEncoding.EncodeToString(key255) + "\n",
		},
	)
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
		{"wrong key size", []string{
			"1:" + base64.StdEncoding.EncodeToString(make([]byte, ed25519.PublicKeySize-1))},
		},
		{"duplicate ID", []string{
			"1:" + base64.StdEncoding.EncodeToString(key0),
			"01:" + base64.StdEncoding.EncodeToString(key255),
		}},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				keys, parseErr := parseTicketSigningPublicKeys(tc.entries)
				require.Error(t, parseErr)
				require.Nil(t, keys)
			},
		)
	}
}

func TestLoadTicketVerifierWithoutSharedSecret(t *testing.T) {
	seed := tickettest.SigningSeed()
	pub := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	loader := viper.New()
	loader.Set("TICKET_SIGNING_PUBLIC_KEYS", "7:"+base64.StdEncoding.EncodeToString(pub))
	loader.Set("TICKET_CLOCK_SKEW", "5s")
	verifier, err := loadTicketVerifier(loader)
	require.NoError(t, err)

	in := tickettest.New()
	wire, err := tickettest.Signed(7, in)
	require.NoError(t, err)

	out, err := verifier.Verify(
		wire,
		transport.Identity{
			Audience:  in.Audience,
			KeyID:     in.HubKeyID,
			PublicKey: in.HubPublicKey,
			Version:   in.WireVersion,
		},
	)
	require.NoError(t, err)
	require.Equal(t, in, out)
	require.NoError(t, verifier.CheckExpiry(out, time.Unix(out.ExpiresAt, 0).Add(5*time.Second)))
	require.NoError(t, verifier.CheckExpiry(out, time.Unix(out.IssuedAt, 0).Add(-5*time.Second)))
	require.ErrorIs(
		t,
		verifier.CheckExpiry(out, time.Unix(out.ExpiresAt, 0).Add(5*time.Second+time.Nanosecond)),
		crypto.ErrTicketExpired,
	)
	require.ErrorIs(
		t,
		verifier.CheckExpiry(out, time.Unix(out.IssuedAt, 0).Add(-5*time.Second-time.Nanosecond)),
		crypto.ErrTicketExpired,
	)

	for _, value := range []string{
		"",
		"not-duration",
		"-1s",
	} {
		loader.Set("TICKET_CLOCK_SKEW", value)
		_, err = loadTicketVerifier(loader)
		require.Error(t, err)
	}

	loader.Set("TICKET_CLOCK_SKEW", "5s")
	loader.Set("TICKET_SIGNING_PUBLIC_KEYS", "")
	_, err = loadTicketVerifier(loader)
	require.Error(t, err)
}
