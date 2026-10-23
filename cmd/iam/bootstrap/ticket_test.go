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

	"xxx/internal/shared/ticket"
	"xxx/internal/shared/ticket/tickettest"

	slicepool "xxx/pkg/pool/slice"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestParseTicketSigningPrivate(t *testing.T) {
	seed := bytes.Repeat([]byte{0xB2}, ed25519.SeedSize)
	got, err := parseTicketSigningPrivate(" \n" + base64.StdEncoding.EncodeToString(seed) + " \t")
	require.NoError(t, err)
	require.Equal(t, seed, got)
	clear(got[:cap(got)])

	values := []string{
		"",
		"not-base64",
		base64.StdEncoding.EncodeToString(make([]byte, ed25519.SeedSize-1)),
		base64.StdEncoding.EncodeToString(make([]byte, ed25519.SeedSize+1)),
		base64.StdEncoding.EncodeToString(make([]byte, ed25519.PrivateKeySize)),
	}
	for _, value := range values {
		got, err = parseTicketSigningPrivate(value)
		require.Error(t, err)
		require.Nil(t, got)
	}
}

func TestLoadTicketSignerSignsWithoutEncryptionConfiguration(t *testing.T) {
	loader := viper.New()
	seed := tickettest.SigningSeed()
	loader.Set("TICKET_SIGNING_KEY_ID", 7)
	loader.Set("TICKET_SIGNING_PRIVATE_KEY", base64.StdEncoding.EncodeToString(seed))
	signer, err := loadTicketSigner(loader)
	require.NoError(t, err)

	in := tickettest.New()
	wire, err := signer.Sign(in)
	require.NoError(t, err)
	t.Cleanup(func() {
		clear(wire[:cap(wire)])
		slicepool.Put(wire)
	})

	key := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	require.GreaterOrEqual(t, len(wire), ticket.MinSignedSize)
	require.Equal(t, byte(7), wire[1])

	unsigned := wire[:len(wire)-ed25519.SignatureSize]
	message := ticket.SigningMessage(unsigned)
	t.Cleanup(func() {
		clear(message[:cap(message)])
		slicepool.Put(message)
	})
	require.True(t, ed25519.Verify(key, message, wire[len(unsigned):]))

	for _, tc := range []struct {
		key   string
		value any
	}{
		{"TICKET_SIGNING_KEY_ID", 0},
		{"TICKET_SIGNING_PRIVATE_KEY", "not-base64"},
	} {
		old := loader.Get(tc.key)
		loader.Set(tc.key, tc.value)
		_, err = loadTicketSigner(loader)
		require.Error(t, err)
		loader.Set(tc.key, old)
	}
}
