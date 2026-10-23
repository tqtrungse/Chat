/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package hubendpoint

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"strings"
	"testing"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
)

func validDescriptor(t *testing.T) Descriptor {
	t.Helper()

	privateKey, err := ecdh.X25519().NewPrivateKey(bytes.Repeat([]byte{0x42}, 32))
	require.NoError(t, err)

	return Descriptor{
		Address:  "hub.example.com:9000",
		Audience: "hub.chat", KeyID: 7,
		PublicKey: privateKey.PublicKey().Bytes(),
		Protocol:  Protocol,
		Version:   WireVersion,
	}
}

func TestDescriptorRejectsInvalidPublicConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Descriptor)
	}{
		{
			"protocol",
			func(d *Descriptor) { d.Protocol = "Noise_NN_25519_ChaChaPoly_SHA256" },
		},
		{
			"version",
			func(d *Descriptor) { d.Version = 1 },
		},
		{
			"zero key ID",
			func(d *Descriptor) { d.KeyID = 0 },
		},
		{
			"short key",
			func(d *Descriptor) { d.PublicKey = d.PublicKey[:X25519PublicKeySize-1] },
		},
		{
			"long key",
			func(d *Descriptor) { d.PublicKey = append(d.PublicKey, 0) },
		},
		{
			"low order zero",
			func(d *Descriptor) { d.PublicKey = make([]byte, X25519PublicKeySize) },
		},
		{
			"low order one",
			func(d *Descriptor) { d.PublicKey = append([]byte{1}, make([]byte, X25519PublicKeySize-1)...) },
		},
		{
			"empty audience",
			func(d *Descriptor) { d.Audience = "" },
		},
		{
			"audience spaces",
			func(d *Descriptor) { d.Audience = "hub chat" },
		},
		{
			"audience oversized",
			func(d *Descriptor) { d.Audience = strings.Repeat("a", 129) },
		},
		{
			"missing host",
			func(d *Descriptor) { d.Address = ":9000" },
		},
		{
			"URL",
			func(d *Descriptor) { d.Address = "https://hub.example.com:9000" },
		},
		{
			"port zero",
			func(d *Descriptor) { d.Address = "hub.example.com:0" },
		},
		{
			"port overflow",
			func(d *Descriptor) { d.Address = "hub.example.com:65536" },
		},
		{
			"hostname",
			func(d *Descriptor) { d.Address = "bad_host:9000" },
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				d := validDescriptor(t)
				tc.change(&d)
				require.Error(t, d.Validate())

				_, err := d.Prologue()
				require.Error(t, err)
			},
		)
	}

	for _, address := range []string{
		"localhost:9000",
		"127.0.0.1:9000",
		"[::1]:9000",
		"hub.example.com.:9000",
	} {
		d := validDescriptor(t)
		d.Address = address
		require.NoError(t, d.Validate())
	}
}

func TestProtocolVectors(t *testing.T) {
	data, err := os.ReadFile("../../../docs/protocol/noise-v2-vectors.json")
	require.NoError(t, err)

	var vector struct {
		Descriptor           Descriptor `json:"descriptor"`
		PrologueHex          string     `json:"prologue_hex"`
		ChannelBindingHex    string     `json:"channel_binding_hex"`
		TicketHex            string     `json:"ticket_hex"`
		ChallengeHex         string     `json:"challenge_hex"`
		SignatureInputHex    string     `json:"signature_input_hex"`
		IdentityPublicHex    string     `json:"identity_public_hex"`
		IdentitySignatureHex string     `json:"identity_signature_hex"`
	}
	require.NoError(t, json.Unmarshal(data, &vector))

	decode := func(s string) []byte {
		b, err := hex.DecodeString(s)
		require.NoError(t, err)
		return b
	}
	prologue, err := vector.Descriptor.Prologue()
	t.Cleanup(func() {
		slicepool.Put(prologue)
	})
	require.NoError(t, err)
	require.Equal(t, decode(vector.PrologueHex), prologue)

	// Verify the published identity-proof vector independently of the future
	// production proof helper, including all four length-delimited fields.
	input := bytes.NewReader(decode(vector.SignatureInputHex))
	ticketHash := sha256.Sum256(decode(vector.TicketHex))
	for _, want := range [][]byte{
		[]byte("xxx/hub/device-auth/v2"),
		decode(vector.ChannelBindingHex), ticketHash[:],
		decode(vector.ChallengeHex),
	} {
		var length uint32
		require.NoError(t, binary.Read(input, binary.LittleEndian, &length))
		require.Equal(t, uint32(len(want)), length)

		got := make([]byte, length)
		_, err = input.Read(got)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}

	require.Zero(t, input.Len())
	require.True(
		t,
		ed25519.Verify(
			decode(vector.IdentityPublicHex),
			decode(vector.SignatureInputHex),
			decode(vector.IdentitySignatureHex),
		),
	)
}
