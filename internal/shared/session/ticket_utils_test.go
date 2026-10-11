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

package session

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseTicketEncryptKeys_Valid(t *testing.T) {
	k1, k2 := ticketTestKey(t), ticketTestKey(t)
	defer clear(k1)
	defer clear(k2)
	enc := base64.StdEncoding.EncodeToString

	got, err := ParseTicketEncryptKeys([]string{
		"0:" + enc(k1),
		"1:" + enc(k1),
		"  2:" + enc(k2) + "\n",
		"255:" + enc(k1),
	})
	require.NoError(t, err)
	require.Equal(t, map[byte][]byte{0: k1, 1: k1, 2: k2, 255: k1}, got)
	for _, key := range got {
		clear(key)
	}
}

func TestParseTicketEncryptKeys_EmptyListYieldsEmptyMap(t *testing.T) {
	for _, in := range [][]string{nil, {}} {
		got, err := ParseTicketEncryptKeys(in)
		require.NoError(t, err)
		require.Len(t, got, 0)

		// The constructor must reject this incomplete configuration.
		sealer, err := NewTicketSealer(1, got)
		require.Error(t, err)
		require.Nil(t, sealer)
	}
}

func TestParseTicketEncryptKeys_RejectsMalformedEntries(t *testing.T) {
	good := base64.StdEncoding.EncodeToString(ticketTestKey(t))
	urlSafe := base64.URLEncoding.EncodeToString(bytes.Repeat([]byte{0xfb, 0xff}, 16))
	for _, tc := range []struct {
		name  string
		entry string
	}{
		{"empty entry", ""},
		{"missing separator", good},
		{"empty ID", ":" + good},
		{"non-numeric ID", "x:" + good},
		{"negative ID", "-1:" + good},
		{"ID above byte range", "256:" + good},
		{"invalid base64", "1:***not-base64***"},
		{"URL-safe base64 alphabet", "1:" + urlSafe},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseTicketEncryptKeys([]string{tc.entry})
			require.Error(t, err)
			require.Nil(t, got)

			got, err = ParseTicketEncryptKeys([]string{"1:" + good, tc.entry})
			require.Error(t, err)
			require.Nil(t, got, "an invalid entry must reject the entire configuration")
		})
	}
}

// Regression: duplicate IDs must be rejected rather than silently replacing
// key material. This is expected to fail until ParseTicketEncryptKeys adds
// the same duplicate-ID validation as ParseTicketSigningPublicKeys.
func TestParseTicketEncryptKeys_RejectsDuplicateIDs(t *testing.T) {
	first := bytes.Repeat([]byte{0x11}, 32)
	second := bytes.Repeat([]byte{0x22}, 32)
	enc := base64.StdEncoding.EncodeToString
	for _, tc := range []struct {
		name    string
		entries []string
	}{
		{"same material", []string{"1:" + enc(first), "1:" + enc(first)}},
		{"different material", []string{"1:" + enc(first), "1:" + enc(second)}},
		{"equivalent numeric IDs", []string{"1:" + enc(first), "01:" + enc(second)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseTicketEncryptKeys(tc.entries)
			defer func() {
				for _, key := range got {
					clear(key)
				}
			}()
			require.Error(t, err, "duplicate AES key IDs must not overwrite existing keys")
			require.Nil(t, got)
		})
	}
}

// AES key-length validation currently belongs to NewTicketSealer.
func TestParseTicketEncryptKeys_FeedsNewTicketSealer(t *testing.T) {
	enc := base64.StdEncoding.EncodeToString
	keys, err := ParseTicketEncryptKeys([]string{
		"1:" + enc(ticketTestKey(t)),
		"2:" + enc(ticketTestKey(t)),
	})
	require.NoError(t, err)

	sealer, err := NewTicketSealer(2, keys)
	for _, key := range keys {
		clear(key)
	}
	require.NoError(t, err)

	in := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
	defer in.Clear()
	wire, err := sealer.Seal(in)
	require.NoError(t, err)

	out, err := sealer.Open(in.DeviceID, wire)
	require.NoError(t, err)
	defer out.Clear()
	require.Equal(t, in, out)

	for name, entry := range map[string]string{
		"16-byte key": "1:" + enc(make([]byte, 16)),
		"empty key":   "1:",
	} {
		t.Run(name, func(t *testing.T) {
			ks, err2 := ParseTicketEncryptKeys([]string{entry})
			require.NoError(t, err2)

			s, err2 := NewTicketSealer(1, ks)
			require.Error(t, err2)
			require.Nil(t, s)
		})
	}
}

func TestParseTicketSigningPrivate_ValidSeed(t *testing.T) {
	seed := ticketTestKey(t)
	defer clear(seed)
	value := " \t" + base64.StdEncoding.EncodeToString(seed) + "\n "
	got, err := ParseTicketSigningPrivate(value)
	require.NoError(t, err)
	defer clear(got)
	require.Len(t, got, ed25519.SeedSize)
	require.Equal(t, seed, got)

	sealer := newTicketTestSealer(t, 1, 1)
	signer, err := NewTicketSigner(sealer, 7, got)
	require.NoError(t, err)
	defer clear(signer.privateKey)
	clear(got)
	require.Equal(t, seed, signer.privateKey.Seed())
}

func TestParseTicketSigningPrivate_RejectsInvalidInput(t *testing.T) {
	for _, value := range []string{
		"", " \t\n", "***not-base64***",
		base64.URLEncoding.EncodeToString(bytes.Repeat([]byte{0xfb, 0xff}, 16)),
		base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 24)) + "!",
	} {
		got, err := ParseTicketSigningPrivate(value)
		require.Error(t, err)
		require.Nil(t, got)
	}
	for _, size := range []int{0, 1, 31, 33, ed25519.PrivateKeySize} {
		t.Run(fmt.Sprintf("decoded length %d", size), func(t *testing.T) {
			value := base64.StdEncoding.EncodeToString(make([]byte, size))
			got, err := ParseTicketSigningPrivate(value)
			require.Error(t, err)
			require.Nil(t, got, "this parser accepts a seed, not Go's 64-byte PrivateKey")
		})
	}
}

func TestParseTicketSigningPublicKeys_ValidAndFeedsVerifier(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)
	signerA, publicA := newTicketSignatureTestSigner(t, sealer, 0)
	signerB, publicB := newTicketSignatureTestSigner(t, sealer, 255)
	enc := base64.StdEncoding.EncodeToString
	keys, err := ParseTicketSigningPublicKeys([]string{
		" \t0 : " + enc(publicA) + " \n",
		"255:" + enc(publicB),
	})
	require.NoError(t, err)
	require.Equal(t, map[byte]ed25519.PublicKey{0: publicA, 255: publicB}, keys)

	verifier, err := NewTicketVerifier(sealer, keys, 5*time.Second)
	require.NoError(t, err)
	in := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
	defer in.Clear()

	for _, signer := range []*TicketSigner{signerA, signerB} {
		wire, err := signer.Seal(in)
		require.NoError(t, err)

		out, err := verifier.Open(in.DeviceID, wire)
		require.NoError(t, err)
		require.Equal(t, in, out)

		out.Clear()
	}
}

func TestParseTicketSigningPublicKeys_RejectsInvalidEntries(t *testing.T) {
	good := base64.StdEncoding.EncodeToString(make([]byte, ed25519.PublicKeySize))
	urlSafe := base64.URLEncoding.EncodeToString(bytes.Repeat([]byte{0xfb, 0xff}, 16))
	for _, tc := range []struct {
		name    string
		entries []string
	}{
		{"nil list", nil},
		{"empty list", []string{}},
		{"empty entry", []string{""}},
		{"missing separator", []string{good}},
		{"empty ID", []string{":" + good}},
		{"non-numeric ID", []string{"x:" + good}},
		{"negative ID", []string{"-1:" + good}},
		{"ID above byte range", []string{"256:" + good}},
		{"empty public key", []string{"1:"}},
		{"invalid base64", []string{"1:***not-base64***"}},
		{"URL-safe base64 alphabet", []string{"1:" + urlSafe}},
		{"duplicate ID", []string{"1:" + good, "1:" + good}},
		{"equivalent numeric IDs", []string{"1:" + good, "01:" + good}},
		{"valid then invalid", []string{"1:" + good, "2:***not-base64***"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseTicketSigningPublicKeys(tc.entries)
			require.Error(t, err)
			require.Nil(t, got)
		})
	}
	for _, size := range []int{0, 1, 31, 33, 64} {
		t.Run(fmt.Sprintf("public key length %d", size), func(t *testing.T) {
			got, err := ParseTicketSigningPublicKeys([]string{
				"1:" + base64.StdEncoding.EncodeToString(make([]byte, size)),
			})
			require.Error(t, err)
			require.Nil(t, got)
		})
	}
}
