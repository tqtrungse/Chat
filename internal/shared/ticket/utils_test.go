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

package ticket

import (
	"bytes"
	"encoding/base64"
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
		sealer, err := NewSealer(1, got)
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

// Duplicate IDs must be rejected rather than silently replacing key material.
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

// AES key-length validation currently belongs to NewSealer.
func TestParseTicketEncryptKeys_FeedsNewSealer(t *testing.T) {
	enc := base64.StdEncoding.EncodeToString
	keys, err := ParseTicketEncryptKeys([]string{
		"1:" + enc(ticketTestKey(t)),
		"2:" + enc(ticketTestKey(t)),
	})
	require.NoError(t, err)

	sealer, err := NewSealer(2, keys)
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

			s, err2 := NewSealer(1, ks)
			require.Error(t, err2)
			require.Nil(t, s)
		})
	}
}
