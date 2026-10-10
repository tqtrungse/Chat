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
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// helpers (prefixed with "ticket" to avoid clashing with other test files in
// package crypto)
// ---------------------------------------------------------------------------

func ticketTestKey(t *testing.T) []byte {
	t.Helper()

	k := make([]byte, 32)
	_, err := rand.Read(k)
	require.NoError(t, err)
	return k
}

// newTicketTestSealer builds a sealer whose current key is curID, holding one
// fresh random key for every id in ids.
func newTicketTestSealer(t *testing.T, curID byte, ids ...byte) *TicketSealer {
	t.Helper()

	keys := make(map[byte][]byte, len(ids))
	for _, id := range ids {
		keys[id] = ticketTestKey(t)
	}
	s, err := NewTicketSealer(curID, keys)
	require.NoError(t, err)
	return s
}

// newTicketTestTicket returns a ticket with every field populated with
// distinct random data, so a marshaling bug (wrong offset, overlapping
// fields, a field dropped) cannot hide behind an all-zero field.
func newTicketTestTicket(t *testing.T, deviceID uint64, expiresAt int64) *Ticket {
	t.Helper()

	tk := &Ticket{DeviceID: deviceID, ExpiresAt: expiresAt}
	for _, b := range [][]byte{
		tk.Keys.RecvEnc[:],
		tk.Keys.SendEnc[:],
		tk.Keys.RecvMac[:],
		tk.Keys.SendMac[:],
		tk.IdentityPub[:],
	} {
		_, err := rand.Read(b)
		require.NoError(t, err)
	}
	return tk
}

func ticketExpiry(d time.Duration) int64 {
	return time.Now().Add(d).Unix()
}

// requireTicketOpenFails asserts Open fails with exactly the wanted sentinel
// error and does not hand back a partially filled ticket.
func requireTicketOpenFails(
	t *testing.T,
	s *TicketSealer,
	deviceID uint64,
	tk []byte,
	want error,
	msgAndArgs ...any,
) {
	t.Helper()

	out, err := s.Open(deviceID, tk)
	require.ErrorIs(t, err, want, msgAndArgs...)
	require.Nil(t, out, msgAndArgs...)
}

// ----------------------------------------
// Seal / Open: round trip, tamper, expiry
// ----------------------------------------

func TestTicket_RoundTripAndTamper(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	s, err := NewTicketSealer(1, map[byte][]byte{1: key})
	require.NoError(t, err)

	in := newTicketTestTicket(t, 42, time.Now().Add(20*time.Second).Unix())

	tk, err := s.Seal(in)
	require.NoError(t, err)
	require.Len(t, tk, ticketSize)

	out, err := s.Open(42, tk)
	require.NoError(t, err)
	require.Equal(t, in, out)

	_, err = s.Open(43, tk) // wrong device -> AAD does not match
	require.ErrorIs(t, err, ErrTicketInvalid)

	bad := append([]byte(nil), tk...)
	bad[20] ^= 1
	_, err = s.Open(42, bad)
	require.ErrorIs(t, err, ErrTicketInvalid)

	in.ExpiresAt = time.Now().Add(-time.Minute).Unix()
	tk, _ = s.Seal(in)
	_, err = s.Open(42, tk)
	require.ErrorIs(t, err, ErrTicketExpired)
}

func TestTicket_RoundTrip_DeviceIDBoundaries(t *testing.T) {
	s := newTicketTestSealer(t, 1, 1)

	for _, id := range []uint64{0, 1, math.MaxUint64} {
		in := newTicketTestTicket(t, id, ticketExpiry(20*time.Second))
		tk, err := s.Seal(in)
		require.NoError(t, err)

		out, err := s.Open(id, tk)
		require.NoError(t, err, "device %d", id)
		require.Equal(t, in, out, "device %d", id)
	}
}

// Flipping any single bit anywhere in the ticket -- key ID, nonce, ciphertext
// or tag -- must be rejected as ErrTicketInvalid. Byte 20 alone (the old
// tamper check) only covers the ciphertext region.
func TestTicket_Open_RejectsEveryBitFlip(t *testing.T) {
	s := newTicketTestSealer(t, 1, 1)
	tk, err := s.Seal(newTicketTestTicket(t, 42, ticketExpiry(20*time.Second)))
	require.NoError(t, err)

	for i := range tk {
		for bit := range 8 {
			bad := bytes.Clone(tk)
			bad[i] ^= 1 << bit
			requireTicketOpenFails(t, s, 42, bad, ErrTicketInvalid, "byte %d bit %d", i, bit)
		}
	}

	// The untouched ticket is still fine (the loop worked on copies).
	_, err = s.Open(42, tk)
	require.NoError(t, err)
}

func TestTicket_Open_RejectsWrongLength(t *testing.T) {
	s := newTicketTestSealer(t, 1, 1)
	tk, err := s.Seal(newTicketTestTicket(t, 42, ticketExpiry(20*time.Second)))
	require.NoError(t, err)

	requireTicketOpenFails(t, s, 42, nil, ErrTicketInvalid)
	for n := range ticketSize {
		requireTicketOpenFails(t, s, 42, tk[:n], ErrTicketInvalid, "truncated to %d", n)
	}
	requireTicketOpenFails(t, s, 42, append(bytes.Clone(tk), 0), ErrTicketInvalid, "one extra byte")
	requireTicketOpenFails(t, s, 42, append(bytes.Clone(tk), tk...), ErrTicketInvalid, "doubled")
}

// Timestamps are compared with second granularity and ticketMaxSkew of
// tolerance. The cases keep a 2s margin on each side of the boundary so the
// test is not sensitive to the clock ticking over mid-test.
func TestTicket_Open_ExpiryAndSkew(t *testing.T) {
	s := newTicketTestSealer(t, 1, 1)
	skew := int64(ticketMaxSkew.Seconds())
	now := time.Now().Unix()

	cases := []struct {
		name      string
		expiresAt int64
		wantErr   error
	}{
		{"not yet expired", now + 20, nil},
		{"expired but inside skew", now - (skew - 2), nil},
		{"expired beyond skew", now - (skew + 2), ErrTicketExpired},
		{"long expired", now - 3600, ErrTicketExpired},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := newTicketTestTicket(t, 42, tc.expiresAt)
			tk, err := s.Seal(in)
			require.NoError(t, err)

			if tc.wantErr != nil {
				requireTicketOpenFails(t, s, 42, tk, tc.wantErr)
				return
			}
			out, err := s.Open(42, tk)
			require.NoError(t, err)
			require.Equal(t, in, out)
		})
	}
}

// Authentication must come first: a forged/tampered/wrong-device ticket has
// to look "invalid" even when its (unauthenticated) contents would say
// "expired", so Open never reports expiry state for something it could not
// authenticate.
func TestTicket_Open_AuthenticatesBeforeExpiry(t *testing.T) {
	s := newTicketTestSealer(t, 1, 1)
	tk, err := s.Seal(newTicketTestTicket(t, 42, ticketExpiry(-time.Hour)))
	require.NoError(t, err)

	requireTicketOpenFails(t, s, 42, tk, ErrTicketExpired) // control: genuine but expired

	requireTicketOpenFails(t, s, 43, tk, ErrTicketInvalid) // wrong device

	bad := bytes.Clone(tk)
	bad[20] ^= 1
	requireTicketOpenFails(t, s, 42, bad, ErrTicketInvalid) // tampered
}

// Open's AEAD already binds the device ID through the AAD, so the explicit
// "plaintext DeviceID == deviceID" check is defense in depth and cannot be
// reached through Seal. Hand-seal a ticket whose AAD says device 8 while the
// plaintext says device 7: the AEAD accepts it for device 8, and only the
// explicit check can reject it.
func TestTicket_Open_RejectsPlaintextDeviceIDMismatch(t *testing.T) {
	key := ticketTestKey(t)
	s, err := NewTicketSealer(1, map[byte][]byte{1: key})
	require.NoError(t, err)
	aead := s.aeads[1]

	in := newTicketTestTicket(t, 7, ticketExpiry(20*time.Second))
	var pt [ticketPlainSize]byte
	marshalTicket(pt[:], in)

	out := make([]byte, ticketHeader, ticketSize)
	out[0] = 1
	_, err = rand.Read(out[1:ticketHeader])
	require.NoError(t, err)
	tk := aead.Seal(out, out[1:ticketHeader], pt[:], ticketAAD(8))
	require.Len(t, tk, ticketSize)

	requireTicketOpenFails(t, s, 8, tk, ErrTicketInvalid)
}

// Open's input aliases the nio read buffer, so it must never be modified --
// on success or on any failure path.
func TestTicket_Open_DoesNotMutateInput(t *testing.T) {
	s := newTicketTestSealer(t, 1, 1)

	valid, err := s.Seal(newTicketTestTicket(t, 42, ticketExpiry(20*time.Second)))
	require.NoError(t, err)
	expired, err := s.Seal(newTicketTestTicket(t, 42, ticketExpiry(-time.Hour)))
	require.NoError(t, err)
	tampered := bytes.Clone(valid)
	tampered[20] ^= 1

	cases := []struct {
		name     string
		deviceID uint64
		tk       []byte
	}{
		{"valid", 42, valid},
		{"expired", 42, expired},
		{"tampered", 42, tampered},
		{"wrong device", 43, valid},
	}
	for _, tc := range cases {
		before := bytes.Clone(tc.tk)
		_, _ = s.Open(tc.deviceID, tc.tk)
		require.Equal(t, before, tc.tk, "Open modified its input (%s)", tc.name)
	}
}

// --------------------------------------------
// Seal: nonce, confidentiality, input handling
// --------------------------------------------

func TestTicket_Seal_UsesFreshNonce(t *testing.T) {
	s := newTicketTestSealer(t, 1, 1)
	in := newTicketTestTicket(t, 42, ticketExpiry(20*time.Second))

	const n = 1000
	seen := make(map[string]struct{}, n)
	var first []byte
	for i := range n {
		tk, err := s.Seal(in)
		require.NoError(t, err)

		nonce := string(tk[1:ticketHeader])
		_, dup := seen[nonce]
		require.False(t, dup, "nonce reused on call %d", i)
		seen[nonce] = struct{}{}

		if first == nil {
			first = tk
			continue
		}
		require.NotEqual(t, first, tk, "same input sealed to identical tickets")

		out, err := s.Open(42, tk)
		require.NoError(t, err)
		require.Equal(t, in, out)
	}
}

func TestTicket_Seal_DoesNotLeakPlaintextOrMutateInput(t *testing.T) {
	s := newTicketTestSealer(t, 1, 1)
	in := newTicketTestTicket(t, 42, ticketExpiry(20*time.Second))
	snapshot := *in

	tk, err := s.Seal(in)
	require.NoError(t, err)
	require.Equal(t, snapshot, *in, "Seal must not modify its input")

	secrets := map[string][]byte{
		"RecvEnc":     in.Keys.RecvEnc[:],
		"SendEnc":     in.Keys.SendEnc[:],
		"RecvMac":     in.Keys.RecvMac[:],
		"SendMac":     in.Keys.SendMac[:],
		"IdentityPub": in.IdentityPub[:],
	}
	for name, secret := range secrets {
		require.False(t, bytes.Contains(tk, secret), "%s appears in clear text in the ticket", name)
	}
}

// -----------
// Wire format
// -----------

// Pins the documented sizes. authTagSize is defined elsewhere in the package,
// so also check it matches the real GCM overhead.
func TestTicket_SizeConstants(t *testing.T) {
	s := newTicketTestSealer(t, 1, 1)
	aead := s.aeads[1]

	require.Equal(t, 16, aead.Overhead(), "authTagSize must equal the GCM tag size")
	require.Equal(t, ticketHeader-1, aead.NonceSize(), "header must hold keyID + a full GCM nonce")
	require.Equal(t, 176, ticketPlainSize)
	require.Equal(t, 205, ticketSize)
}

// Pins the plaintext layout byte for byte. Tickets are issued by one hub
// instance and opened by another (and across deploys), so a silently changed
// layout would invalidate live tickets.
func TestTicket_MarshalLayout(t *testing.T) {
	in := &Ticket{DeviceID: 0x0102030405060708, ExpiresAt: 0x1112131415161718}
	fill := func(dst []byte, v byte) {
		for i := range dst {
			dst[i] = v
		}
	}
	fill(in.Keys.RecvEnc[:], 0xA1)
	fill(in.Keys.SendEnc[:], 0xA2)
	fill(in.Keys.RecvMac[:], 0xA3)
	fill(in.Keys.SendMac[:], 0xA4)
	fill(in.IdentityPub[:], 0xA5)

	var b [ticketPlainSize]byte
	marshalTicket(b[:], in)

	require.Equal(t, []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}, b[0:8], "DeviceID, little-endian")
	require.Equal(t, []byte{0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11}, b[8:16], "ExpiresAt, little-endian")
	require.Equal(t, bytes.Repeat([]byte{0xA1}, 32), b[16:48], "RecvEnc")
	require.Equal(t, bytes.Repeat([]byte{0xA2}, 32), b[48:80], "SendEnc")
	require.Equal(t, bytes.Repeat([]byte{0xA3}, 32), b[80:112], "RecvMac")
	require.Equal(t, bytes.Repeat([]byte{0xA4}, 32), b[112:144], "SendMac")
	require.Equal(t, bytes.Repeat([]byte{0xA5}, 32), b[144:176], "IdentityPub")

	require.Equal(t, in, unmarshalTicket(b[:]))
}

func TestTicket_MarshalRoundTripExtremes(t *testing.T) {
	cases := []struct {
		name      string
		deviceID  uint64
		expiresAt int64
	}{
		{"zero", 0, 0},
		{"negative expiry", 1, -1},
		{"min int64 expiry", 1, math.MinInt64},
		{"max values", math.MaxUint64, math.MaxInt64},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := newTicketTestTicket(t, tc.deviceID, tc.expiresAt)

			var b [ticketPlainSize]byte
			marshalTicket(b[:], in)
			require.Equal(t, in, unmarshalTicket(b[:]))
		})
	}
}

// --------------------------------------------------------------
// Key management: rotation, wrong key, caller-owned key material
// --------------------------------------------------------------

func TestTicket_KeyRotation(t *testing.T) {
	k1, k2 := ticketTestKey(t), ticketTestKey(t)
	mk := func(cur byte, keys map[byte][]byte) *TicketSealer {
		s, err := NewTicketSealer(cur, keys)
		require.NoError(t, err)
		return s
	}
	before := mk(1, map[byte][]byte{1: k1})        // not rotated yet
	during := mk(2, map[byte][]byte{1: k1, 2: k2}) // new current key, previous kept
	after := mk(2, map[byte][]byte{2: k2})         // previous key dropped

	in := newTicketTestTicket(t, 42, ticketExpiry(20*time.Second))

	oldTk, err := before.Seal(in)
	require.NoError(t, err)
	require.Equal(t, byte(1), oldTk[0], "sealed under the current key ID")

	newTk, err := during.Seal(in)
	require.NoError(t, err)
	require.Equal(t, byte(2), newTk[0], "sealed under the current key ID")

	// While rotating, in-flight tickets (previous key) and new ones both open.
	for _, tk := range [][]byte{oldTk, newTk} {
		out, err := during.Open(42, tk)
		require.NoError(t, err)
		require.Equal(t, in, out)
	}

	// A replica that has not received the new key yet cannot open new tickets.
	requireTicketOpenFails(t, before, 42, newTk, ErrTicketInvalid)

	// Once the previous key is dropped, old tickets die and new ones keep working.
	requireTicketOpenFails(t, after, 42, oldTk, ErrTicketInvalid)
	out, err := after.Open(42, newTk)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

// Same key ID, different key material (e.g. misconfigured replica): the AEAD
// tag must not verify.
func TestTicket_Open_RejectsTicketSealedUnderDifferentKey(t *testing.T) {
	a := newTicketTestSealer(t, 1, 1)
	b := newTicketTestSealer(t, 1, 1)

	tk, err := a.Seal(newTicketTestTicket(t, 42, ticketExpiry(20*time.Second)))
	require.NoError(t, err)

	requireTicketOpenFails(t, b, 42, tk, ErrTicketInvalid)
}

// The sealer builds its AEADs up front, so callers may wipe their key
// material afterward without breaking it.
func TestNewTicketSealer_DoesNotRetainCallerKeys(t *testing.T) {
	key := ticketTestKey(t)
	keys := map[byte][]byte{1: key}
	s, err := NewTicketSealer(1, keys)
	require.NoError(t, err)

	in := newTicketTestTicket(t, 42, ticketExpiry(20*time.Second))
	tk, err := s.Seal(in)
	require.NoError(t, err)

	clear(key)
	delete(keys, 1)

	out, err := s.Open(42, tk)
	require.NoError(t, err)
	require.Equal(t, in, out)

	tk2, err := s.Seal(in)
	require.NoError(t, err)
	out, err = s.Open(42, tk2)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestNewTicketSealer_Validation(t *testing.T) {
	key := func(n int) []byte { return make([]byte, n) }

	cases := []struct {
		name    string
		cur     byte
		keys    map[byte][]byte
		wantErr bool
	}{
		{"single key", 1, map[byte][]byte{1: key(32)}, false},
		{"key ID 0", 0, map[byte][]byte{0: key(32)}, false},
		{"key ID 255", 255, map[byte][]byte{255: key(32)}, false},
		{"current plus previous key", 2, map[byte][]byte{1: key(32), 2: key(32)}, false},

		{"nil map", 1, nil, true},
		{"empty map", 1, map[byte][]byte{}, true},
		{"current key not configured", 3, map[byte][]byte{1: key(32), 2: key(32)}, true},

		{"nil key", 1, map[byte][]byte{1: nil}, true},
		{"AES-128 sized key", 1, map[byte][]byte{1: key(16)}, true},
		{"AES-192 sized key", 1, map[byte][]byte{1: key(24)}, true},
		{"31-byte key", 1, map[byte][]byte{1: key(31)}, true},
		{"33-byte key", 1, map[byte][]byte{1: key(33)}, true},
		{"bad previous key, good current key", 2, map[byte][]byte{1: key(16), 2: key(32)}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewTicketSealer(tc.cur, tc.keys)
			if tc.wantErr {
				require.Error(t, err)
				require.Nil(t, s)
				return
			}
			require.NoError(t, err)

			in := newTicketTestTicket(t, 42, ticketExpiry(20*time.Second))
			tk, err := s.Seal(in)
			require.NoError(t, err)
			require.Equal(t, tc.cur, tk[0])

			out, err := s.Open(42, tk)
			require.NoError(t, err)
			require.Equal(t, in, out)
		})
	}
}

// ---------------
// ParseTicketKeys
// ---------------

func TestParseTicketKeys_Valid(t *testing.T) {
	k1, k2 := ticketTestKey(t), ticketTestKey(t)
	enc := base64.StdEncoding.EncodeToString

	got, err := ParseTicketKeys([]string{
		"1:" + enc(k1),
		"  2:" + enc(k2) + "\n", // surrounding whitespace is trimmed
		"255:" + enc(k1),
	})
	require.NoError(t, err)
	require.Equal(t, map[byte][]byte{1: k1, 2: k2, 255: k1}, got)
}

func TestParseTicketKeys_EmptyListYieldsEmptyMap(t *testing.T) {
	for _, in := range [][]string{nil, {}} {
		got, err := ParseTicketKeys(in)
		require.NoError(t, err)
		require.Len(t, got, 0)

		// Such a config must not produce a usable sealer.
		s, err := NewTicketSealer(1, got)
		require.Error(t, err)
		require.Nil(t, s)
	}
}

func TestParseTicketKeys_RejectsMalformedEntries(t *testing.T) {
	enc := base64.StdEncoding.EncodeToString
	good := enc(ticketTestKey(t))
	// 0xfb 0xff ... encodes to '-'/'_' in the URL-safe alphabet.
	urlSafe := base64.URLEncoding.EncodeToString(bytes.Repeat([]byte{0xfb, 0xff}, 16))

	cases := []struct {
		name  string
		entry string
	}{
		{"empty entry", ""},
		{"missing separator", good},
		{"empty id", ":" + good},
		{"non-numeric id", "x:" + good},
		{"negative id", "-1:" + good},
		{"id above byte range", "256:" + good},
		{"invalid base64", "1:***not-base64***"},
		{"url-safe base64 alphabet", "1:" + urlSafe},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseTicketKeys([]string{tc.entry})
			require.Error(t, err)
			require.Nil(t, got)

			// One bad entry poisons the whole list, even after a good one.
			got, err = ParseTicketKeys([]string{"1:" + good, tc.entry})
			require.Error(t, err)
			require.Nil(t, got)
		})
	}
}

// ParseTicketKeys only decodes; key-length policy is NewTicketSealer's job.
func TestParseTicketKeys_FeedsNewTicketSealer(t *testing.T) {
	enc := base64.StdEncoding.EncodeToString

	keys, err := ParseTicketKeys([]string{
		"1:" + enc(ticketTestKey(t)),
		"2:" + enc(ticketTestKey(t)),
	})
	require.NoError(t, err)

	s, err := NewTicketSealer(2, keys)
	require.NoError(t, err)

	in := newTicketTestTicket(t, 42, ticketExpiry(20*time.Second))
	tk, err := s.Seal(in)
	require.NoError(t, err)
	out, err := s.Open(42, tk)
	require.NoError(t, err)
	require.Equal(t, in, out)

	for name, entry := range map[string]string{
		"16-byte key": "1:" + enc(make([]byte, 16)),
		"empty key":   "1:",
	} {
		keys, err := ParseTicketKeys([]string{entry})
		require.NoError(t, err, name)

		s, err := NewTicketSealer(1, keys)
		require.Error(t, err, name)
		require.Nil(t, s, name)
	}
}

// -----------
// Concurrency
// -----------

// ticketSealer is documented as read-only after construction, so one instance
// is shared by every connection goroutine. Run with -race.
func TestTicket_ConcurrentSealOpen(t *testing.T) {
	s := newTicketTestSealer(t, 2, 1, 2)

	const goroutines, iterations = 8, 200

	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := range goroutines {
		in := newTicketTestTicket(t, uint64(g), ticketExpiry(20*time.Second))

		wg.Add(1)
		go func(g int, in *Ticket) {
			defer wg.Done()

			for i := range iterations {
				tk, err := s.Seal(in)
				if err != nil {
					errs <- err
					return
				}
				out, err := s.Open(in.DeviceID, tk)
				if err != nil {
					errs <- err
					return
				}
				if !reflect.DeepEqual(in, out) {
					errs <- fmt.Errorf("goroutine %d, iteration %d: round trip mismatch", g, i)
					return
				}
			}
		}(g, in)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}
