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

package connection

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"testing"
	"time"

	sharedsession "xxx/internal/shared/session"

	"github.com/stretchr/testify/require"
)

type expiryTestVerifier struct {
	verifier   *sharedsession.TicketVerifier
	prepared   *sharedsession.Ticket
	now        time.Time
	openCalls  int
	checkCalls int
	checked    *sharedsession.Ticket
	passedTime time.Time
}

func (v *expiryTestVerifier) Open(uint64, []byte) (*sharedsession.Ticket, error) {
	v.openCalls++
	return v.prepared, nil
}

func (v *expiryTestVerifier) CheckExpiry(ticket *sharedsession.Ticket, now time.Time) error {
	v.checkCalls++
	v.checked = ticket
	v.passedTime = now
	return v.verifier.CheckExpiry(ticket, v.now)
}

type expiryTestSigner struct{ calls int }

func (s *expiryTestSigner) Verify([]byte, []byte, []byte) error {
	s.calls++
	return nil
}

func newExpiryTestVerifier(t *testing.T, skew time.Duration) *sharedsession.TicketVerifier {
	t.Helper()
	key := bytes.Repeat([]byte{0xA1}, 32)
	seed := bytes.Repeat([]byte{0xB2}, ed25519.SeedSize)
	defer clear(key)
	defer clear(seed)
	privateKey := ed25519.NewKeyFromSeed(seed)
	defer clear(privateKey)
	sealer, err := sharedsession.NewTicketSealer(1, map[byte][]byte{1: key})
	require.NoError(t, err)
	verifier, err := sharedsession.NewTicketVerifier(
		sealer,
		map[byte]ed25519.PublicKey{1: privateKey.Public().(ed25519.PublicKey)},
		skew,
	)
	require.NoError(t, err)
	return verifier
}

func TestRouter_ActivateConn_RechecksExpiryWithoutPreparingAgain(t *testing.T) {
	expiresAt := time.Unix(1_800_000_000, 0)
	deadline := expiresAt.Add(5 * time.Second)
	cases := []struct {
		name    string
		now     time.Time
		wantErr error
	}{
		{"before expiry", expiresAt.Add(-time.Second), ErrRouterClosed},
		{"inside skew", expiresAt.Add(3 * time.Second), ErrRouterClosed},
		{"at skew deadline", deadline, ErrRouterClosed},
		{"expired after preparation", deadline.Add(time.Nanosecond), sharedsession.ErrTicketExpired},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ticket := &sharedsession.Ticket{DeviceID: 42, ExpiresAt: expiresAt.Unix()}
			ticket.ActivationKey[0] = 0xAA
			ticket.Keys.RecvEnc[0] = 0xBB
			defer ticket.Clear()
			verifier := &expiryTestVerifier{
				verifier: newExpiryTestVerifier(t, 5*time.Second),
				prepared: ticket,
				now:      tc.now,
			}
			signer := new(expiryTestSigner)
			router := &Router{ticketVerifier: verifier, signer: signer}
			token := binary.LittleEndian.AppendUint64(nil, ticket.DeviceID)
			prepared, err := router.PrepareActivation(token, []byte("already verified envelope"), []byte("identity signature"))
			require.NoError(t, err)
			require.Same(t, ticket, prepared)

			// Valid tickets reach the next gate. A closed router lets this test
			// inspect expiry without starting presence workers or registering a conn.
			router.meta.Store(closed | 3)
			before := *ticket
			started := time.Now()
			err = router.ActivateConn(context.Background(), prepared, nil, nil)
			finished := time.Now()
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, uint32(closed|3), router.meta.Load())
			require.Equal(t, 1, verifier.openCalls, "ActivateConn must not reopen the ticket")
			require.Equal(t, 1, signer.calls, "ActivateConn must not verify the identity signature again")
			require.Equal(t, 1, verifier.checkCalls)
			require.Same(t, prepared, verifier.checked)
			require.False(t, verifier.passedTime.Before(started))
			require.False(t, verifier.passedTime.After(finished))
			require.Equal(t, before, *ticket, "the caller retains ownership on rejection")
		})
	}
}

func TestRouter_ActivateConn_ExpiredTicketDoesNotReserveSlot(t *testing.T) {
	ticket := &sharedsession.Ticket{DeviceID: 42, ExpiresAt: 1_800_000_000}
	defer ticket.Clear()
	verifier := &expiryTestVerifier{
		verifier: newExpiryTestVerifier(t, 0),
		now:      time.Unix(ticket.ExpiresAt, 1),
	}
	router := &Router{ticketVerifier: verifier, maxConns: 10}
	router.meta.Store(3)

	// A nil connection deliberately makes any attempt to register it fail:
	// expiry must reject before touching the conn or creating a session.
	err := router.ActivateConn(context.Background(), ticket, nil, nil)
	require.ErrorIs(t, err, sharedsession.ErrTicketExpired)
	require.Equal(t, uint32(3), router.meta.Load())
	require.Equal(t, uint32(3), router.CurConns())
	require.Equal(t, 0, verifier.openCalls)
	require.Equal(t, 1, verifier.checkCalls)
}

func TestRouter_ActivateConn_RejectsNilTicket(t *testing.T) {
	verifier := &expiryTestVerifier{verifier: newExpiryTestVerifier(t, 0), now: time.Now()}
	router := &Router{ticketVerifier: verifier}
	err := router.ActivateConn(context.Background(), nil, nil, nil)
	require.ErrorIs(t, err, sharedsession.ErrTicketInvalid)
	require.Zero(t, router.meta.Load())
	require.Equal(t, 1, verifier.checkCalls)
	require.Nil(t, verifier.checked)
}
