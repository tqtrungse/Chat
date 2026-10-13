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

package connection

import (
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"xxx/internal/hub/domain/message"
	shareddevice "xxx/internal/shared/device"
	sharedsession "xxx/internal/shared/session"
	sharedticket "xxx/internal/shared/ticket"

	"xxx/pkg/nio"

	"github.com/stretchr/testify/require"
)

func newLifecycleForTest(
	t *testing.T,
	ticker time.Duration,
	registry *connectionRegistry,
	verifier TicketVerifier,
	signer Signer,
	cache DistributedCache,
) (*connectionLifecycle, *presenceSync) {
	t.Helper()
	logger := newTestLogger(t)
	pool, _ := newTestPool(t)
	cb := testCircuitBreaker{}
	presence, _ := newTestPresenceSync(t, PresenceSyncConfig{
		BatchSize: 1,
		Linger:    time.Hour,
	}, cache, cb, logger, pool)
	lifecycle := newConnectionLifecycle(signer, verifier, ticker, registry, logger, pool, presence)
	return lifecycle, presence
}

func testTicket(deviceID uint64) *sharedticket.Ticket {
	ticket := &sharedticket.Ticket{
		DeviceID:  deviceID,
		ExpiresAt: time.Now().Add(time.Minute).Unix(),
	}
	for i := range ticket.Keys.RecvEnc {
		ticket.Keys.RecvEnc[i] = byte(i + 1)
		ticket.Keys.SendEnc[i] = byte(i + 2)
		ticket.Keys.RecvMac[i] = byte(i + 3)
		ticket.Keys.SendMac[i] = byte(i + 4)
		ticket.ActivationKey[i] = byte(i + 5)
		ticket.IdentityPub[i] = byte(i + 6)
	}
	return ticket
}

func awaitTestResult[T any](t *testing.T, result <-chan T) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for asynchronous connection operation")
		var zero T
		return zero
	}
}

func TestLifecycle_PrepareActivationValidatesTokenBeforeOpeningTicket(t *testing.T) {
	openCalls := 0
	lifecycle := newConnectionLifecycle(
		testSigner{},
		testTicketVerifier{open: func(uint64, []byte) (*sharedticket.Ticket, error) {
			openCalls++
			return nil, nil
		}},
		time.Second,
		newConnectionRegistry(1),
		newTestLogger(t),
		nil,
		&presenceSync{},
	)

	got, err := lifecycle.PrepareActivation([]byte{1}, []byte("ticket"), []byte("sign"))

	require.ErrorIs(t, err, ErrActivationInvalid)
	require.Nil(t, got)
	require.Zero(t, openCalls)
}

func TestLifecycle_PrepareActivationOpensAndVerifiesTicket(t *testing.T) {
	verifierTicket := testTicket(0x12A)
	var openedDeviceID uint64
	var openedTicket []byte
	var verifiedPub, verifiedPayload, verifiedSign []byte
	ticketBlob := []byte("ticket envelope")
	signature := []byte("device signature")
	lifecycle := newConnectionLifecycle(
		testSigner{verify: func(identityPub, payload, sign []byte) error {
			verifiedPub = append([]byte(nil), identityPub...)
			verifiedPayload = append([]byte(nil), payload...)
			verifiedSign = append([]byte(nil), sign...)
			return nil
		}},
		testTicketVerifier{open: func(id uint64, ticket []byte) (*sharedticket.Ticket, error) {
			openedDeviceID = id
			openedTicket = append([]byte(nil), ticket...)
			return verifierTicket, nil
		}},
		time.Second,
		newConnectionRegistry(1),
		newTestLogger(t),
		nil,
		&presenceSync{},
	)
	token := make([]byte, tokenSize)
	binary.LittleEndian.PutUint64(token, verifierTicket.DeviceID)

	got, err := lifecycle.PrepareActivation(token, ticketBlob, signature)

	require.NoError(t, err)
	require.Same(t, verifierTicket, got)
	require.Equal(t, verifierTicket.DeviceID, openedDeviceID)
	require.Equal(t, ticketBlob, openedTicket)
	require.Equal(t, verifierTicket.IdentityPub[:], verifiedPub)
	require.Equal(t, ticketBlob, verifiedPayload)
	require.Equal(t, signature, verifiedSign)
}

func TestLifecycle_PrepareActivationClearsTicketWhenSignatureFails(t *testing.T) {
	wantErr := errors.New("bad device signature")
	ticket := testTicket(0x12A)
	token := make([]byte, tokenSize)
	binary.LittleEndian.PutUint64(token, ticket.DeviceID)
	lifecycle := newConnectionLifecycle(
		testSigner{verify: func([]byte, []byte, []byte) error { return wantErr }},
		testTicketVerifier{open: func(uint64, []byte) (*sharedticket.Ticket, error) { return ticket, nil }},
		time.Second,
		newConnectionRegistry(1),
		newTestLogger(t),
		nil,
		&presenceSync{},
	)

	got, err := lifecycle.PrepareActivation(token, []byte("ticket"), []byte("bad"))

	require.ErrorIs(t, err, wantErr)
	require.Nil(t, got)
	require.Zero(t, ticket.Keys)
	require.Zero(t, ticket.ActivationKey)
	require.Zero(t, ticket.IdentityPub)
	require.Equal(t, uint64(0x12A), ticket.DeviceID)
	require.NotZero(t, ticket.ExpiresAt)
}

func TestLifecycle_ActivateConnValidatesExpiryBeforeRegistering(t *testing.T) {
	wantErr := errors.New("ticket expired")
	var checkedTicket *sharedticket.Ticket
	verifier := testTicketVerifier{checkExpiry: func(ticket *sharedticket.Ticket, _ time.Time) error {
		checkedTicket = ticket
		return wantErr
	}}
	registry := newConnectionRegistry(1)
	conn, _ := newTestConn()
	lifecycle, _ := newLifecycleForTest(t, time.Second, registry, verifier, testSigner{}, &testDistributedCache{})
	ticket := testTicket(1)
	keys := &ticket.Keys

	err := lifecycle.ActivateConn(context.Background(), ticket, keys, conn, func(shareddevice.ID) {})

	require.ErrorIs(t, err, wantErr)
	require.Same(t, ticket, checkedTicket)
	require.Zero(t, registry.CurConns())
	require.Nil(t, conn.Context())
}

func TestLifecycle_ActivateConnRegistersAndPublishesPresence(t *testing.T) {
	registry := newConnectionRegistry(2)
	added := make(chan []shareddevice.ID, 1)
	cache := &testDistributedCache{add: func(_ context.Context, ids []shareddevice.ID) (map[shareddevice.ID][]message.Date, error) {
		added <- append([]shareddevice.ID(nil), ids...)
		return nil, nil
	}}
	lifecycle, _ := newLifecycleForTest(t, 5*time.Second, registry, testTicketVerifier{}, testSigner{}, cache)
	conn, _ := newTestConn()
	ticket := testTicket(42)
	keys := ticket.Keys
	callback := make(chan shareddevice.ID, 1)

	err := lifecycle.ActivateConn(context.Background(), ticket, &keys, conn, func(id shareddevice.ID) { callback <- id })
	require.NoError(t, err)
	require.EqualValues(t, 1, registry.CurConns())
	active, exists := registry.Lookup(shareddevice.ID(ticket.DeviceID))
	require.True(t, exists)
	require.Same(t, conn, active)
	require.Equal(t, keys, conn.Context().(*Session).keys)
	require.Equal(t, 5*time.Second, lifecycle.Tick())

	require.ElementsMatch(t, []shareddevice.ID{shareddevice.ID(ticket.DeviceID)}, awaitTestResult(t, added))
	require.Equal(t, shareddevice.ID(ticket.DeviceID), awaitTestResult(t, callback))
}

func TestLifecycle_ActivateConnReplacesAndClosesOldConnection(t *testing.T) {
	registry := newConnectionRegistry(3)
	added := make(chan []shareddevice.ID, 2)
	cache := &testDistributedCache{add: func(_ context.Context, ids []shareddevice.ID) (map[shareddevice.ID][]message.Date, error) {
		added <- append([]shareddevice.ID(nil), ids...)
		return nil, nil
	}}
	lifecycle, _ := newLifecycleForTest(t, 0, registry, testTicketVerifier{}, testSigner{}, cache)
	oldConn, oldState := newTestConn()
	newConn, _ := newTestConn()
	ticket := testTicket(77)
	keys := ticket.Keys
	activated := make(chan shareddevice.ID, 2)

	require.NoError(t, lifecycle.ActivateConn(context.Background(), ticket, &keys, oldConn, func(id shareddevice.ID) { activated <- id }))
	_ = awaitTestResult(t, added)
	_ = awaitTestResult(t, activated)

	keys2 := ticket.Keys
	require.NoError(t, lifecycle.ActivateConn(context.Background(), ticket, &keys2, newConn, func(id shareddevice.ID) { activated <- id }))
	require.EqualValues(t, 1, oldState.closeCount.Load())
	require.True(t, oldConn.Context().(*Session).isClosed())
	require.Same(t, newConn, mustLookupConnection(t, registry, shareddevice.ID(ticket.DeviceID)))
	_ = awaitTestResult(t, added)
	_ = awaitTestResult(t, activated)
	require.Equal(t, defaultTickerDuration, lifecycle.Tick())
}

func TestLifecycle_ActivateConnClosesConnectionWhenPresenceAddFails(t *testing.T) {
	addErr := errors.New("cache unavailable")
	cache := &testDistributedCache{add: func(context.Context, []shareddevice.ID) (map[shareddevice.ID][]message.Date, error) {
		return nil, addErr
	}}
	lifecycle, _ := newLifecycleForTest(t, time.Second, newConnectionRegistry(1), testTicketVerifier{}, testSigner{}, cache)
	conn, state := newTestConn()
	callbackCalled := atomic.Bool{}
	ticket := testTicket(5)
	keys := ticket.Keys

	require.NoError(t, lifecycle.ActivateConn(context.Background(), ticket, &keys, conn, func(shareddevice.ID) { callbackCalled.Store(true) }))
	deadline := time.Now().Add(3 * time.Second)
	for state.closeCount.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	require.EqualValues(t, 1, state.closeCount.Load())
	require.False(t, callbackCalled.Load())
}

func TestLifecycle_RemoveConnOnlyPublishesForRegisteredConnection(t *testing.T) {
	deleted := make(chan []shareddevice.ID, 1)
	cache := &testDistributedCache{del: func(_ context.Context, ids []shareddevice.ID) error {
		deleted <- append([]shareddevice.ID(nil), ids...)
		return nil
	}}
	registry := newConnectionRegistry(2)
	lifecycle, _ := newLifecycleForTest(t, time.Second, registry, testTicketVerifier{}, testSigner{}, cache)
	conn, _ := newTestConn()
	other, _ := newTestConn()
	id := shareddevice.ID(61)
	session := newSession(sharedsession.Keys{}, id, time.Now().Unix())
	_, err := registry.Replace(id, conn, session)
	require.NoError(t, err)

	lifecycle.RemoveConn(context.Background(), other)
	require.Empty(t, deleted)
	require.EqualValues(t, 1, registry.CurConns())

	lifecycle.RemoveConn(context.Background(), conn)
	require.ElementsMatch(t, []shareddevice.ID{id}, awaitTestResult(t, deleted))
	require.EqualValues(t, 0, registry.CurConns())
	lifecycle.RemoveConn(context.Background(), conn)
	require.Empty(t, deleted)
}

func TestLifecycle_ForceCloseClosesConnection(t *testing.T) {
	conn, state := newTestConn()
	lifecycle := newConnectionLifecycle(testSigner{}, testTicketVerifier{}, time.Second, newConnectionRegistry(1), newTestLogger(t), nil, &presenceSync{})

	lifecycle.ForceClose(conn)

	require.EqualValues(t, 1, state.closeCount.Load())
}

func mustLookupConnection(t *testing.T, registry *connectionRegistry, id shareddevice.ID) nio.Conn {
	t.Helper()
	conn, ok := registry.Lookup(id)
	require.True(t, ok)
	return conn
}
