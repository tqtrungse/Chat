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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func requireSignedTicketOpenFails(
	t *testing.T,
	v *TicketVerifier,
	deviceID uint64,
	wire []byte,
	want error,
	msgAndArgs ...any,
) {
	t.Helper()
	before := bytes.Clone(wire)
	out, err := v.Open(deviceID, wire)
	if out != nil {
		defer out.Clear()
	}
	require.ErrorIs(t, err, want, msgAndArgs...)
	require.Nil(t, out, msgAndArgs...)
	require.Equal(t, before, wire, "Open must not modify rejected input")
}

func TestNewTicketVerifier_Validation(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)
	_, publicKey := newTicketSignatureTestSigner(t, sealer, 1)

	verifier, err := NewTicketVerifier(nil, map[byte]ed25519.PublicKey{1: publicKey}, 5*time.Second)
	require.ErrorIs(t, err, ErrTicketSealerRequired)
	require.Nil(t, verifier)

	for _, keys := range []map[byte]ed25519.PublicKey{nil, {}} {
		verifier, err := NewTicketVerifier(sealer, keys, 5*time.Second)
		require.ErrorIs(t, err, ErrTicketPubKeysRequired)
		require.Nil(t, verifier)
	}

	for _, size := range []int{0, 1, 31, 33, 64} {
		t.Run(fmt.Sprintf("public key length %d", size), func(t *testing.T) {
			verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{
				1: publicKey,
				2: make(ed25519.PublicKey, size),
			}, 5*time.Second)
			require.Error(t, err)
			require.Nil(t, verifier)
		})
	}

	for _, id := range []byte{0, 1, 255} {
		t.Run(fmt.Sprintf("key ID %d", id), func(t *testing.T) {
			signer, publicKey := newTicketSignatureTestSigner(t, sealer, id)
			verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{id: publicKey}, 5*time.Second)
			require.NoError(t, err)
			in := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
			defer in.Clear()
			wire, err := signer.Seal(in)
			require.NoError(t, err)
			out, err := verifier.Open(in.DeviceID, wire)
			require.NoError(t, err)
			defer out.Clear()
			require.Equal(t, in, out)
		})
	}
}

func TestNewTicketVerifier_ClockSkew(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)
	_, publicKey := newTicketSignatureTestSigner(t, sealer, 1)
	keys := map[byte]ed25519.PublicKey{1: publicKey}

	for _, skew := range []time.Duration{0, time.Nanosecond, 500 * time.Millisecond, 5 * time.Second} {
		t.Run(skew.String(), func(t *testing.T) {
			verifier, err := NewTicketVerifier(sealer, keys, skew)
			require.NoError(t, err)
			require.NotNil(t, verifier)
			require.Equal(t, skew, verifier.clockSkew)
		})
	}
	for _, skew := range []time.Duration{-time.Nanosecond, -time.Second} {
		t.Run(skew.String(), func(t *testing.T) {
			verifier, err := NewTicketVerifier(sealer, keys, skew)
			require.ErrorIs(t, err, ErrTicketClockSkewRequired)
			require.Nil(t, verifier)
		})
	}
}

func TestTicketVerifier_CheckExpiry_BoundariesAndOwnership(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)
	_, publicKey := newTicketSignatureTestSigner(t, sealer, 1)
	expiresAt := time.Unix(1_800_000_000, 0)

	cases := []struct {
		name    string
		skew    time.Duration
		offset  time.Duration
		wantErr error
	}{
		{"before expiry without skew", 0, -time.Nanosecond, nil},
		{"at expiry without skew", 0, 0, nil},
		{"after expiry without skew", 0, time.Nanosecond, ErrTicketExpired},
		{"inside skew", 5 * time.Second, 3 * time.Second, nil},
		{"just before skew deadline", 5 * time.Second, 5*time.Second - time.Nanosecond, nil},
		{"at skew deadline", 5 * time.Second, 5 * time.Second, nil},
		{"just after skew deadline", 5 * time.Second, 5*time.Second + time.Nanosecond, ErrTicketExpired},
		{"long expired", 5 * time.Second, time.Hour, ErrTicketExpired},
		{"fractional skew before deadline", 500 * time.Millisecond, 499 * time.Millisecond, nil},
		{"fractional skew at deadline", 500 * time.Millisecond, 500 * time.Millisecond, nil},
		{"fractional skew after deadline", 500 * time.Millisecond, 500*time.Millisecond + time.Nanosecond, ErrTicketExpired},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{1: publicKey}, tc.skew)
			require.NoError(t, err)
			ticket := newTicketTestTicket(t, 42, expiresAt.Unix())
			before := *ticket

			err = verifier.CheckExpiry(ticket, expiresAt.Add(tc.offset))
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, before, *ticket, "CheckExpiry must not mutate or clear the caller's ticket")
		})
	}

	verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{1: publicKey}, 0)
	require.NoError(t, err)
	require.ErrorIs(t, verifier.CheckExpiry(nil, expiresAt), ErrTicketInvalid)
}

func TestTicketVerifier_Open_EnforcesConfiguredSkew(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)
	signer, publicKey := newTicketSignatureTestSigner(t, sealer, 1)
	in := newTicketTestTicket(t, 42, ticketExpiry(-30*time.Second))
	wire, err := signer.Seal(in)
	require.NoError(t, err)

	cases := []struct {
		name    string
		skew    time.Duration
		wantErr error
	}{
		{"zero skew", 0, ErrTicketExpired},
		{"expired beyond skew", 5 * time.Second, ErrTicketExpired},
		{"expired inside skew", 2 * time.Minute, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{1: publicKey}, tc.skew)
			require.NoError(t, err)
			if tc.wantErr != nil {
				requireSignedTicketOpenFails(t, verifier, in.DeviceID, wire, tc.wantErr)
				return
			}
			before := bytes.Clone(wire)
			out, err := verifier.Open(in.DeviceID, wire)
			require.NoError(t, err)
			defer out.Clear()
			require.Equal(t, in, out)
			require.Equal(t, before, wire)
		})
	}
}

func TestTicketVerifier_CheckExpiry_RechecksOpenedTicket(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)
	signer, publicKey := newTicketSignatureTestSigner(t, sealer, 1)
	verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{1: publicKey}, 5*time.Second)
	require.NoError(t, err)
	in := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
	wire, err := signer.Seal(in)
	require.NoError(t, err)
	out, err := verifier.Open(in.DeviceID, wire)
	require.NoError(t, err)
	defer out.Clear()
	before := *out
	deadline := time.Unix(out.ExpiresAt, 0).Add(5 * time.Second)

	require.NoError(t, verifier.CheckExpiry(out, deadline))
	require.ErrorIs(t, verifier.CheckExpiry(out, deadline.Add(time.Nanosecond)), ErrTicketExpired)
	require.Equal(t, before, *out, "the caller still owns and clears the rejected ticket")
}

func TestNewTicketVerifier_CopiesCallerPublicKeys(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)
	signer, publicKey := newTicketSignatureTestSigner(t, sealer, 7)
	keys := map[byte]ed25519.PublicKey{7: publicKey}
	verifier, err := NewTicketVerifier(sealer, keys, 5*time.Second)
	require.NoError(t, err)

	clear(publicKey)
	delete(keys, 7)
	keys[9] = make(ed25519.PublicKey, ed25519.PublicKeySize)

	in := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
	defer in.Clear()
	wire, err := signer.Seal(in)
	require.NoError(t, err)
	out, err := verifier.Open(in.DeviceID, wire)
	require.NoError(t, err)
	defer out.Clear()
	require.Equal(t, in, out)
	require.Len(t, verifier.publicKeys, 1)
}

func TestTicketVerifier_RoundTripAcrossIndependentInstances(t *testing.T) {
	key := ticketTestKey(t)
	iamSealer, err := NewTicketSealer(3, map[byte][]byte{3: key})
	require.NoError(t, err)
	hubSealer, err := NewTicketSealer(3, map[byte][]byte{3: bytes.Clone(key)})
	require.NoError(t, err)
	clear(key)
	signer, publicKey := newTicketSignatureTestSigner(t, iamSealer, 7)
	verifier, err := NewTicketVerifier(hubSealer, map[byte]ed25519.PublicKey{7: publicKey}, 5*time.Second)
	require.NoError(t, err)

	for _, id := range []uint64{0, 1, 42, math.MaxUint64} {
		in := newTicketTestTicket(t, id, ticketExpiry(time.Hour))
		wire, err := signer.Seal(in)
		require.NoError(t, err)
		before := bytes.Clone(wire)
		out, err := verifier.Open(id, wire)
		require.NoError(t, err)
		require.Equal(t, in, out)
		require.Equal(t, before, wire, "Open must not modify the nio read buffer")
		out.Clear()
		in.Clear()
	}
}

func TestTicketVerifier_Open_RejectsEveryBitFlip(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)
	signer, publicKey := newTicketSignatureTestSigner(t, sealer, 1)
	// ID 3 deliberately maps to the same public key. Changing ID 1 to ID 3
	// must still fail because the signing key ID itself is signed.
	verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{
		1: publicKey,
		3: publicKey,
	}, 5*time.Second)
	require.NoError(t, err)
	in := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
	defer in.Clear()
	wire, err := signer.Seal(in)
	require.NoError(t, err)

	for i := range wire {
		for bit := range 8 {
			bad := bytes.Clone(wire)
			bad[i] ^= 1 << bit
			requireSignedTicketOpenFails(t, verifier, in.DeviceID, bad,
				ErrTicketInvalid, "byte %d bit %d", i, bit)
		}
	}
	out, err := verifier.Open(in.DeviceID, wire)
	require.NoError(t, err)
	defer out.Clear()
	require.Equal(t, in, out)
}

func TestTicketVerifier_Open_RejectsMalformedEnvelope(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)
	signer, publicKey := newTicketSignatureTestSigner(t, sealer, 7)
	verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{7: publicKey}, 5*time.Second)
	require.NoError(t, err)
	in := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
	defer in.Clear()
	wire, err := signer.Seal(in)
	require.NoError(t, err)

	requireSignedTicketOpenFails(t, verifier, in.DeviceID, nil, ErrTicketInvalid)
	for n := range SignedTicketSize {
		requireSignedTicketOpenFails(t, verifier, in.DeviceID, wire[:n], ErrTicketInvalid, "truncated to %d", n)
	}
	requireSignedTicketOpenFails(t, verifier, in.DeviceID, append(bytes.Clone(wire), 0), ErrTicketInvalid)
	requireSignedTicketOpenFails(t, verifier, in.DeviceID, append(bytes.Clone(wire), wire...), ErrTicketInvalid)

	badVersion := bytes.Clone(wire)
	badVersion[0] = 1
	requireSignedTicketOpenFails(t, verifier, in.DeviceID, badVersion, ErrTicketInvalid)
	unknownKey := bytes.Clone(wire)
	unknownKey[1] = 8
	requireSignedTicketOpenFails(t, verifier, in.DeviceID, unknownKey, ErrTicketInvalid)

	// The old bare AES format must not bypass IAM signature verification.
	requireSignedTicketOpenFails(t, verifier, in.DeviceID, wire[2:239], ErrTicketInvalid)
}

func TestTicketVerifier_Open_RejectsWrongPublicKey(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)
	signer, _ := newTicketSignatureTestSigner(t, sealer, 7)
	_, wrongPublic := newTicketSignatureTestSigner(t, sealer, 7)
	verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{7: wrongPublic}, 5*time.Second)
	require.NoError(t, err)
	in := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
	defer in.Clear()
	wire, err := signer.Seal(in)
	require.NoError(t, err)
	requireSignedTicketOpenFails(t, verifier, in.DeviceID, wire, ErrTicketInvalid)
}

func TestTicketVerifier_Open_AuthenticatesBeforeExpiry(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)
	signer, publicKey := newTicketSignatureTestSigner(t, sealer, 7)
	verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{7: publicKey}, 5*time.Second)
	require.NoError(t, err)
	in := newTicketTestTicket(t, 42, ticketExpiry(-time.Hour))
	defer in.Clear()
	wire, err := signer.Seal(in)
	require.NoError(t, err)

	requireSignedTicketOpenFails(t, verifier, 42, wire, ErrTicketExpired)
	requireSignedTicketOpenFails(t, verifier, 43, wire, ErrTicketInvalid)
	bad := bytes.Clone(wire)
	bad[len(bad)-1] ^= 1
	requireSignedTicketOpenFails(t, verifier, 42, bad, ErrTicketInvalid)
}

func TestTicketVerifier_Open_StillChecksAESAuthentication(t *testing.T) {
	iamSealer := newTicketTestSealer(t, 1, 1)
	signer, publicKey := newTicketSignatureTestSigner(t, iamSealer, 7)
	verifier, err := NewTicketVerifier(iamSealer, map[byte]ed25519.PublicKey{7: publicKey}, 5*time.Second)
	require.NoError(t, err)
	in := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
	defer in.Clear()
	wire, err := signer.Seal(in)
	require.NoError(t, err)

	// A valid IAM signature must not replace the inner GCM authentication.
	for _, offset := range []int{2, 3, 15, 238} {
		bad := bytes.Clone(wire)
		bad[offset] ^= 1
		message := append([]byte(ticketSigningDomain), bad[:239]...)
		copy(bad[239:], ed25519.Sign(signer.privateKey, message))
		require.True(t, ed25519.Verify(publicKey, message, bad[239:]))
		requireSignedTicketOpenFails(t, verifier, in.DeviceID, bad, ErrTicketInvalid)
	}

	// A Hub configured with different AES material also rejects a signed ticket.
	wrongAES := newTicketTestSealer(t, 1, 1)
	wrongVerifier, err := NewTicketVerifier(wrongAES, map[byte]ed25519.PublicKey{7: publicKey}, 5*time.Second)
	require.NoError(t, err)
	requireSignedTicketOpenFails(t, wrongVerifier, in.DeviceID, wire, ErrTicketInvalid)
	requireSignedTicketOpenFails(t, verifier, 43, wire, ErrTicketInvalid)
}

func TestTicketVerifier_Open_RejectsForgeryWithKnownAESKey(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)
	signer, publicKey := newTicketSignatureTestSigner(t, sealer, 7)
	verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{7: publicKey}, 5*time.Second)
	require.NoError(t, err)
	in := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
	defer in.Clear()
	wire, err := signer.Seal(in)
	require.NoError(t, err)

	// Simulate a holder of the AES key replacing the device identity and
	// session secrets. The device ID remains that of the victim.
	forged := newTicketTestTicket(t, in.DeviceID, in.ExpiresAt)
	defer forged.Clear()
	forgedAES, err := sealer.Seal(forged)
	require.NoError(t, err)
	openedAES, err := sealer.Open(in.DeviceID, forgedAES)
	require.NoError(t, err, "AES authentication alone accepts this forgery")
	defer openedAES.Clear()
	require.Equal(t, forged, openedAES)

	bad := bytes.Clone(wire)
	copy(bad[2:239], forgedAES)
	requireSignedTicketOpenFails(t, verifier, in.DeviceID, bad, ErrTicketInvalid)
}

func TestTicketVerifier_SigningKeyRotation(t *testing.T) {
	sealer := newTicketTestSealer(t, 3, 3)
	signerA, publicA := newTicketSignatureTestSigner(t, sealer, 1)
	signerB, publicB := newTicketSignatureTestSigner(t, sealer, 2)
	before, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{1: publicA}, 5*time.Second)
	require.NoError(t, err)

	during, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{1: publicA, 2: publicB}, 5*time.Second)
	require.NoError(t, err)

	after, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{2: publicB}, 5*time.Second)
	require.NoError(t, err)

	in := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
	defer in.Clear()

	oldWire, err := signerA.Seal(in)
	require.NoError(t, err)

	newWire, err := signerB.Seal(in)
	require.NoError(t, err)
	require.Equal(t, byte(1), oldWire[1])
	require.Equal(t, byte(2), newWire[1])
	require.Equal(t, byte(3), oldWire[2], "AES ID is independent of signing ID")
	require.Equal(t, byte(3), newWire[2])

	for _, tc := range []struct {
		name     string
		verifier *TicketVerifier
		wire     []byte
		valid    bool
	}{
		{"before old", before, oldWire, true},
		{"before new", before, newWire, false},
		{"during old", during, oldWire, true},
		{"during new", during, newWire, true},
		{"after old", after, oldWire, false},
		{"after new", after, newWire, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.valid {
				requireSignedTicketOpenFails(t, tc.verifier, in.DeviceID, tc.wire, ErrTicketInvalid)
				return
			}
			out, err := tc.verifier.Open(in.DeviceID, tc.wire)
			require.NoError(t, err)
			defer out.Clear()
			require.Equal(t, in, out)
		})
	}
}

func TestTicketVerifier_DeviceIdentitySignsWholeEnvelope(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)
	signer, iamPublic := newTicketSignatureTestSigner(t, sealer, 7)
	verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{7: iamPublic}, 5*time.Second)
	require.NoError(t, err)

	deviceSeed := ticketTestKey(t)
	devicePrivate := ed25519.NewKeyFromSeed(deviceSeed)
	clear(deviceSeed)
	defer clear(devicePrivate)

	devicePublic := devicePrivate.Public().(ed25519.PublicKey)
	in := newTicketTestTicket(t, 42, ticketExpiry(time.Hour))
	defer in.Clear()
	copy(in.IdentityPub[:], devicePublic)

	wire, err := signer.Seal(in)
	require.NoError(t, err)
	deviceSignature := ed25519.Sign(devicePrivate, wire)

	out, err := verifier.Open(in.DeviceID, wire)
	require.NoError(t, err)
	defer out.Clear()

	identity := ed25519.PublicKey(out.IdentityPub[:])
	require.True(t, ed25519.Verify(identity, wire, deviceSignature))
	require.False(t, ed25519.Verify(identity, wire[2:239], deviceSignature))
	require.False(t, ed25519.Verify(identity, []byte(base64.StdEncoding.EncodeToString(wire)), deviceSignature))
	require.False(t, ed25519.Verify(iamPublic, wire, deviceSignature), "IAM signing identity and device identity are independent")
}

func TestSignedTicket_ConcurrentSealOpen(t *testing.T) {
	sealer := newTicketTestSealer(t, 1, 1)
	signer, publicKey := newTicketSignatureTestSigner(t, sealer, 7)
	verifier, err := NewTicketVerifier(sealer, map[byte]ed25519.PublicKey{7: publicKey}, 5*time.Second)
	require.NoError(t, err)

	const workers, iterations = 8, 64
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for worker := range workers {
		in := newTicketTestTicket(t, uint64(worker), ticketExpiry(time.Hour))
		wg.Go(func() {
			defer in.Clear()
			for i := range iterations {
				wire, err2 := signer.Seal(in)
				if err2 != nil {
					errs <- fmt.Errorf("worker %d iteration %d: Seal: %w", worker, i, err2)
					return
				}

				before := bytes.Clone(wire)
				out, err2 := verifier.Open(in.DeviceID, wire)
				if err2 != nil {
					errs <- fmt.Errorf("worker %d iteration %d: Open: %w", worker, i, err2)
					return
				}

				matches := *in == *out
				out.Clear()
				if !matches || !bytes.Equal(before, wire) {
					errs <- fmt.Errorf("worker %d iteration %d: data mismatch or input mutation", worker, i)
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
