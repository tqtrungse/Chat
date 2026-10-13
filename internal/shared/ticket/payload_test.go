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

package ticket

import (
	"bytes"
	"math"
	"testing"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
)

func TestTicketPayload_MarshalWireLayout(t *testing.T) {
	in := &Ticket{
		DeviceID:  0x0102030405060708,
		ExpiresAt: 0x1112131415161718,
	}
	for i := range in.Keys.RecvEnc {
		in.Keys.RecvEnc[i] = 0x21
		in.Keys.SendEnc[i] = 0x32
		in.Keys.RecvMac[i] = 0x43
		in.Keys.SendMac[i] = 0x54
		in.ActivationKey[i] = 0x65
		in.IdentityPub[i] = 0x76
	}
	defer in.Clear()

	want := make([]byte, plainSize)
	copy(want[0:8], []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01})
	copy(want[8:16], []byte{0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11})
	// Use distinct fixed byte values so a swapped or overlapping field fails.
	for i := 16; i < 48; i++ {
		want[i] = 0x21
	}
	for i := 48; i < 80; i++ {
		want[i] = 0x32
	}
	for i := 80; i < 112; i++ {
		want[i] = 0x43
	}
	for i := 112; i < 144; i++ {
		want[i] = 0x54
	}
	for i := 144; i < 176; i++ {
		want[i] = 0x65
	}
	for i := 176; i < 208; i++ {
		want[i] = 0x76
	}

	got := make([]byte, plainSize)
	marshal(got, in)
	require.Equal(t, want, got)

	out := unmarshal(want)
	defer out.Clear()
	require.Equal(t, in, out)
}

func TestTicketPayload_UnmarshalCopiesInput(t *testing.T) {
	plain := bytes.Repeat([]byte{0xA5}, plainSize)
	plain[0] = 0x2A
	plain[1] = 0x01
	clear(plain[2:8])
	plain[8] = 0x34
	plain[9] = 0x12
	clear(plain[10:16])

	got := unmarshal(plain)
	defer got.Clear()
	before := *got
	clear(plain)

	require.Equal(t, before, *got, "the decoded ticket must own its key material")
	require.Equal(t, uint64(0x12A), got.DeviceID)
	require.Equal(t, int64(0x1234), got.ExpiresAt)
}

func TestTicketPayload_AADUsesLittleEndianDeviceID(t *testing.T) {
	got := aad(0x0102030405060708)
	require.Equal(t, []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01}, got)
}

func TestTicketPayload_RoundTripExtremes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		deviceID  uint64
		expiresAt int64
	}{
		{"zero", 0, 0},
		{"negative expiry", 1, -1},
		{"minimum expiry", 1, math.MinInt64},
		{"maximum values", math.MaxUint64, math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := newTicketTestTicket(t, tc.deviceID, tc.expiresAt)
			payload := make([]byte, plainSize)
			marshal(payload, in)

			out := unmarshal(payload)
			defer out.Clear()
			require.Equal(t, in, out)
		})
	}
}

func TestSignedTicketEnvelope_ProtocolConstantsAndSigningMessage(t *testing.T) {
	require.Equal(t, byte(2), SignedVersion)
	require.Equal(t, 2, SignedHeaderSize)
	require.Equal(t, 239, SignedUnsignedSize)
	require.Equal(t, 303, SignedSize)
	require.Equal(t, "xxx/iam/ticket-signature/v1\x00", SigningDomain)

	unsigned := []byte{0x01, 0x02, 0x03}
	message := SigningMessage(unsigned)
	defer func() {
		clear(message)
		slicepool.Put(message)
	}()
	require.Equal(t, append([]byte("xxx/iam/ticket-signature/v1\x00"), unsigned...), message)

	unsigned[0] = 0xFF
	require.Equal(t, []byte{0x01, 0x02, 0x03}, message[len(SigningDomain):], "signing message must not alias input")
}
