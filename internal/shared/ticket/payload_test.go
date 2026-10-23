/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package ticket_test

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"xxx/internal/shared/ticket"
	"xxx/internal/shared/ticket/tickettest"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
)

func TestPayloadLayoutRoundTripBoundsAndOwnership(t *testing.T) {
	for _, n := range []int{1, 8, 128} {
		meta := tickettest.New()
		meta.Audience = strings.Repeat("a", n)
		buf, err := ticket.MarshalPayload(meta)
		require.NoError(t, err)
		require.Len(t, buf, 111+n)
		require.Equal(t, meta.TicketID[:], buf[:ticket.IDSize])
		require.Equal(t, uint64(42), binary.LittleEndian.Uint64(buf[ticket.IDSize:]))
		require.Equal(t, uint16(n), binary.LittleEndian.Uint16(buf[72:]))
		require.Equal(t, uint32(7), binary.LittleEndian.Uint32(buf[74+n:]))

		out, err := ticket.UnmarshalPayload(buf)
		require.NoError(t, err)
		clear(buf[:cap(buf)])
		slicepool.Put(buf)
		require.Equal(t, meta, out)
	}

	meta := tickettest.New()
	buf, err := ticket.MarshalPayload(meta)
	t.Cleanup(func() {
		clear(buf[:cap(buf)])
		slicepool.Put(buf)
	})
	require.NoError(t, err)

	for n := range buf {
		_, err = ticket.UnmarshalPayload(buf[:n])
		require.Error(t, err)
	}

	bads := [][]byte{
		append(bytes.Clone(buf), 0),
		bytes.Repeat([]byte{1}, 240),
	}
	for _, bad := range bads {
		_, err = ticket.UnmarshalPayload(bad)
		require.Error(t, err)
	}

	binary.LittleEndian.PutUint16(buf[72:], 65535)
	_, err = ticket.UnmarshalPayload(buf)
	require.Error(t, err)
	require.Equal(t, byte(2), ticket.SignedVersion)
	require.Equal(t, 307, ticket.MaxSignedSize)

	raw := []byte{1, 2, 3}
	msg := ticket.SigningMessage(raw)
	t.Cleanup(func() {
		clear(msg[:cap(msg)])
		slicepool.Put(msg)
	})
	raw[0] = 9
	require.Equal(t, []byte{1, 2, 3}, msg[len(ticket.SigningDomain):])
}

func FuzzUnmarshalPayload(f *testing.F) {
	buf, _ := ticket.MarshalPayload(tickettest.New())
	f.Cleanup(func() {
		clear(buf[:cap(buf)])
		slicepool.Put(buf)
	})

	f.Add(buf)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		v, err := ticket.UnmarshalPayload(b)
		if err == nil {
			require.NoError(t, v.Validate())
		}
	})
}

func TestDecodePayloadDoesNotReplaceFullValidation(t *testing.T) {
	claims := tickettest.New()
	payload, err := ticket.MarshalPayload(claims)
	require.NoError(t, err)
	defer func() {
		clear(payload[:cap(payload)])
		slicepool.Put(payload)
	}()

	// A structurally decodable payload may still contain invalid curve points.
	identity := payload[40:72:72]
	clear(identity[:cap(identity)])
	decoded, err := ticket.DecodePayload(payload)
	require.NoError(t, err)
	require.ErrorIs(t, decoded.Validate(), ticket.ErrTicketInvalid)

	_, err = ticket.UnmarshalPayload(payload)
	require.ErrorIs(t, err, ticket.ErrTicketInvalid)
}
