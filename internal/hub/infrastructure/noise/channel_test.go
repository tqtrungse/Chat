/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package noise

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"testing"

	"xxx/internal/hub/core/protocol"

	flynn "github.com/flynn/noise"
	"github.com/stretchr/testify/require"
)

func TestChannelRecordSizesAndOwnership(t *testing.T) {
	for _, size := range []int{0, 1, 31, 32, 33, 64, 496} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			ch, peer, _ := establish(t)
			want := bytes.Repeat([]byte{0xA5}, size)
			frame, err := peer.Encrypt(want)
			require.NoError(t, err)

			original := bytes.Clone(frame)
			plain := decryptRecord(t, ch, frame)
			require.Len(t, plain, size)
			require.Len(t, plain, len(frame)-18)
			require.True(t, bytes.Equal(want, plain))
			require.Equal(t, original, frame, "decrypt must not overwrite the input frame")
			clear(frame)
			require.True(t, bytes.Equal(want, plain), "output must not alias the input frame")

			// Keep the first plaintext alive while decrypting a second record.
			other := bytes.Repeat([]byte{0x5A}, size)
			next, err := peer.Encrypt(other)
			require.NoError(t, err)

			second := decryptRecord(t, ch, next)
			require.True(t, bytes.Equal(other, second))
			require.True(t, bytes.Equal(want, plain), "outputs must have independent storage")

			input := bytes.Clone(want)
			response := encryptRecord(t, ch, input)
			require.Len(t, response, size+18)
			require.EqualValues(t, size+16, binary.LittleEndian.Uint16(response))
			clear(input)

			got, err := peer.Decrypt(response)
			require.NoError(t, err)
			require.True(t, bytes.Equal(want, got))
		})
	}
}

func TestChannelRejectsInvalidFrameSizesAndClosesBothDirections(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame []byte
	}{
		{"nil", nil},
		{"short prefix", []byte{1}},
		{"prefix only", []byte{16, 0}},
		{"short tag", make([]byte, 17)},
		{"mismatched prefix", make([]byte, 18)},
		{"over configured limit", make([]byte, 515)},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				ch, _, _ := establish(t)
				if len(tc.frame) == 515 {
					binary.LittleEndian.PutUint16(tc.frame, 513)
				}
				plain, err := ch.DecryptRecord(tc.frame)
				require.ErrorIs(t, err, ErrProtocol)
				require.Nil(t, plain)

				c := ch.(*channel)
				require.Nil(t, c.recv)
				require.Nil(t, c.send)

				frame, err := ch.EncryptRecord(nil)
				require.ErrorIs(t, err, ErrProtocol)
				require.Nil(t, frame)
			},
		)
	}
}

func TestChannelCipherErrorsCloseBothDirections(t *testing.T) {
	for _, direction := range []string{
		"send",
		"receive",
	} {
		t.Run(
			direction,
			func(t *testing.T) {
				ch, peer, _ := establish(t)
				c := ch.(*channel)
				var out []byte
				var err error
				if direction == "send" {
					c.send.SetNonce(flynn.MaxNonce + 1)
					out, err = ch.EncryptRecord([]byte("secret"))
				} else {
					frame, encryptErr := peer.Encrypt([]byte("secret"))
					require.NoError(t, encryptErr)
					c.recv.SetNonce(flynn.MaxNonce + 1)
					out, err = ch.DecryptRecord(frame)
				}
				require.ErrorIs(t, err, flynn.ErrMaxNonce)
				require.Nil(t, out)
				require.Nil(t, c.recv)
				require.Nil(t, c.send)
			})
	}
}

func TestClosedChannelRejectsRecords(t *testing.T) {
	ch, peer, _ := establish(t)
	frame, err := peer.Encrypt([]byte("secret"))
	require.NoError(t, err)
	ch.Close()
	ch.Close()

	plain, err := ch.DecryptRecord(frame)
	require.ErrorIs(t, err, ErrProtocol)
	require.Nil(t, plain)

	out, err := ch.EncryptRecord(nil)
	require.ErrorIs(t, err, ErrProtocol)
	require.Nil(t, out)
	require.Greater(t, len(frame), protocol.FramePrefixSize)
}
