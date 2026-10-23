/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package noise

import (
	"bytes"
	"testing"

	"xxx/internal/hub/core/auth"
	"xxx/internal/hub/core/protocol"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
)

func TestResponderRejectsInvalidClientHello(t *testing.T) {
	for _, kind := range []string{
		"missing",
		"short",
		"long",
		"invalid tag",
		"repeated",
	} {
		t.Run(
			kind,
			func(t *testing.T) {
				resp, peer := newHandshake(t)
				frame, err := peer.Hello(7)
				require.NoError(t, err)

				message := frame[protocol.FramePrefixSize+protocol.ClientHelloHeaderSize:]

				switch kind {
				case "missing":
					message = nil
				case "short":
					message = message[:len(message)-1]
				case "long":
					message = append(message, 0)
				case "invalid tag":
					message[len(message)-1] ^= 1
				case "repeated":
					require.NoError(t, resp.ReadClientHello(message))
				}

				require.ErrorIs(t, resp.ReadClientHello(message), ErrProtocol)
				out, ch, binding, err := resp.WriteServerHello(make([]byte, auth.ChallengeSize))
				require.ErrorIs(t, err, ErrProtocol)
				require.Nil(t, out)
				require.Nil(t, ch)
				require.Nil(t, binding)
			},
		)
	}
}

func TestResponderRejectsInvalidServerHelloStateAndChallenge(t *testing.T) {
	for _, kind := range []string{
		"before client hello",
		"closed",
		"missing challenge",
		"short challenge",
		"long challenge",
	} {
		t.Run(
			kind,
			func(t *testing.T) {
				resp, peer := newHandshake(t)
				challenge := make([]byte, auth.ChallengeSize)
				if kind != "before client hello" {
					frame, err := peer.Hello(7)
					require.NoError(t, err)
					require.NoError(t, resp.ReadClientHello(frame[protocol.FramePrefixSize+protocol.ClientHelloHeaderSize:]))
				}

				switch kind {
				case "closed":
					resp.Close()
				case "missing challenge":
					challenge = nil
				case "short challenge":
					challenge = challenge[:len(challenge)-1]
				case "long challenge":
					challenge = append(challenge, 0)
				}

				out, ch, binding, err := resp.WriteServerHello(challenge)
				require.ErrorIs(t, err, ErrProtocol)
				require.Nil(t, out)
				require.Nil(t, ch)
				require.Nil(t, binding)
			},
		)
	}
}

func TestServerHelloOwnsBuffersAndCanOnlyBeWrittenOnce(t *testing.T) {
	resp, peer := newHandshake(t)
	hello, err := peer.Hello(7)
	require.NoError(t, err)
	require.NoError(t, resp.ReadClientHello(hello[protocol.FramePrefixSize+protocol.ClientHelloHeaderSize:]))

	challenge := bytes.Repeat([]byte{0xA5}, auth.ChallengeSize)
	message, ch, binding, err := resp.WriteServerHello(challenge)
	require.NoError(t, err)
	t.Cleanup(ch.Close)
	defer func() {
		clear(message[:cap(message)])
		slicepool.Put(message)
	}()
	require.Len(t, message, 82)
	require.Len(t, binding, 32)

	clear(challenge)
	resp.Close()
	frame, err := protocol.EncodeFrame(message, MinFrameBytes)
	require.NoError(t, err)
	defer func() {
		clear(frame[:cap(frame)])
		slicepool.Put(frame)
	}()
	require.NoError(t, peer.Finish(frame))
	require.Equal(t, bytes.Repeat([]byte{0xA5}, auth.ChallengeSize), peer.Challenge)
	require.Equal(t, binding, peer.Binding)

	out, nextChannel, nextBinding, err := resp.WriteServerHello(challenge)
	require.ErrorIs(t, err, ErrProtocol)
	require.Nil(t, out)
	require.Nil(t, nextChannel)
	require.Nil(t, nextBinding)

	// Closing/reusing the responder must not invalidate the returned channel.
	request, err := peer.Encrypt([]byte("still active"))
	require.NoError(t, err)
	require.Equal(t, []byte("still active"), decryptRecord(t, ch, request))
}
