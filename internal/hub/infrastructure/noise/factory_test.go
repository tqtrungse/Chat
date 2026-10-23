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
	"xxx/internal/hub/core/transport"
	"xxx/internal/hub/infrastructure/noise/noisetest"
	"xxx/internal/shared/ticket/tickettest"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/chacha20poly1305"
)

func newHandshake(t *testing.T) (transport.Responder, *noisetest.Peer) {
	t.Helper()

	pub := tickettest.HubPublic()
	key := KeyEntry{
		KeyID:      7,
		PrivateKey: tickettest.HubPrivate(),
		PublicKey:  pub[:],
	}
	ring, err := LoadKeyring(keyFile(t, key), 7)
	require.NoError(t, err)
	t.Cleanup(ring.Close)

	factory, err := NewFactory(ring, "hub.chat", MinFrameBytes)
	require.NoError(t, err)

	resp, err := factory.NewResponder(7)
	require.NoError(t, err)
	t.Cleanup(resp.Close)

	peer, err := noisetest.New(key.PublicKey, "hub.chat", 7)
	require.NoError(t, err)
	return resp, peer
}

func establish(t *testing.T) (transport.Channel, *noisetest.Peer, []byte) {
	t.Helper()
	resp, peer := newHandshake(t)

	hello, err := peer.Hello(7)
	require.NoError(t, err)
	require.Len(t, hello, 55)
	require.NoError(t, resp.ReadClientHello(hello[protocol.FramePrefixSize+protocol.ClientHelloHeaderSize:]))

	msg, ch, binding, err := resp.WriteServerHello(bytes.Repeat([]byte{1}, auth.ChallengeSize))
	require.NoError(t, err)
	defer func() {
		clear(msg[:cap(msg)])
		slicepool.Put(msg)
	}()
	t.Cleanup(ch.Close)
	require.Len(t, msg, 82)

	frame, err := protocol.EncodeFrame(msg, MinFrameBytes)
	require.NoError(t, err)
	defer func() {
		clear(frame[:cap(frame)])
		slicepool.Put(frame)
	}()
	require.NoError(t, peer.Finish(frame))
	require.Equal(t, binding, peer.Binding)
	require.Len(t, binding, auth.ChannelBindingSize)
	require.Equal(t, bytes.Repeat([]byte{1}, auth.ChallengeSize), peer.Challenge)

	return ch, peer, binding
}

func TestNoiseDirectionMappingNonceProgressionAndIndependentSessions(t *testing.T) {
	ch, peer, binding := establish(t)
	_, _, other := establish(t)
	require.NotEqual(t, binding, other)

	for i := range 4 {
		plain := []byte{byte(i)}
		frame, err := peer.Encrypt(plain)
		require.NoError(t, err)

		out := decryptRecord(t, ch, frame)
		require.Equal(t, plain, out)

		frame = encryptRecord(t, ch, plain)
		out, err = peer.Decrypt(frame)
		require.NoError(t, err)
		require.Equal(t, plain, out)
	}
	// Rejected output size must not consume the next send nonce.
	rejected, err := ch.EncryptRecord(make([]byte, MinFrameBytes-chacha20poly1305.Overhead+1))
	require.Error(t, err)
	require.Nil(t, rejected)

	frame := encryptRecord(t, ch, []byte("next"))
	plain, err := peer.Decrypt(frame)
	require.NoError(t, err)
	require.Equal(t, "next", string(plain))
}

func TestNoiseTamperReplayReorderAndCrossConnectionReplayCloseChannel(t *testing.T) {
	for _, kind := range []string{
		"tamper",
		"prefix",
		"truncated",
		"replay",
		"reorder",
		"cross connection",
	} {
		t.Run(
			kind,
			func(t *testing.T) {
				ch, peer, _ := establish(t)
				first, err := peer.Encrypt([]byte("one"))
				require.NoError(t, err)

				switch kind {
				case "tamper":
					first[len(first)-1] ^= 1
				case "prefix":
					first[0] ^= 1
				case "truncated":
					first = first[:len(first)-1]
				case "replay":
					decryptRecord(t, ch, first)
				case "reorder":
					first, err = peer.Encrypt([]byte("two"))
					require.NoError(t, err)
				case "cross connection":
					ch, _, _ = establish(t)
				}

				out, err := ch.DecryptRecord(first)
				require.Error(t, err)
				require.Nil(t, out)

				rejected, err := ch.EncryptRecord(nil)
				require.Error(t, err, "authentication failure is terminal")
				require.Nil(t, rejected)
			},
		)
	}
}

func encryptRecord(t *testing.T, ch transport.Channel, plain []byte) []byte {
	t.Helper()

	frame, err := ch.EncryptRecord(plain)
	require.NoError(t, err)
	t.Cleanup(func() {
		clear(frame[:cap(frame)])
		slicepool.Put(frame)
	})
	return frame
}

func decryptRecord(t *testing.T, ch transport.Channel, frame []byte) []byte {
	t.Helper()

	plain, err := ch.DecryptRecord(frame)
	require.NoError(t, err)
	t.Cleanup(func() {
		clear(plain[:cap(plain)])
		slicepool.Put(plain)
	})
	return plain
}

func TestNoiseRejectsUnknownKeyWrongStaticPrologueAndUnexpectedPayload(t *testing.T) {
	key := entry(t, 7)
	other := entry(t, 8)
	ring, err := LoadKeyring(keyFile(t, key, other), 7)
	require.NoError(t, err)
	defer ring.Close()

	factory, err := NewFactory(ring, "hub.chat", MinFrameBytes)
	require.NoError(t, err)

	_, err = factory.NewResponder(99)
	require.Error(t, err)

	for _, tc := range []struct {
		pub []byte
		aud string
		id  uint32
	}{
		{other.PublicKey, "hub.chat", 7},
		{key.PublicKey, "other", 7},
		{key.PublicKey, "hub.chat", 8},
	} {
		resp, err2 := factory.NewResponder(7)
		require.NoError(t, err2)

		peer, err2 := noisetest.New(tc.pub, tc.aud, tc.id)
		require.NoError(t, err2)

		hello, err2 := peer.Hello(tc.id)
		require.NoError(t, err2)
		require.Error(t, resp.ReadClientHello(hello[protocol.FramePrefixSize+protocol.ClientHelloHeaderSize:]))

		resp.Close()
	}

	for _, id := range []uint32{7, 8} {
		resp, err2 := factory.NewResponder(id)
		require.NoError(t, err2)

		public := key.PublicKey
		if id == 8 {
			public = other.PublicKey
		}
		peer, err2 := noisetest.New(public, "hub.chat", id)
		require.NoError(t, err2)

		hello, err2 := peer.Hello(id)
		require.NoError(t, err2)
		require.NoError(t, resp.ReadClientHello(hello[protocol.FramePrefixSize+protocol.ClientHelloHeaderSize:]))

		message, ch, _, err2 := resp.WriteServerHello(make([]byte, auth.ChallengeSize))
		require.NoError(t, err2)
		t.Cleanup(func() {
			clear(message[:cap(message)])
			slicepool.Put(message)
		})

		frame, err2 := protocol.EncodeFrame(message, MinFrameBytes)
		require.NoError(t, err2)
		t.Cleanup(func() {
			clear(frame[:cap(frame)])
			slicepool.Put(frame)
		})
		require.NoError(t, peer.Finish(frame))

		ch.Close()
	}

	resp, err := factory.NewResponder(7)
	require.NoError(t, err)
	require.Error(t, resp.ReadClientHello(make([]byte, ClientHelloSize+1)))
}
