/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package noise

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	pbpub "xxx/api/hub/v2/proto/gen/pub"
	"xxx/internal/hub/core/auth"
	"xxx/internal/shared/hubendpoint"

	slicepool "xxx/pkg/pool/slice"

	"github.com/flynn/noise"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/chacha20poly1305"
	"google.golang.org/protobuf/proto"
)

func entry(t *testing.T, id uint32) KeyEntry {
	t.Helper()

	pair, err := noise.DH25519.GenerateKeypair(nil)
	require.NoError(t, err)
	t.Cleanup(func() { clear(pair.Private[:cap(pair.Private)]) })
	return KeyEntry{KeyID: id, PrivateKey: pair.Private, PublicKey: pair.Public}
}

func keyFile(t *testing.T, entries ...KeyEntry) string {
	t.Helper()

	data, err := json.Marshal(KeyFile{Keys: entries})
	require.NoError(t, err)
	defer clear(data[:cap(data)])

	path := filepath.Join(t.TempDir(), "keyring.json")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

func TestKeyringRotationAndOwnership(t *testing.T) {
	old, current := entry(t, 1), entry(t, 2)
	path := keyFile(t, old, current)
	ring, err := LoadKeyring(path, 2)
	require.NoError(t, err)
	defer ring.Close()
	require.Equal(t, uint32(2), ring.CurrentKeyID())

	pair, ok := ring.Keypair(1)
	require.True(t, ok)
	require.Equal(t, old.PublicKey, pair.Public)
	clear(pair.Private[:cap(pair.Private)])
	clear(pair.Public[:cap(pair.Public)])

	pair, ok = ring.Keypair(1)
	require.True(t, ok)
	require.Equal(t, old.PrivateKey, pair.Private, "a caller must not erase the shared key")
	clear(pair.Private[:cap(pair.Private)])

	_, ok = ring.Keypair(3)
	require.False(t, ok)

	ring.Close()
	_, ok = ring.Keypair(2)
	require.False(t, ok)
	// Projected Kubernetes Secret volumes use symlinks and optional group read.
	require.NoError(t, os.Chmod(path, 0o440))

	link := filepath.Join(t.TempDir(), "projected-keyring.json")
	require.NoError(t, os.Symlink(path, link))

	ring, err = LoadKeyring(link, 1)
	require.NoError(t, err)
	ring.Close()
}

func TestKeyringRejectsInvalidKeysAndPermissions(t *testing.T) {
	valid := entry(t, 1)
	other := entry(t, 2)

	for _, tc := range []struct {
		name    string
		entries []KeyEntry
		current uint32
		want    error
	}{
		{
			"empty",
			nil,
			1,
			ErrKeyringKeyCountInvalid,
		},
		{
			"zero current",
			[]KeyEntry{valid},
			0,
			ErrKeyIDRequired,
		},
		{
			"missing current",
			[]KeyEntry{valid},
			2,
			nil,
		},
		{
			"duplicate ID",
			[]KeyEntry{valid, valid},
			1,
			nil,
		},
		{
			"zero ID",
			[]KeyEntry{
				{
					KeyID:      0,
					PrivateKey: valid.PrivateKey,
					PublicKey:  valid.PublicKey,
				},
			},
			1,
			ErrKeyIDRequired,
		},
		{
			"short private",
			[]KeyEntry{
				{
					KeyID:      1,
					PrivateKey: valid.PrivateKey[:len(valid.PrivateKey)-1],
					PublicKey:  valid.PublicKey,
				},
			},
			1,
			nil,
		},
		{
			"long public",
			[]KeyEntry{
				{
					KeyID:      1,
					PrivateKey: valid.PrivateKey,
					PublicKey:  append(bytes.Clone(valid.PublicKey), 0),
				},
			},
			1,
			nil,
		},
		{
			"mismatched public",
			[]KeyEntry{
				{
					KeyID:      1,
					PrivateKey: valid.PrivateKey,
					PublicKey:  other.PublicKey,
				},
			},
			1,
			nil,
		},
		{
			"too many keys",
			make([]KeyEntry, MaxKeyringKeys+1), 1,
			ErrKeyringKeyCountInvalid,
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				ring, err := LoadKeyring(keyFile(t, tc.entries...), tc.current)
				require.Error(t, err)

				if tc.want != nil {
					require.ErrorIs(t, err, tc.want)
				}
				require.Nil(t, ring)
			},
		)
	}

	path := keyFile(t, valid)
	require.NoError(t, os.Chmod(path, 0o644))

	_, err := LoadKeyring(path, 1)
	require.ErrorIs(t, err, ErrKeyringPermissions)

	for i, input := range []string{
		`{"keys":[],"secret":"do-not-log"}`,
		`{"keys":[]} {}`,
		`{"keys":[{"private_key":"do-not-log"}]}`,
		strings.Repeat(" ", MaxKeyringBytes+1),
	} {
		require.NoError(t, os.Chmod(path, 0o600))
		require.NoError(t, os.WriteFile(path, []byte(input), 0o600))

		_, err = LoadKeyring(path, 1)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "do-not-log")

		if i == 3 {
			require.ErrorIs(t, err, ErrKeyringTooLarge)
		} else {
			require.ErrorIs(t, err, ErrKeyringJSONInvalid)
		}
	}
}

func TestTwoReplicasShareStaticIdentityButNotSession(t *testing.T) {
	var (
		static     = entry(t, 7)
		path       = keyFile(t, static)
		descriptor = hubendpoint.Descriptor{
			Address:   "hub.example.com:9000",
			Audience:  "hub.chat",
			KeyID:     7,
			PublicKey: static.PublicKey,
			Protocol:  hubendpoint.Protocol,
			Version:   hubendpoint.WireVersion,
		}
	)

	prologue, err := descriptor.Prologue()
	t.Cleanup(func() {
		clear(prologue[:cap(prologue)])
		slicepool.Put(prologue)
	})
	require.NoError(t, err)

	var (
		received    [2]*noise.CipherState
		ciphertext  [2][]byte
		plain       = []byte("authenticated client payload")
		aad         = binary.LittleEndian.AppendUint16(nil, uint16(len(plain)+chacha20poly1305.Overhead))
		cipherSuite = noise.NewCipherSuite(
			noise.DH25519,
			noise.CipherChaChaPoly,
			noise.HashSHA256,
		)
	)

	for replica := range 2 {
		ring, err2 := LoadKeyring(path, 7)
		require.NoError(t, err2)
		t.Cleanup(ring.Close)

		pair, ok := ring.Keypair(7)
		require.True(t, ok)
		t.Cleanup(func() { clear(pair.Private[:cap(pair.Private)]) })

		client, err2 := noise.NewHandshakeState(
			noise.Config{
				CipherSuite: cipherSuite,
				Pattern:     noise.HandshakeNK,
				Initiator:   true,
				PeerStatic:  descriptor.PublicKey,
				Prologue:    prologue,
			},
		)
		require.NoError(t, err2)

		server, err2 := noise.NewHandshakeState(
			noise.Config{
				CipherSuite:   cipherSuite,
				Pattern:       noise.HandshakeNK,
				StaticKeypair: pair,
				Prologue:      prologue,
			},
		)
		require.NoError(t, err2)

		m1, _, _, err2 := client.WriteMessage(nil, nil)
		require.NoError(t, err2)

		payload, _, _, err2 := server.ReadMessage(nil, m1)
		require.NoError(t, err2)
		require.Empty(t, payload)

		challenge, err2 := proto.Marshal(
			&pbpub.NoiseServerHelloPayload{
				Challenge: bytes.Repeat([]byte{byte(replica + 1)}, auth.ChallengeSize),
			},
		)
		require.NoError(t, err2)

		m2, serverRecv, serverSend, err2 := server.WriteMessage(nil, challenge)
		require.NoError(t, err2)

		payload, clientSend, clientRecv, err2 := client.ReadMessage(nil, m2)
		require.NoError(t, err2)
		require.Equal(t, challenge, payload)
		require.Equal(t, client.ChannelBinding(), server.ChannelBinding())

		ciphertext[replica], err2 = clientSend.Encrypt(nil, aad, plain)
		require.NoError(t, err2)

		received[replica] = serverRecv
		response, err2 := serverSend.Encrypt(nil, aad, plain)
		require.NoError(t, err2)

		got, err2 := clientRecv.Decrypt(nil, aad, response)
		require.NoError(t, err2)
		require.Equal(t, plain, got)
	}
	require.NotEqual(t, ciphertext[0], ciphertext[1])

	_, err = received[1].Decrypt(nil, aad, ciphertext[0])
	require.Error(t, err, "replica 2 cannot decrypt replica 1's independent connection")

	for i := range 2 {
		got, err2 := received[i].Decrypt(nil, aad, ciphertext[i])
		require.NoError(t, err2)
		require.Equal(t, plain, got)
	}
}
