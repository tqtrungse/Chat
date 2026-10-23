/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package auth_test

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"xxx/internal/hub/core/auth"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
)

func TestSigningMessageMatchesPublishedVector(t *testing.T) {
	b, err := os.ReadFile("../../../../docs/protocol/noise-v2-vectors.json")
	require.NoError(t, err)

	var v map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(b, &v))

	read := func(name string) []byte {
		var s string
		require.NoError(t, json.Unmarshal(v[name], &s))
		buf, decodeErr := hex.DecodeString(s)
		require.NoError(t, decodeErr)
		return buf
	}

	msg, err := auth.SigningMessage(
		read("channel_binding_hex"),
		read("ticket_hex"),
		read("challenge_hex"),
	)
	t.Cleanup(func() {
		slicepool.Put(msg)
	})
	require.NoError(t, err)
	require.Equal(t, read("signature_input_hex"), msg)
	require.True(
		t,
		ed25519.Verify(
			read("identity_public_hex"),
			msg,
			read("identity_signature_hex"),
		),
	)

	for _, n := range []int{0, 31, 33} {
		_, err = auth.SigningMessage(
			make([]byte, n),
			[]byte{1},
			bytes.Repeat([]byte{1}, 32),
		)
		require.Error(t, err)
	}

	_, err = auth.SigningMessage(
		make([]byte, 32),
		nil,
		make([]byte, 32),
	)
	require.Error(t, err)
}
