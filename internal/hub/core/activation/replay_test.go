/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package activation_test

import (
	"bytes"
	"crypto/ed25519"
	"testing"

	"xxx/internal/hub/core/auth"
	"xxx/internal/shared/ticket/tickettest"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
)

func TestIdentityProofCannotReplayAcrossBindingTicketOrChallenge(t *testing.T) {
	var (
		binding   = bytes.Repeat([]byte{1}, 32)
		challenge = bytes.Repeat([]byte{2}, 32)
		ticket    = []byte("signed ticket")
		msg, err  = auth.SigningMessage(binding, ticket, challenge)
	)

	t.Cleanup(func() {
		clear(msg[:cap(msg)])
		slicepool.Put(msg)
	})
	require.NoError(t, err)

	var (
		private   = tickettest.IdentityPrivate()
		signature = ed25519.Sign(private, msg)
		public    = private.Public().(ed25519.PublicKey)
	)

	for _, change := range []int{0, 1, 2} {
		b, c, w := bytes.Clone(binding), bytes.Clone(challenge), bytes.Clone(ticket)
		switch change {
		case 0:
			b[0] ^= 1
		case 1:
			c[0] ^= 1
		case 2:
			w[0] ^= 1
		}

		var other []byte
		other, err = auth.SigningMessage(b, w, c)
		require.NoError(t, err)
		require.False(t, ed25519.Verify(public, other, signature))
		slicepool.Put(other)
	}
}
