/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package ticket_test

import (
	"crypto/ed25519"
	"testing"

	"xxx/internal/shared/hubendpoint"
	"xxx/internal/shared/ticket"
	"xxx/internal/shared/ticket/tickettest"

	"github.com/stretchr/testify/require"
)

func TestTicketValidatesPublicClaims(t *testing.T) {
	require.NoError(t, tickettest.New().Validate())
	for _, change := range []func(*ticket.Info){
		func(t *ticket.Info) { t.DeviceID = 0 },
		func(t *ticket.Info) { t.TicketID = [ticket.IDSize]byte{} },
		func(t *ticket.Info) { t.ExpiresAt = t.IssuedAt },
		func(t *ticket.Info) { t.IssuedAt = -1 },
		func(t *ticket.Info) { t.ExpiresAt = t.IssuedAt + 16 },
		func(t *ticket.Info) { t.IdentityPub = [ed25519.PublicKeySize]byte{} },
		func(t *ticket.Info) { t.IdentityPub = [ed25519.PublicKeySize]byte{1} },
		func(t *ticket.Info) { t.HubPublicKey = [hubendpoint.X25519PublicKeySize]byte{} },
		func(t *ticket.Info) { t.HubKeyID = 0 },
		func(t *ticket.Info) { t.WireVersion = 1 },
		func(t *ticket.Info) { t.Audience = "" },
	} {
		meta := tickettest.New()
		change(meta)
		require.Error(t, meta.Validate())
	}

	var nilTicket *ticket.Info
	require.Error(t, nilTicket.Validate())
	require.NotPanics(t, nilTicket.Clear)

	meta := tickettest.New()
	before := *meta
	meta.Clear()
	require.Equal(t, ticket.Info{}, *meta)
	require.NotEqual(t, *meta, before)
	require.NotPanics(t, meta.Clear)
}
