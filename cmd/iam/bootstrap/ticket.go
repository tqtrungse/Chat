/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package bootstrap

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"xxx/internal/iam/infrastructure/crypto"

	"xxx/pkg/config"
)

func loadTicketSigner(loader config.Loader) (*crypto.TicketSigner, error) {
	id := loader.GetUint8("TICKET_SIGNING_KEY_ID")
	if id == 0 {
		return nil, errors.New("invalid TICKET_SIGNING_KEY_ID")
	}

	seed, err := parseTicketSigningPrivate(
		strings.TrimSpace(loader.GetString("TICKET_SIGNING_PRIVATE_KEY")),
	)
	if err != nil {
		return nil, err
	}

	signer, err := crypto.NewTicketSigner(id, seed)
	clear(seed[:cap(seed)])
	if err != nil {
		return nil, fmt.Errorf("failed to initialize ticket signer: %w", err)
	}

	return signer, nil
}

// parseTicketSigningPrivate decodes a 32-byte Ed25519 seed for IAM.
// The caller must clear the returned seed after NewTicketSigner copies it.
func parseTicketSigningPrivate(value string) ([]byte, error) {
	seed, err := base64.StdEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		clear(seed[:cap(seed)])
		return nil, fmt.Errorf("decode ticket signing seed: %w", err)
	}

	if len(seed) != ed25519.SeedSize {
		got := len(seed)
		clear(seed[:cap(seed)])
		return nil, fmt.Errorf("ticket signing seed: want %d bytes, got %d", ed25519.SeedSize, got)
	}

	return seed, nil
}
