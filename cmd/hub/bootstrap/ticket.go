/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package bootstrap

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"xxx/internal/hub/infrastructure/crypto"

	"xxx/pkg/config"
)

func loadTicketVerifier(loader config.Loader) (*crypto.TicketVerifier, error) {
	publicKeys, err := parseTicketSigningPublicKeys(
		strings.Split(loader.GetString("TICKET_SIGNING_PUBLIC_KEYS"), ","),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ticket signing public keys: %w", err)
	}

	clockSkew, err := time.ParseDuration(
		strings.TrimSpace(loader.GetString("TICKET_CLOCK_SKEW")),
	)
	if err != nil {
		return nil, fmt.Errorf("invalid TICKET_CLOCK_SKEW: %w", err)
	}

	return crypto.NewTicketVerifier(publicKeys, clockSkew)
}

// parseTicketSigningPublicKeys parses ID:base64(public key) entries for Hub.
func parseTicketSigningPublicKeys(entries []string) (map[byte]ed25519.PublicKey, error) {
	if len(entries) == 0 {
		return nil, fmt.Errorf("ticket verification keys are required")
	}

	out := make(map[byte]ed25519.PublicKey, len(entries))
	for _, entry := range entries {
		idText, encoded, ok := strings.Cut(strings.TrimSpace(entry), ":")
		if !ok {
			return nil, fmt.Errorf("ticket public key: expected ID:base64")
		}

		id, err := strconv.ParseUint(strings.TrimSpace(idText), 10, 8)
		if err != nil {
			return nil, fmt.Errorf("ticket public key ID: %w", err)
		}

		_, exists := out[byte(id)]
		if exists {
			return nil, fmt.Errorf("duplicate ticket public key ID %d", id)
		}

		key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
		if err != nil {
			return nil, fmt.Errorf("decode ticket public key %d: %w", id, err)
		}

		if len(key) != ed25519.PublicKeySize {
			return nil,
				fmt.Errorf(
					"ticket public key %d: want %d bytes, got %d",
					id, ed25519.PublicKeySize, len(key),
				)
		}

		out[byte(id)] = key
	}
	return out, nil
}
