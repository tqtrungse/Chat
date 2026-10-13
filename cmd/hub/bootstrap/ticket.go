/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bootstrap

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"xxx/internal/hub/infrastructure/crypto"
	"xxx/internal/shared/ticket"

	"xxx/pkg"
)

func loadTicketVerifier(cfgLoader pkg.ConfigLoader) (*crypto.TicketVerifier, error) {
	keys, err := ticket.ParseTicketEncryptKeys(
		strings.Split(cfgLoader.GetString("TICKET_ENCRYPT_KEYS"), ","),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ticket encryption keys: %w", err)
	}
	defer func() {
		for _, key := range keys {
			clear(key)
		}
	}()

	id := cfgLoader.GetUint8("TICKET_ENCRYPT_CURRENT_KEY_ID")
	if id == 0 {
		return nil, errors.New("invalid TICKET_ENCRYPT_CURRENT_KEY_ID")
	}

	sealer, err := ticket.NewSealer(id, keys)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize ticket sealer: %w", err)
	}

	publicKeys, err := parseTicketSigningPublicKeys(
		strings.Split(cfgLoader.GetString("TICKET_SIGNNING_PUBLIC_KEYS"), ","),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ticket signing public keys: %w", err)
	}

	clockSkew, err := time.ParseDuration(strings.TrimSpace(cfgLoader.GetString("TICKET_CLOCK_SKEW")))
	if err != nil {
		return nil, fmt.Errorf("invalid TICKET_CLOCK_SKEW: %w", err)
	}

	return crypto.NewTicketVerifier(
		sealer,
		publicKeys,
		clockSkew,
	)
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

		if _, exists := out[byte(id)]; exists {
			return nil, fmt.Errorf("duplicate ticket public key ID %d", id)
		}

		key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
		if err != nil {
			return nil, fmt.Errorf("decode ticket public key %d: %w", id, err)
		}

		if len(key) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("ticket public key %d: want %d bytes, got %d", id, ed25519.PublicKeySize, len(key))
		}

		out[byte(id)] = key
	}
	return out, nil
}
