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
	"strings"

	"xxx/internal/iam/infrastructure/crypto"
	"xxx/internal/shared/ticket"

	"xxx/pkg"
)

func loadTicketSigner(loader pkg.ConfigLoader) (*crypto.TicketSigner, error) {
	keys, err := ticket.ParseTicketEncryptKeys(
		strings.Split(loader.GetString("TICKET_ENCRYPT_KEYS"), ","),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ticket encryption keys: %w", err)
	}
	defer func() {
		for _, key := range keys {
			clear(key)
		}
	}()

	sealer, err := ticket.NewSealer(
		loader.GetUint8("TICKET_ENCRYPT_CURRENT_KEY_ID"),
		keys,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize ticket sealer: %w", err)
	}

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

	signer, err := crypto.NewTicketSigner(sealer, id, seed)
	clear(seed)
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
		clear(seed)
		return nil, fmt.Errorf("decode ticket signing seed: %w", err)
	}

	if len(seed) != ed25519.SeedSize {
		got := len(seed)
		clear(seed)
		return nil, fmt.Errorf("ticket signing seed: want %d bytes, got %d", ed25519.SeedSize, got)
	}
	return seed, nil
}
