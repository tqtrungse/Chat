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

package session

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

// ParseTicketEncryptKeys parses entries like "1:<base64 32 bytes>".
func ParseTicketEncryptKeys(entries []string) (map[byte][]byte, error) {
	out := make(map[byte][]byte, len(entries))
	for _, e := range entries {
		idStr, b64, ok := strings.Cut(strings.TrimSpace(e), ":")
		if !ok {
			return nil, ErrTicketBadEntry
		}

		id, err := strconv.ParseUint(idStr, 10, 8)
		if err != nil {
			return nil, err
		}
		if _, exists := out[byte(id)]; exists {
			return nil, fmt.Errorf("duplicate ticket encryption key ID %d", id)
		}

		k, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return nil, err
		}

		out[byte(id)] = k
	}
	return out, nil
}

// ParseTicketSigningPrivate decodes a 32-byte Ed25519 seed for IAM.
// The caller must clear the returned seed after NewTicketSigner copies it.
func ParseTicketSigningPrivate(value string) ([]byte, error) {
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

// ParseTicketSigningPublicKeys parses ID:base64(public key) entries for Hub.
func ParseTicketSigningPublicKeys(entries []string) (map[byte]ed25519.PublicKey, error) {
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
