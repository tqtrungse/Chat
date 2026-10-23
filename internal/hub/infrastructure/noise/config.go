/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package noise

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"xxx/internal/shared/hubendpoint"

	"xxx/pkg/config"
)

const defaultHandshakeTimeout = 5 * time.Second

// Config contains the Hub's Noise service identity and keyring settings.
type Config struct {
	KeyringFile      string
	CurrentKeyID     uint32
	Audience         string
	HandshakeTimeout time.Duration
}

// LoadConfig reads the Noise settings from loader. A nil loader is an empty
// source. LoadConfig returns an error if a value is absent or not valid.
func LoadConfig(loader config.Loader) (Config, error) {
	get := func(key string) string {
		if loader == nil {
			return ""
		}
		return loader.GetString(key)
	}

	var c Config

	c.KeyringFile = strings.TrimSpace(get("HUB_NOISE_KEYRING_FILE"))
	if len(c.KeyringFile) == 0 {
		return Config{}, fmt.Errorf("HUB_NOISE_KEYRING_FILE is required")
	}

	id, err := strconv.ParseUint(strings.TrimSpace(get("HUB_NOISE_CURRENT_KEY_ID")), 10, 32)
	if err != nil || id == 0 {
		return Config{}, fmt.Errorf("HUB_NOISE_CURRENT_KEY_ID must be in 1..4294967295")
	}
	c.CurrentKeyID = uint32(id)

	c.Audience = get("HUB_NOISE_AUDIENCE")
	err = hubendpoint.ValidateAudience(c.Audience)
	if err != nil {
		return Config{}, err
	}

	c.HandshakeTimeout, err = time.ParseDuration(strings.TrimSpace(get("HUB_NOISE_HANDSHAKE_TIMEOUT")))
	if err != nil {
		return Config{}, fmt.Errorf("invalid HUB_NOISE_HANDSHAKE_TIMEOUT: %w", err)
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = defaultHandshakeTimeout
	}
	return c, nil
}
