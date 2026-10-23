/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
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

func (c *Config) Load(loader config.Loader) error {
	if loader == nil {
		return fmt.Errorf("noise configuration is required")
	}

	c.KeyringFile = strings.TrimSpace(loader.GetString("HUB_NOISE_KEYRING_FILE"))
	if len(c.KeyringFile) == 0 {
		return fmt.Errorf("HUB_NOISE_KEYRING_FILE is required")
	}

	id, err := strconv.ParseUint(
		strings.TrimSpace(loader.GetString("HUB_NOISE_CURRENT_KEY_ID")),
		10,
		32,
	)
	if err != nil || id == 0 {
		return fmt.Errorf("HUB_NOISE_CURRENT_KEY_ID must be in 1..4294967295")
	}
	c.CurrentKeyID = uint32(id)

	c.Audience = loader.GetString("HUB_NOISE_AUDIENCE")
	if err = hubendpoint.ValidateAudience(c.Audience); err != nil {
		return err
	}

	c.HandshakeTimeout, err = time.ParseDuration(
		strings.TrimSpace(loader.GetString("HUB_NOISE_HANDSHAKE_TIMEOUT")),
	)
	if err != nil {
		return fmt.Errorf("invalid HUB_NOISE_HANDSHAKE_TIMEOUT: %w", err)
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = defaultHandshakeTimeout
	}
	return nil
}
