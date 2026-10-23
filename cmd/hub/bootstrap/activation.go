/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package bootstrap

import (
	"fmt"

	"xxx/internal/hub/adapter/tcp"
	"xxx/internal/hub/application/activateconn"
	"xxx/internal/hub/core/connection"
	"xxx/internal/hub/core/protocol"
	"xxx/internal/hub/infrastructure/noise"
)

// resolveActivationConfig combines already loaded settings without reading env
// or modifying its inputs. Only bootstrap knows about all three dependencies.
func resolveActivationConfig(
	cfg activateconn.Config,
	cfgNoise noise.Config,
	cfgTCP tcp.Config,
	cfgConnection connection.Config,
) (activateconn.Config, error) {
	if cfg.PreAuthTimeout <= 0 {
		return cfg, fmt.Errorf("ACTIVATE_PREAUTH_TIMEOUT must be positive")
	}
	if cfgNoise.HandshakeTimeout <= 0 {
		return cfg, fmt.Errorf("HUB_NOISE_HANDSHAKE_TIMEOUT must be positive")
	}
	if cfgTCP.MaxFrameBytes < noise.MinFrameBytes || cfgTCP.MaxFrameBytes > protocol.MaxFrameSize {
		return cfg, fmt.Errorf("TCP_SERVER_MAX_FRAME_BYTES must be in 512..65535")
	}
	if cfgConnection.MaxConns == 0 {
		return cfg, fmt.Errorf("CONNECTION_MAX_CONNS must be positive")
	}

	cfg.HandshakeTimeout = cfgNoise.HandshakeTimeout
	cfg.MaxFrameBytes = cfgTCP.MaxFrameBytes
	if cfg.MaxPreAuthConns == 0 {
		cfg.MaxPreAuthConns = max(cfgConnection.MaxConns/10, 1)
	}
	return cfg, nil
}
