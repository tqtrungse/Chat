/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package activateconn

import (
	"runtime"
	"time"

	"xxx/pkg/config"
)

const defaultPreAuthTimeout = 10 * time.Second

type Config struct {
	// VerifyWorkers bounds concurrent authorization jobs in the dedicated pool.
	// Default: max(GOMAXPROCS/2, 1).
	VerifyWorkers int

	// MaxVerifyJobs bounds queued plus running authorization jobs, independently
	// of connection lifetime. Default: VerifyWorkers * 4.
	MaxVerifyJobs int

	// HandshakeTimeout is injected by bootstrap from the Noise configuration.
	HandshakeTimeout time.Duration

	// MaxFrameBytes is injected by bootstrap from the TCP frame-body limit.
	MaxFrameBytes int

	// PreAuthTimeout bounds the whole pre-auth window: accept -> handshake -> authorization -> registration and ACK.
	//
	// Default: 10s
	PreAuthTimeout time.Duration

	// MaxPreAuthConns caps connections that have not completed activation.
	//
	// Zero requests automatic sizing in bootstrap: max(connection.MaxConns/10, 1).
	// LoadConfig leaves this value at zero until bootstrap resolves the dependencies.
	MaxPreAuthConns uint32
}

// LoadConfig reads the activation settings from loader and applies defaults. A nil
// loader is an empty source. Bootstrap sets the injected fields after LoadConfig.
func LoadConfig(loader config.Loader) Config {
	var c Config
	if loader != nil {
		c.VerifyWorkers = loader.GetInt("ACTIVATE_VERIFY_WORKERS")
		c.MaxVerifyJobs = loader.GetInt("ACTIVATE_MAX_VERIFY_JOBS")
		c.PreAuthTimeout = loader.GetDuration("ACTIVATE_PREAUTH_TIMEOUT")
		c.MaxPreAuthConns = loader.GetUint32("ACTIVATE_MAX_PREAUTH_CONNS")
	}
	c.applyDefault()
	return c
}

// applyDefault puts the default value in each field that is zero or negative.
// MaxPreAuthConns keeps the value zero, because bootstrap sizes it.
func (c *Config) applyDefault() {
	if c.VerifyWorkers <= 0 {
		c.VerifyWorkers = max(runtime.GOMAXPROCS(0)/2, 1)
	}
	if c.MaxVerifyJobs <= 0 {
		c.MaxVerifyJobs = c.VerifyWorkers * 4
	}
	if c.PreAuthTimeout <= 0 {
		c.PreAuthTimeout = defaultPreAuthTimeout
	}
}
