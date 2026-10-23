/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connection

import (
	"time"

	"xxx/pkg/config"
)

const (
	defaultTickerDuration = time.Second
	defaultMaxConns       = 128000
)

type Config struct {
	// TickerDuration defines interval time to the next polling.
	//
	// Default: 1s
	TickerDuration time.Duration

	// MaxConns defines the maximum number of keeping connections in the hub.
	//
	// Default: 128000
	MaxConns uint32
}

// Load reads connection settings when a loader is provided and fills unset values with defaults.
func (c *Config) Load(loader config.Loader) {
	if loader != nil {
		c.TickerDuration = loader.GetDuration("CONNECTION_TICKER_DURATION")
		c.MaxConns = loader.GetUint32("CONNECTION_MAX_CONNS")
	}
	if c.TickerDuration <= 0 {
		c.TickerDuration = defaultTickerDuration
	}
	if c.MaxConns == 0 {
		c.MaxConns = defaultMaxConns
	}
}
