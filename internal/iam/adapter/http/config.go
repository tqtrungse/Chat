/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package http

import (
	"strings"
	"time"

	"xxx/pkg/config"
)

const (
	defaultReadHeaderTimeout = 5 * time.Second
	defaultReadTimeout       = 10 * time.Second
	defaultWriteTimeout      = 30 * time.Second
	defaultIdleTimeout       = 15 * time.Second
	defaultMaxHeaderBytes    = 16384
)

type Config struct {
	Addr              string
	OIDCIssuer        string
	OIDCAudience      string
	OIDCJWKSURL       string
	CertFile          string
	KeyFile           string
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	MaxHeaderBytes    int
}

// Load reads HTTP, TLS certificate, and OIDC settings and applies server defaults.
// Explicitly configured zero timeouts are preserved to allow disabling them.
func (c *Config) Load(loader config.Loader) {
	if loader != nil {
		c.Addr = strings.TrimSpace(loader.GetString("HTTP_SERVER_ADDR"))
		c.CertFile = strings.TrimSpace(loader.GetString("HTTP_SERVER_CERT_FILE"))
		c.KeyFile = strings.TrimSpace(loader.GetString("HTTP_SERVER_KEY_FILE"))
		c.ReadHeaderTimeout = loader.GetDuration("HTTP_READ_HEADER_TIMEOUT")
		c.ReadTimeout = loader.GetDuration("HTTP_READ_TIMEOUT")
		c.WriteTimeout = loader.GetDuration("HTTP_WRITE_TIMEOUT")
		c.IdleTimeout = loader.GetDuration("HTTP_IDLE_TIMEOUT")
		c.MaxHeaderBytes = loader.GetInt("HTTP_MAX_HEADER_BYTES")
		c.OIDCIssuer = strings.TrimSpace(loader.GetString("HTTP_SERVER_OIDC_ISSUER"))
		c.OIDCAudience = strings.TrimSpace(loader.GetString("HTTP_SERVER_OIDC_AUDIENCE"))
		c.OIDCJWKSURL = strings.TrimSpace(loader.GetString("HTTP_SERVER_OIDC_JWKS_URL"))
	}
	if c.ReadHeaderTimeout == 0 && (loader == nil || loader.GetString("HTTP_READ_HEADER_TIMEOUT") == "") {
		c.ReadHeaderTimeout = defaultReadHeaderTimeout
	}
	if c.ReadTimeout == 0 && (loader == nil || loader.GetString("HTTP_READ_TIMEOUT") == "") {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.WriteTimeout == 0 && (loader == nil || loader.GetString("HTTP_WRITE_TIMEOUT") == "") {
		c.WriteTimeout = defaultWriteTimeout
	}
	if c.IdleTimeout == 0 && (loader == nil || loader.GetString("HTTP_IDLE_TIMEOUT") == "") {
		c.IdleTimeout = defaultIdleTimeout
	}
	if c.MaxHeaderBytes <= 0 {
		c.MaxHeaderBytes = defaultMaxHeaderBytes
	}
}
