/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package http

import (
	"errors"
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
	// Addr is the TCP address that the HTTPS server listens on, in the format
	// "host:port". If the host is empty, the server listens on all interfaces.
	//
	// Required.
	Addr string

	// OIDCIssuer is the issuer URL of the OIDC provider. The server accepts a
	// JWT only if its "iss" claim is equal to this value.
	//
	// Required.
	OIDCIssuer string

	// OIDCAudience is the audience of this service. The server accepts a JWT
	// only if its "aud" claim contains this value.
	//
	// Required.
	OIDCAudience string

	// OIDCJWKSURL is the URL of the JWKS document of the OIDC provider. The
	// server gets the public keys from this URL to verify each JWT signature.
	//
	// Required.
	OIDCJWKSURL string

	// CertFile is the path to the PEM file of the TLS certificate. If a CA
	// signs the certificate, the file must contain the server certificate,
	// then the intermediate certificates, then the CA certificate.
	//
	// Required.
	CertFile string

	// KeyFile is the path to the PEM file of the private key for CertFile.
	//
	// Required.
	KeyFile string

	// ReadHeaderTimeout is the maximum time to read the request line and the
	// request headers. It stops a client that sends the headers very slowly
	// and holds a connection open.
	//
	// Default: 5s
	ReadHeaderTimeout time.Duration

	// ReadTimeout is the maximum time to read a full request, which includes
	// the body.
	//
	// Default: 10s
	ReadTimeout time.Duration

	// WriteTimeout is the maximum time to write a response. The server starts
	// this time again when it reads the headers of each new request.
	//
	// Default: 30s
	WriteTimeout time.Duration

	// IdleTimeout is the maximum time that a keep-alive connection waits for
	// the next request. After this time, the server closes the connection.
	//
	// Default: 15s
	IdleTimeout time.Duration

	// MaxHeaderBytes is the maximum size in bytes of the request line and the
	// request headers. It does not limit the size of the request body. If the
	// value is zero or negative, the default applies.
	//
	// Default: 16384
	MaxHeaderBytes int
}

// LoadConfig reads HTTP, TLS certificate, and OIDC settings and applies server defaults.
// A negative timeout disables that timeout.
// A nil loader is an empty source. LoadConfig returns an error if a required field is empty.
func LoadConfig(loader config.Loader) (Config, error) {
	var c Config
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
	c.applyDefault()

	err := c.validate()
	if err != nil {
		return Config{}, err
	}
	return c, nil
}

// applyDefault puts the default value in each timeout that is zero. It also
// puts the default value in MaxHeaderBytes if it is zero or negative. A
// negative timeout stays negative, because it disables that timeout.
func (c *Config) applyDefault() {
	if c.ReadHeaderTimeout == 0 {
		c.ReadHeaderTimeout = defaultReadHeaderTimeout
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = defaultIdleTimeout
	}
	if c.MaxHeaderBytes <= 0 {
		c.MaxHeaderBytes = defaultMaxHeaderBytes
	}
}

// validate returns an error if a required field is empty.
func (c *Config) validate() error {
	if len(c.Addr) == 0 {
		return errors.New("HTTP_SERVER_ADDR is required")
	}
	if len(c.CertFile) == 0 || len(c.KeyFile) == 0 {
		return errors.New("HTTP_SERVER_CERT_FILE and HTTP_SERVER_KEY_FILE are required; IAM serves HTTPS")
	}
	return nil
}
