/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package http

import (
	"time"

	"xxx/pkg"
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
	MaxHeaderBytes    int
}

func (c *Config) Load(loader pkg.ConfigLoader) {
	if loader == nil {
		return
	}
	c.Addr = loader.GetString("HTTP_SERVER_ADDR")
	c.CertFile = loader.GetString("HTTP_SERVER_CERT_FILE")
	c.KeyFile = loader.GetString("HTTP_SERVER_KEY_FILE")
	c.ReadHeaderTimeout = loader.GetDuration("HTTP_READ_HEADER_TIMEOUT")
	c.ReadTimeout = loader.GetDuration("HTTP_READ_TIMEOUT")
	c.WriteTimeout = loader.GetDuration("HTTP_WRITE_TIMEOUT")
	c.MaxHeaderBytes = loader.GetInt("HTTP_MAX_HEADER_BYTES")
	c.OIDCIssuer = loader.GetString("HTTP_SERVER_OIDC_ISSUER")
	c.OIDCAudience = loader.GetString("HTTP_SERVER_OIDC_AUDIENCE")
	c.OIDCJWKSURL = loader.GetString("HTTP_SERVER_OIDC_JWKS_URL")
}
