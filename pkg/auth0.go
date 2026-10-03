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

package pkg

import (
	"context"
	"errors"
	"fmt"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

type Auth0Config struct {
	Auth0Domain   string
	Auth0Audience string
}

type Auth0Claims struct {
	jwt.RegisteredClaims
	Scope string `json:"scope"`
}

type Auth0 struct {
	jwks keyfunc.Keyfunc
	cfg  Auth0Config
}

func NewAuth0(ctx context.Context, cfg Auth0Config) (*Auth0, error) {
	jwksURL := cfg.Auth0Domain + "/.well-known/jwks.json"
	jwks, err := keyfunc.NewDefaultCtx(ctx, []string{jwksURL})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch JWKS: %w", err)
	}
	return &Auth0{
		jwks: jwks,
		cfg:  cfg,
	}, nil
}

func (a *Auth0) Verify(token string) (*Auth0Claims, error) {
	var (
		claims = new(Auth0Claims)
		tk     *jwt.Token
		err    error
	)

	tk, err = jwt.ParseWithClaims(
		token,
		claims,
		a.jwks.Keyfunc,
		jwt.WithValidMethods([]string{"RS256"}), // Force signature algorithm to prevent algorithm confusion attack
		jwt.WithIssuer(a.cfg.Auth0Domain+"/"),   // Iss must be right
		jwt.WithAudience(a.cfg.Auth0Audience),   // Aud must be right
		jwt.WithExpirationRequired(),            // Expiration must exist and doesn't expire
	)
	if err != nil {
		return nil, fmt.Errorf("invalid token: %w", err)
	}
	if !tk.Valid {
		return nil, errors.New("invalid token")
	}
	return claims, nil
}
