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

package jwt

import (
	"context"
	"errors"
	"fmt"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrIssuerRequired   = errors.New("JWT issuer is required")
	ErrAudienceRequired = errors.New("JWT audience is required")
	ErrJwksUrlRequired  = errors.New("JWT JWKS URL is required")
	ErrTokenInvalid     = errors.New("invalid token")
)

type AuthConfig struct {
	Issuer   string
	Audience string
	JWKSURL  string
}

type Claims struct {
	jwt.RegisteredClaims
	Scope string `json:"scope"`
}

type Auth struct {
	jwks keyfunc.Keyfunc
	cfg  AuthConfig
}

func NewAuth(ctx context.Context, cfg AuthConfig) (*Auth, error) {
	if cfg.Issuer == "" {
		return nil, ErrIssuerRequired
	}
	if cfg.Audience == "" {
		return nil, ErrAudienceRequired
	}
	if cfg.JWKSURL == "" {
		return nil, ErrJwksUrlRequired
	}

	jwks, err := keyfunc.NewDefaultCtx(ctx, []string{cfg.JWKSURL})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch JWKS: %w", err)
	}
	return &Auth{
		jwks: jwks,
		cfg:  cfg,
	}, nil
}

func (v *Auth) Verify(token string) (*Claims, error) {
	claims := new(Claims)
	tk, err := jwt.ParseWithClaims(
		token,
		claims,
		v.jwks.Keyfunc,
		jwt.WithValidMethods([]string{"RS256"}), // Force signature algorithm to prevent algorithm confusion attacks.
		jwt.WithIssuer(v.cfg.Issuer),
		jwt.WithAudience(v.cfg.Audience),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to parse claims: %w", err)
	}
	if !tk.Valid {
		return nil, ErrTokenInvalid
	}
	return claims, nil
}
