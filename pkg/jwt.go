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

type JWTAuthConfig struct {
	Issuer   string
	Audience string
	JWKSURL  string
}

type JWTClaims struct {
	jwt.RegisteredClaims
	Scope string `json:"scope"`
}

type JWTAuth struct {
	jwks keyfunc.Keyfunc
	cfg  JWTAuthConfig
}

func NewJWTAuth(ctx context.Context, cfg JWTAuthConfig) (*JWTAuth, error) {
	if cfg.Issuer == "" {
		return nil, errors.New("JWT issuer is required")
	}
	if cfg.Audience == "" {
		return nil, errors.New("JWT audience is required")
	}
	if cfg.JWKSURL == "" {
		return nil, errors.New("JWT JWKS URL is required")
	}

	jwks, err := keyfunc.NewDefaultCtx(ctx, []string{cfg.JWKSURL})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch JWKS: %w", err)
	}
	return &JWTAuth{
		jwks: jwks,
		cfg:  cfg,
	}, nil
}

func (v *JWTAuth) Verify(token string) (*JWTClaims, error) {
	claims := new(JWTClaims)
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
		return nil, fmt.Errorf("invalid token: %w", err)
	}
	if !tk.Valid {
		return nil, errors.New("invalid token")
	}
	return claims, nil
}
