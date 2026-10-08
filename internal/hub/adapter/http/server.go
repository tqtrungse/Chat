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
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"xxx/internal/hub/application/exchange_key"

	"xxx/pkg"
	"xxx/pkg/log"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/mailru/easyjson"
	"github.com/mailru/easyjson/jwriter"
	"go.uber.org/zap"
)

const claimsKey string = "claims"

type Server struct {
	srv          *http.Server
	auth0        *pkg.Auth0
	logger       *log.Logger
	keyExchanger exchange_key.Usecase
	certFile     string
	keyFile      string
}

func NewServer(
	ctx context.Context,
	cfg Config,
	logger *log.Logger,
	keyExchanger exchange_key.Usecase,
) (*Server, error) {
	auth0, err := pkg.NewAuth0(
		ctx,
		pkg.Auth0Config{
			Auth0Domain:   cfg.Auth0Domain,
			Auth0Audience: cfg.Auth0Audience,
		},
	)
	if err != nil {
		return nil, err
	}

	s := &Server{
		auth0:        auth0,
		logger:       logger,
		keyExchanger: keyExchanger,
		certFile:     cfg.CertFile,
		keyFile:      cfg.KeyFile,
	}

	router := chi.NewRouter()
	router.Use(middleware.Logger)
	router.Use(middleware.Recoverer)
	router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			next.ServeHTTP(w, r)
		})
	})
	router.Group(func(r chi.Router) {
		r.Use(s.authenticate)
		r.Post("/hub/v1/exchange-key", s.exchangeKey)
	})

	s.srv = &http.Server{
		Addr:              cfg.Addr,
		Handler:           router,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
	}
	return s, nil
}

func (s *Server) Run() error {
	return s.srv.ListenAndServeTLS(s.certFile, s.keyFile)
}

func (s *Server) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if !strings.HasPrefix(authHeader, "Bearer ") {
			s.respondErr(w, http.StatusUnauthorized, errors.New("missing or invalid Authorization header"))
			return
		}

		token := strings.TrimPrefix(authHeader, "Bearer ")
		claims, err := s.auth0.Verify(token)
		if err != nil {
			s.logger.Error("failed to verify token", zap.Error(err))
			s.respondErr(w, http.StatusUnauthorized, err)
			return
		}

		// Attach claims to the context.
		ctx := context.WithValue(r.Context(), claimsKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (s *Server) respond(w http.ResponseWriter, v easyjson.Marshaler) {
	jw := jwriter.Writer{}

	v.MarshalEasyJSON(&jw)
	if jw.Error != nil {
		s.respondErr(w, http.StatusInternalServerError, jw.Error)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(jw.Size()))
	w.WriteHeader(http.StatusOK)

	// Writes chunks to w and releases them back to the pool.
	_, err := jw.DumpTo(w)
	if err != nil {
		// Only log errors.
		// Do not call s.respondErr here because the header has already been sent!
		s.logger.Error("failed to dump response", zap.Error(err))
	}
}

func (s *Server) respondErr(w http.ResponseWriter, status int, err error) {
	http.Error(w, err.Error(), status)
}
