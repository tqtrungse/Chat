/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package http

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

	"xxx/internal/iam/core/rejection"
	"xxx/internal/iam/core/ticketlimit"

	"xxx/pkg/jwt"
	"xxx/pkg/log"
	"xxx/pkg/reject"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/mailru/easyjson"
	"github.com/mailru/easyjson/jwriter"
	"go.uber.org/zap"
)

type contextKey uint8

const claimsContextKey contextKey = iota

type Server struct {
	srv           *http.Server
	auth          *jwt.Auth
	logger        *log.Logger
	rejections    *reject.Stats[rejection.Reason]
	ticketIssuer  TicketIssuer
	ticketLimiter ticketlimit.Limiter
	certFile      string
	keyFile       string
}

// NewServer initializes JWT verification and builds the authenticated HTTPS router
// with request logging, recovery, timeouts, and a TLS 1.3 minimum. Admission and
// rejection counters are supplied by the caller; the listener starts only in Run.
func NewServer(
	ctx context.Context,
	cfg Config,
	logger *log.Logger,
	rejections *reject.Stats[rejection.Reason],
	ticketIssuer TicketIssuer,
	ticketLimiter ticketlimit.Limiter,
) (*Server, error) {
	if ticketLimiter == nil {
		return nil, errTicketLimiterRequired
	}

	auth, err := jwt.NewAuth(
		ctx,
		jwt.AuthConfig{
			Issuer:   cfg.OIDCIssuer,
			Audience: cfg.OIDCAudience,
			JWKSURL:  cfg.OIDCJWKSURL,
		},
	)
	if err != nil {
		return nil, err
	}

	s := &Server{
		auth:          auth,
		logger:        logger,
		rejections:    rejections,
		ticketIssuer:  ticketIssuer,
		ticketLimiter: ticketLimiter,
		certFile:      cfg.CertFile,
		keyFile:       cfg.KeyFile,
	}

	router := chi.NewRouter()
	router.Use(middleware.RequestLogger(requestLogFormatter{logger: s.logger}))
	router.Use(middleware.Recoverer)
	router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			next.ServeHTTP(w, r)
		})
	})
	router.Group(func(r chi.Router) {
		r.Use(s.authenticate)
		r.Post("/iam/v2/connection-ticket", s.issueConnectionTicket)
	})

	s.srv = &http.Server{
		Addr:              cfg.Addr,
		Handler:           router,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
		IdleTimeout:       cfg.IdleTimeout,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS13},
	}
	return s, nil
}

// Run serves HTTPS using the configured certificate and private key until the
// server shuts down or the listener fails.
func (s *Server) Run() error {
	return s.srv.ListenAndServeTLS(s.certFile, s.keyFile)
}

// Shutdown stops accepting new requests and waits up to 30 seconds for active
// HTTP requests to finish using a fresh shutdown context.
func (s *Server) Shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}

// authenticate requires a Bearer token, verifies it, and attaches its claims to
// the request context before invoking the next handler. Rejections are sampled.
func (s *Server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		scheme, token, found := strings.Cut(authHeader, " ")
		token = strings.TrimLeft(token, " ")
		if !found || !strings.EqualFold(scheme, "Bearer") || token == "" {
			s.rejections.Record(rejection.AuthHeader, errAuthHeaderNotFound)
			s.respondErr(w, http.StatusUnauthorized, errAuthHeaderNotFound)
			return
		}

		claims, err := s.auth.Verify(token)
		if err != nil {
			s.rejections.Record(rejection.AuthToken, err)
			s.respondErr(w, http.StatusUnauthorized, errAuthVerify)
			return
		}

		// Attach claims to the context.
		ctx := context.WithValue(r.Context(), claimsContextKey, claims)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// respond marshals and writes a successful JSON response, releasing the writer
// buffers after use. Write failures are logged without attempting a second response.
func (s *Server) respond(w http.ResponseWriter, v easyjson.Marshaler) {
	jw := jwriter.Writer{}

	v.MarshalEasyJSON(&jw)
	if jw.Error != nil {
		s.logger.Error("failed to marshall JSON", zap.Error(jw.Error))
		s.respondErr(w, http.StatusInternalServerError, errJsonMarshall)
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
		var netErr net.Error
		if errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) ||
			(errors.As(err, &netErr) && netErr.Timeout()) {
			s.rejections.Record(rejection.ResponseWrite, err)
			return
		}
		s.logger.Error("failed to dump response", zap.Error(err))
	}
}

// respondTicketRateLimited sets Retry-After and returns 503 for shared storage
// exhaustion or 429 for subject and device quota limits.
func (s *Server) respondTicketRateLimited(w http.ResponseWriter, decision ticketlimit.Decision) {
	w.Header().Set("Retry-After", strconv.Itoa(decision.RetryAfter))

	if decision.Overloaded() {
		s.respondErr(w, http.StatusServiceUnavailable, errTicketCapacity)
	} else {
		s.respondErr(w, http.StatusTooManyRequests, errTicketRateLimited)
	}
}

// respondErr writes the supplied status and error message as a plain-text HTTP response.
func (s *Server) respondErr(w http.ResponseWriter, status int, err error) {
	http.Error(w, err.Error(), status)
}
