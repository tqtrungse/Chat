/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package http

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	api "xxx/api/iam/v2/json"
	httpmocks "xxx/internal/iam/adapter/http/mocks"
	"xxx/internal/iam/application/connticket"
	connticketmocks "xxx/internal/iam/application/connticket/mocks"
	"xxx/internal/iam/core/rejection"
	"xxx/internal/iam/core/ticketlimit"
	limitmocks "xxx/internal/iam/core/ticketlimit/mocks"
	iamcrypto "xxx/internal/iam/infrastructure/crypto"
	"xxx/internal/shared/device"
	"xxx/internal/shared/hubendpoint"
	"xxx/internal/shared/ticket/tickettest"

	"xxx/pkg/database/sql"
	dbmocks "xxx/pkg/database/sql/mocks"
	appjwt "xxx/pkg/jwt"
	"xxx/pkg/log"
	"xxx/pkg/reject"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestIssueConnectionTicketMapsIssuerErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{
		{
			"not found",
			connticket.ErrDeviceNotFound,
			http.StatusNotFound,
		},
		{
			"inactive",
			connticket.ErrDeviceUnactive,
			http.StatusForbidden,
		},
		{
			"invalid request",
			connticket.ErrRequestInvalid,
			http.StatusBadRequest,
		},
		{
			"internal failure",
			errors.New("private database details"),
			http.StatusInternalServerError,
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				issuer := httpmocks.NewMockTicketIssuer(ctrl)
				logger, closeLogger, err := log.NewLogger(nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = closeLogger() })

				limiter := limitmocks.NewMockLimiter(ctrl)
				gomock.InOrder(
					limiter.
						EXPECT().
						AllowSubject("owner").
						Return(ticketlimit.Decision{}),
					limiter.
						EXPECT().
						AllowDevice("owner", uint64(42)).
						Return(ticketlimit.Decision{}),
				)
				server := &Server{
					ticketIssuer:  issuer,
					ticketLimiter: limiter,
					logger:        logger,
					rejections: reject.New[rejection.Reason](
						logger,
						0,
						rejection.Names(),
					),
				}

				t.Cleanup(func() {
					var count uint64
					server.rejections.Each(func(_ string, n uint64) { count += n })

					if tc.status >= http.StatusInternalServerError {
						require.Zero(t, count, "server errors must not count as client rejections")
					} else {
						require.Equal(t, uint64(1), count)
					}
				})

				request := httptest.NewRequest(
					http.MethodPost,
					"/iam/v2/connection-ticket",
					bytes.NewBufferString(`{"device_id":"42"}`),
				)
				request = request.WithContext(context.WithValue(
					request.Context(),
					claimsContextKey,
					&appjwt.Claims{
						Subject: "owner",
					},
				))
				// The adapter owns the port and must preserve the request context.
				issuer.
					EXPECT().
					Issue(
						request.Context(),
						"owner",
						api.ConnectionTicketReq{DeviceID: "42"},
					).
					Return(nil, errors.Join(errors.New("issuance failed"), tc.err))

				response := httptest.NewRecorder()
				server.issueConnectionTicket(response, request)
				require.Equal(t, tc.status, response.Code)

				if tc.status == http.StatusInternalServerError {
					require.NotContains(t, response.Body.String(), tc.err.Error())
				}
			},
		)
	}
}

func TestHTTPSConnectionTicketUsesJWTAndProductionIssuer(t *testing.T) {
	signing, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	logger, closeLogger, err := log.NewLogger(nil)
	require.NoError(t, err)
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(
			map[string]any{
				"keys": []any{
					map[string]any{
						"kty": "RSA",
						"kid": "test",
						"alg": "RS256",
						"use": "sig",
						"n":   base64.RawURLEncoding.EncodeToString(signing.N.Bytes()), "e": "AQAB",
					},
				},
			},
		)
	}))

	ctrl := gomock.NewController(t)
	reader := connticketmocks.NewMockDeviceMetaReader(ctrl)
	db := dbmocks.NewMockDB(ctrl)

	t.Cleanup(func() {
		jwks.Close()
		_ = closeLogger()
		ctrl.Finish()
	})

	ticketSigner, err := iamcrypto.NewTicketSigner(1, tickettest.SigningSeed())
	require.NoError(t, err)

	pub := tickettest.HubPublic()
	tokenFor := func(subject string) string {
		token := jwt.NewWithClaims(
			jwt.SigningMethodRS256,
			jwt.RegisteredClaims{
				Issuer:    jwks.URL,
				Audience:  []string{"iam-api"},
				Subject:   subject,
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
			},
		)
		token.Header["kid"] = "test"
		wire, err := token.SignedString(signing)
		require.NoError(t, err)
		return wire
	}

	type testCase struct {
		name       string
		path       string
		scheme     string
		token      string
		body       string
		code       int
		setupMocks func(*dbmocks.MockDB, *connticketmocks.MockDeviceMetaReader)
	}

	expectLookup := func(id device.ID, meta *connticket.DeviceMeta) func(*dbmocks.MockDB, *connticketmocks.MockDeviceMetaReader) {
		return func(db *dbmocks.MockDB, reader *connticketmocks.MockDeviceMetaReader) {
			db.EXPECT().
				WithDB(
					gomock.Any(),
					sql.TxSourcePrimary,
					gomock.Any(),
				).
				DoAndReturn(func(ctx context.Context, _ sql.TxSource, fn func(context.Context) error) error {
					return fn(ctx)
				})

			reader.EXPECT().
				FindDeviceMeta(gomock.Any(), id).
				Return(meta, nil)
		}
	}

	activeDevice := func() *connticket.DeviceMeta {
		return &connticket.DeviceMeta{
			ExternalID:  "owner",
			State:       device.StateActive,
			IdentityPub: tickettest.New().IdentityPub[:],
		}
	}

	issuer, err := connticket.New(
		db,
		reader,
		ticketSigner,
		hubendpoint.Descriptor{
			Address:   "hub.example.com:9000",
			Audience:  "hub.chat",
			KeyID:     7,
			PublicKey: pub[:],
			Protocol:  hubendpoint.Protocol,
			Version:   hubendpoint.WireVersion,
		},
	)
	require.NoError(t, err)

	limiter := limitmocks.NewMockLimiter(ctrl)
	limiter.
		EXPECT().
		AllowSubject(gomock.Any()).
		Return(ticketlimit.Decision{}).
		AnyTimes()
	limiter.
		EXPECT().
		AllowDevice(gomock.Any(), gomock.Any()).
		Return(ticketlimit.Decision{}).
		AnyTimes()

	rejections := reject.New[rejection.Reason](
		logger,
		0,
		rejection.Names(),
	)
	server, err := NewServer(
		t.Context(),
		Config{
			OIDCIssuer:   jwks.URL,
			OIDCAudience: "iam-api",
			OIDCJWKSURL:  jwks.URL,
		},
		logger,
		rejections,
		issuer,
		limiter,
	)
	require.NoError(t, err)
	require.Same(t, rejections, server.rejections)

	token := tokenFor("owner")
	tlsServer := httptest.NewUnstartedServer(server.srv.Handler)
	tlsServer.TLS = server.srv.TLSConfig.Clone()
	tlsServer.StartTLS()
	t.Cleanup(tlsServer.Close)

	for _, tc := range []struct {
		name    string
		version uint16
		wantErr bool
	}{
		{
			name:    "TLS 1.2 is rejected",
			version: tls.VersionTLS12,
			wantErr: true,
		},
		{
			name:    "TLS 1.3 is accepted",
			version: tls.VersionTLS13,
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				// Keep the test server's trusted roots while forcing one TLS version.
				cfg := tlsServer.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
				cfg.MinVersion = tc.version
				cfg.MaxVersion = tc.version
				conn, err := tls.DialWithDialer(
					&net.Dialer{Timeout: 5 * time.Second},
					"tcp",
					tlsServer.Listener.Addr().String(),
					cfg,
				)
				if conn != nil {
					t.Cleanup(func() { require.NoError(t, conn.Close()) })
				}
				if tc.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.version, conn.ConnectionState().Version)
			},
		)
	}

	t.Run(
		"router limits before primary lookup and signing",
		func(t *testing.T) {
			for _, quota := range []string{"subject", "device"} {
				t.Run(
					quota,
					func(t *testing.T) {
						limitedPort := limitmocks.NewMockLimiter(ctrl)
						calls := []any{
							limitedPort.
								EXPECT().
								AllowSubject("owner").
								Return(ticketlimit.Decision{}),
							limitedPort.
								EXPECT().
								AllowDevice("owner", uint64(42)).
								Return(ticketlimit.Decision{}),
						}
						if quota == "subject" {
							calls = append(
								calls,
								limitedPort.
									EXPECT().
									AllowSubject("owner").
									Return(ticketlimit.Decision{
										Reason:     ticketlimit.SubjectLimited,
										RetryAfter: 3600,
									}),
							)
						} else {
							calls = append(
								calls,
								limitedPort.
									EXPECT().
									AllowSubject("owner").
									Return(ticketlimit.Decision{}),
								limitedPort.
									EXPECT().
									AllowDevice("owner", uint64(42)).
									Return(ticketlimit.Decision{
										Reason:     ticketlimit.DeviceLimited,
										RetryAfter: 3600,
									}),
							)
						}

						gomock.InOrder(calls...)
						limited, err := NewServer(
							t.Context(),
							Config{
								OIDCIssuer:   jwks.URL,
								OIDCAudience: "iam-api",
								OIDCJWKSURL:  jwks.URL,
							},
							logger,
							rejections,
							issuer,
							limitedPort,
						)

						require.NoError(t, err)
						// Only the admitted request may reach the production issuer's DB port.
						expectLookup(42, activeDevice())(db, reader)
						for _, want := range []int{
							http.StatusOK,
							http.StatusTooManyRequests,
						} {
							req := httptest.NewRequest(
								http.MethodPost,
								"/iam/v2/connection-ticket",
								bytes.NewBufferString(`{"device_id":"42"}`),
							)
							req.Header.Set("Authorization", "Bearer "+token)

							w := httptest.NewRecorder()
							limited.srv.Handler.ServeHTTP(w, req)

							require.Equal(t, want, w.Code, w.Body.String())
							require.Equal(t, "no-store", w.Header().Get("Cache-Control"))

							if want == http.StatusTooManyRequests {
								require.Equal(t, "3600", w.Header().Get("Retry-After"))
							}
						}
					},
				)
			}
		},
	)

	for _, tc := range []testCase{
		{
			name:       "success",
			path:       "/iam/v2/connection-ticket",
			token:      token,
			body:       `{"device_id":"42"}`,
			code:       http.StatusOK,
			setupMocks: expectLookup(42, activeDevice()),
		},
		{
			name:       "lowercase bearer",
			path:       "/iam/v2/connection-ticket",
			scheme:     "bearer",
			token:      token,
			body:       `{"device_id":"42"}`,
			code:       http.StatusOK,
			setupMocks: expectLookup(42, activeDevice()),
		},
		{
			name:       "mixed case bearer with multiple spaces",
			path:       "/iam/v2/connection-ticket",
			scheme:     "bEaReR  ",
			token:      token,
			body:       `{"device_id":"42"}`,
			code:       http.StatusOK,
			setupMocks: expectLookup(42, activeDevice()),
		},
		{
			name:   "unsupported scheme",
			path:   "/iam/v2/connection-ticket",
			scheme: "Basic",
			token:  token,
			body:   `{"device_id":"42"}`,
			code:   http.StatusUnauthorized,
		},
		{
			name:   "bearer scheme suffix",
			path:   "/iam/v2/connection-ticket",
			scheme: "BearerX",
			token:  token,
			body:   `{"device_id":"42"}`,
			code:   http.StatusUnauthorized,
		},
		{
			name: "missing token",
			path: "/iam/v2/connection-ticket",
			body: `{"device_id":"42"}`,
			code: http.StatusUnauthorized,
		},
		{
			name:  "bad token",
			path:  "/iam/v2/connection-ticket",
			token: "bad",
			body:  `{"device_id":"42"}`,
			code:  http.StatusUnauthorized,
		},
		{
			name:       "not owner",
			path:       "/iam/v2/connection-ticket",
			token:      tokenFor("other"),
			body:       `{"device_id":"42"}`,
			code:       http.StatusNotFound,
			setupMocks: expectLookup(42, activeDevice()),
		},
		{
			name:       "missing device",
			path:       "/iam/v2/connection-ticket",
			token:      token,
			body:       `{"device_id":"43"}`,
			code:       http.StatusNotFound,
			setupMocks: expectLookup(43, nil),
		},
		{
			name:  "invalid body",
			path:  "/iam/v2/connection-ticket",
			token: token,
			body:  `{`,
			code:  http.StatusBadRequest,
		},
		{
			name:  "empty request",
			path:  "/iam/v2/connection-ticket",
			token: token,
			body:  `{}`,
			code:  http.StatusBadRequest,
		},
		{
			name:  "body too large",
			path:  "/iam/v2/connection-ticket",
			token: token,
			body:  string(bytes.Repeat([]byte{'x'}, 2048)),
			code:  http.StatusBadRequest,
		},
		{
			name:  "previous API version",
			path:  "/iam/v1/connection-ticket",
			token: token,
			body:  `{"device_id":"42"}`,
			code:  http.StatusNotFound,
		},
		{
			name:  "legacy route",
			path:  "/iam/v1/exchange-key",
			token: token,
			body:  `{}`,
			code:  http.StatusNotFound,
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				if tc.setupMocks != nil {
					tc.setupMocks(db, reader)
				}

				var req *http.Request
				req, err = http.NewRequest(
					http.MethodPost,
					tlsServer.URL+tc.path,
					bytes.NewBufferString(tc.body),
				)
				require.NoError(t, err)

				var resp *http.Response
				req.Header.Set("Content-Type", "application/json")
				if tc.token != "" {
					scheme := tc.scheme
					if scheme == "" {
						scheme = "Bearer"
					}
					req.Header.Set("Authorization", scheme+" "+tc.token)
				}
				resp, err = tlsServer.Client().Do(req)
				require.NoError(t, err)
				defer func() { _ = resp.Body.Close() }()

				var body []byte
				body, err = io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.Equal(t, tc.code, resp.StatusCode, string(body))
				require.Equal(t, "no-store", resp.Header.Get("Cache-Control"))

				if tc.code == 200 {
					var result api.ConnectionTicketResp
					require.NoError(t, json.Unmarshal(body, &result))

					var buf []byte
					buf, err = json.Marshal(result.Hub)
					require.NoError(t, err)

					var descriptor *hubendpoint.Descriptor
					descriptor, err = hubendpoint.Parse(buf)
					require.NoError(t, err)
					require.Equal(t, pub[:], descriptor.PublicKey)
					require.NotEmpty(t, result.Ticket)
				}
			},
		)
	}
}
