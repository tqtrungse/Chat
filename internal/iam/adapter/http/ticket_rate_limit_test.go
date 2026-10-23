/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	api "xxx/api/iam/v2/json"
	httpmocks "xxx/internal/iam/adapter/http/mocks"
	"xxx/internal/iam/application/connticket"
	"xxx/internal/iam/core/rejection"
	"xxx/internal/iam/core/ticketlimit"
	limitmocks "xxx/internal/iam/core/ticketlimit/mocks"

	appjwt "xxx/pkg/jwt"
	"xxx/pkg/log"
	"xxx/pkg/reject"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestIssueConnectionTicketAppliesAdmissionBeforeIssuer(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, tc := range []struct {
		name           string
		subject        ticketlimit.Decision
		device         ticketlimit.Decision
		body           string
		deviceChecked  bool
		issuerCalled   bool
		missingSubject bool
		status         int
		retryAfter     int
	}{
		{
			name: "subject quota before body parsing",
			subject: ticketlimit.Decision{
				Reason:     ticketlimit.SubjectLimited,
				RetryAfter: 3,
			},
			body:       "{",
			status:     http.StatusTooManyRequests,
			retryAfter: 3,
		},
		{
			name: "subject capacity",
			subject: ticketlimit.Decision{
				Reason:     ticketlimit.SubjectCapacity,
				RetryAfter: 7,
			},
			body:       `{"device_id":"42"}`,
			status:     http.StatusServiceUnavailable,
			retryAfter: 7,
		},
		{
			name: "device quota",
			device: ticketlimit.Decision{
				Reason:     ticketlimit.DeviceLimited,
				RetryAfter: 2,
			},
			body:          `{"device_id":"42"}`,
			deviceChecked: true,
			status:        http.StatusTooManyRequests,
			retryAfter:    2,
		},
		{
			name: "subject device capacity",
			device: ticketlimit.Decision{
				Reason:     ticketlimit.SubjectDeviceCapacity,
				RetryAfter: 8,
			},
			body:          `{"device_id":"42"}`,
			deviceChecked: true,
			status:        http.StatusTooManyRequests,
			retryAfter:    8,
		},
		{
			name: "device table capacity",
			device: ticketlimit.Decision{
				Reason:     ticketlimit.DeviceCapacity,
				RetryAfter: 9,
			},
			body:          `{"device_id":"42"}`,
			deviceChecked: true,
			status:        http.StatusServiceUnavailable,
			retryAfter:    9,
		},
		{
			name:   "invalid body still consumes subject attempt",
			body:   "{",
			status: http.StatusBadRequest,
		},
		{
			name:   "zero device is rejected before device admission",
			body:   `{"device_id":"0"}`,
			status: http.StatusBadRequest,
		},
		{
			name:          "canonical numeric device key",
			body:          `{"device_id":"042"}`,
			deviceChecked: true,
			issuerCalled:  true,
			status:        http.StatusNotFound,
		},
		{
			name:           "missing subject",
			missingSubject: true,
			body:           `{"device_id":"42"}`,
			status:         http.StatusUnauthorized,
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				logger, closeLogger, err := log.NewLogger(nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, closeLogger()) })

				limiter := limitmocks.NewMockLimiter(ctrl)
				issuer := httpmocks.NewMockTicketIssuer(ctrl)
				server := &Server{
					ticketLimiter: limiter,
					ticketIssuer:  issuer,
					logger:        logger,
					rejections: reject.New[rejection.Reason](
						logger,
						0,
						rejection.Names(),
					),
				}
				req := httptest.NewRequest(
					http.MethodPost,
					"/iam/v2/connection-ticket",
					strings.NewReader(tc.body),
				)

				if !tc.missingSubject {
					req = req.WithContext(context.WithValue(
						req.Context(),
						claimsContextKey,
						&appjwt.Claims{Subject: "owner"},
					))
				}

				var calls []any
				if !tc.missingSubject {
					calls = append(
						calls,
						limiter.
							EXPECT().
							AllowSubject("owner").
							Return(tc.subject),
					)
				}

				if tc.deviceChecked {
					calls = append(
						calls,
						limiter.
							EXPECT().
							AllowDevice("owner", uint64(42)).
							Return(tc.device),
					)
				}

				if tc.issuerCalled {
					calls = append(
						calls,
						issuer.
							EXPECT().
							Issue(
								req.Context(),
								"owner",
								api.ConnectionTicketReq{DeviceID: "042"},
							).
							Return(nil, connticket.ErrDeviceNotFound),
					)
				}

				gomock.InOrder(calls...)
				response := httptest.NewRecorder()
				server.issueConnectionTicket(response, req)
				require.Equal(t, tc.status, response.Code)

				retry := ""
				if tc.retryAfter > 0 {
					retry = strconv.Itoa(tc.retryAfter)
				}
				require.Equal(t, retry, response.Header().Get("Retry-After"))

				switch tc.status {
				case http.StatusTooManyRequests:
					require.Equal(t, errTicketRateLimited.Error()+"\n", response.Body.String())

				case http.StatusServiceUnavailable:
					require.Equal(t, errTicketCapacity.Error()+"\n", response.Body.String())
				}
			},
		)
	}
}

func TestNewServerRequiresTicketLimiter(t *testing.T) {
	server, err := NewServer(
		t.Context(),
		Config{},
		nil,
		nil,
		nil,
		nil,
	)
	require.ErrorIs(t, err, errTicketLimiterRequired)
	require.Nil(t, server)
}
