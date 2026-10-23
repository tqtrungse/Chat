/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package http

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"xxx/internal/iam/core/rejection"

	api "xxx/api/iam/v2/json"
	"xxx/pkg/log"
	"xxx/pkg/reject"

	"github.com/stretchr/testify/require"
)

type failedResponseWriter struct {
	*httptest.ResponseRecorder
	err error
}

func (w failedResponseWriter) Write([]byte) (int, error) { return 0, w.err }

func TestRespondSamplesPeerWriteFailures(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		count uint64
	}{
		{
			name:  "broken pipe",
			err:   syscall.EPIPE,
			count: 1,
		},
		{
			name:  "peer reset",
			err:   syscall.ECONNRESET,
			count: 1,
		},
		{
			name: "internal writer failure",
			err:  errors.New("writer failed"),
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				logger, closeLogger, err := log.NewLogger(nil)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, closeLogger()) })

				server := &Server{
					logger:     logger,
					rejections: reject.New[rejection.Reason](logger, 0, rejection.Names()),
				}
				w := failedResponseWriter{
					ResponseRecorder: httptest.NewRecorder(),
					err:              tc.err,
				}
				server.respond(w, &api.ConnectionTicketReq{DeviceID: "42"})
				require.Equal(t, http.StatusOK, w.Code, "headers were already sent")
				require.Equal(t, tc.count, server.rejections.Count(rejection.ResponseWrite))
			},
		)
	}
}
