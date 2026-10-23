/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"xxx/pkg/log"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/stretchr/testify/require"
)

func TestRequestLoggerUsesApplicationLogger(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		panic  bool
	}{
		{name: "response", status: http.StatusCreated},
		{name: "empty response", status: http.StatusOK},
		{name: "recovered panic", status: http.StatusInternalServerError, panic: true},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "requests.log")
				cfg := log.FileConfig{Filename: path}
				cfg.Load(nil)

				logger, closeLogger, err := log.NewLogger(&cfg)
				require.NoError(t, err)
				t.Cleanup(func() { _ = closeLogger() })

				router := chi.NewRouter()
				router.Use(middleware.RequestLogger(requestLogFormatter{logger: logger}))
				router.Use(middleware.Recoverer)
				router.Post("/test", func(w http.ResponseWriter, r *http.Request) {
					if tc.panic {
						panic("test panic")
					}
					if tc.status == http.StatusCreated {
						w.WriteHeader(tc.status)
						_, _ = w.Write([]byte("ok"))
					}
				})

				req := httptest.NewRequest(
					http.MethodPost,
					"/test?secret=query-secret",
					strings.NewReader("body-secret"),
				)
				req.Header.Set("Authorization", "Bearer token-secret")
				response := httptest.NewRecorder()
				router.ServeHTTP(response, req)
				require.Equal(t, tc.status, response.Code)
				require.NoError(t, closeLogger())

				data, err := os.ReadFile(path)
				require.NoError(t, err)
				for _, secret := range []string{"query-secret", "body-secret", "token-secret"} {
					require.NotContains(t, string(data), secret)
				}

				lines := strings.Split(strings.TrimSpace(string(data)), "\n")
				if tc.panic {
					require.Len(t, lines, 2)

					var panicEntry map[string]any
					require.NoError(t, json.Unmarshal([]byte(lines[0]), &panicEntry))
					require.Equal(t, "error", panicEntry["level"])
					require.Equal(t, "test panic", panicEntry["panic"])
					require.NotEmpty(t, panicEntry["stack"])
				} else {
					require.Len(t, lines, 1)
				}

				var entry map[string]any
				require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &entry))
				require.Equal(t, "info", entry["level"])
				require.Equal(t, "POST", entry["method"])
				require.Equal(t, "/test", entry["path"])
				require.Equal(t, float64(tc.status), entry["status"])
				require.Equal(t, float64(response.Body.Len()), entry["bytes"])
				require.Contains(t, entry, "duration")
			},
		)
	}
}
