/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package http

import (
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func requiredLoader() *viper.Viper {
	l := viper.New()
	l.Set("HTTP_SERVER_ADDR", ":8443")
	l.Set("HTTP_SERVER_CERT_FILE", "cert.pem")
	l.Set("HTTP_SERVER_KEY_FILE", "key.pem")
	return l
}

func TestConfigLoadDefaults(t *testing.T) {
	want := Config{
		Addr:              ":8443",
		CertFile:          "cert.pem",
		KeyFile:           "key.pem",
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       15 * time.Second,
		MaxHeaderBytes:    16384,
	}

	t.Run(
		"unset keys",
		func(t *testing.T) {
			cfg, err := LoadConfig(requiredLoader())
			require.NoError(t, err)
			require.Equal(t, want, cfg)
		},
	)
}

func TestConfigLoadRequiredFields(t *testing.T) {
	for _, tc := range []struct {
		name    string
		missing string
		wantErr string
	}{
		{
			name:    "addr",
			missing: "HTTP_SERVER_ADDR",
			wantErr: "HTTP_SERVER_ADDR is required",
		},
		{
			name:    "cert file",
			missing: "HTTP_SERVER_CERT_FILE",
			wantErr: "HTTP_SERVER_CERT_FILE and HTTP_SERVER_KEY_FILE are required; IAM serves HTTPS",
		},
		{
			name:    "key file",
			missing: "HTTP_SERVER_KEY_FILE",
			wantErr: "HTTP_SERVER_CERT_FILE and HTTP_SERVER_KEY_FILE are required; IAM serves HTTPS",
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				l := requiredLoader()
				l.Set(tc.missing, " ")
				_, err := LoadConfig(l)
				require.EqualError(t, err, tc.wantErr)
			},
		)
	}

	t.Run(
		"nil loader",
		func(t *testing.T) {
			_, err := LoadConfig(nil)
			require.EqualError(t, err, "HTTP_SERVER_ADDR is required")
		},
	)
}

func TestConfigLoadTimeouts(t *testing.T) {
	keys := []string{
		"HTTP_READ_HEADER_TIMEOUT",
		"HTTP_READ_TIMEOUT",
		"HTTP_WRITE_TIMEOUT",
		"HTTP_IDLE_TIMEOUT",
	}
	defaults := []time.Duration{
		5 * time.Second,
		10 * time.Second,
		30 * time.Second,
		15 * time.Second,
	}

	for _, tc := range []struct {
		name  string
		value any
		want  func(def time.Duration) time.Duration
	}{
		{
			name:  "zero uses default",
			value: "0s",
			want:  func(def time.Duration) time.Duration { return def },
		},
		{
			name:  "negative disables",
			value: "-1s",
			want:  func(time.Duration) time.Duration { return -time.Second },
		},
		{
			name:  "custom",
			value: "2s",
			want:  func(time.Duration) time.Duration { return 2 * time.Second },
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				l := requiredLoader()
				for _, key := range keys {
					l.Set(key, tc.value)
				}

				cfg, err := LoadConfig(l)
				require.NoError(t, err)

				got := []time.Duration{
					cfg.ReadHeaderTimeout,
					cfg.ReadTimeout,
					cfg.WriteTimeout,
					cfg.IdleTimeout,
				}
				for i, def := range defaults {
					require.Equal(t, tc.want(def), got[i], keys[i])
				}
			},
		)
	}
}

func TestConfigLoadPreservesExplicitMaxHeaderBytes(t *testing.T) {
	l := requiredLoader()
	l.Set("HTTP_MAX_HEADER_BYTES", 32768)

	cfg, err := LoadConfig(l)
	require.NoError(t, err)
	require.Equal(t, 32768, cfg.MaxHeaderBytes)
}

func TestConfigLoadNegativeTimeoutFromEnvironment(t *testing.T) {
	t.Setenv("HTTP_READ_TIMEOUT", "-1s")
	l := requiredLoader()
	l.AutomaticEnv()

	cfg, err := LoadConfig(l)
	require.NoError(t, err)
	require.Equal(t, -time.Second, cfg.ReadTimeout)
	require.Equal(t, 30*time.Second, cfg.WriteTimeout)
}

func TestConfigApplyDefaultKeepsNegativeTimeouts(t *testing.T) {
	cfg := Config{
		ReadHeaderTimeout: -1,
		IdleTimeout:       -1,
	}
	cfg.applyDefault()
	require.EqualValues(t, -1, cfg.ReadHeaderTimeout)
	require.EqualValues(t, -1, cfg.IdleTimeout)
	require.Equal(t, 10*time.Second, cfg.ReadTimeout)
	require.Equal(t, 30*time.Second, cfg.WriteTimeout)
	require.Equal(t, 16384, cfg.MaxHeaderBytes)
}
