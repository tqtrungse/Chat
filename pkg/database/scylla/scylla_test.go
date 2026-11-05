/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package scylla

import (
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
)

type retryableQuery struct {
	gocql.RetryableQuery
	attempts int
}

func (q retryableQuery) Attempts() int { return q.attempts }

func TestNewClusterConfigQueryRetries(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value any
		want  int
	}{
		{
			name:  "default",
			value: nil,
			want:  3,
		},
		{
			name:  "disabled",
			value: 0,
			want:  0,
		},
		{
			name:  "custom",
			value: 7,
			want:  7,
		},
		{
			name:  "negative uses default",
			value: -1,
			want:  3,
		},
	} {
		t.Run(
			tt.name,
			func(t *testing.T) {
				loader := hostsLoader()
				if tt.value != nil {
					loader.Set("SCYLLA_DATABASE_NUM_RETRIES", tt.value)
				}
				cfg, err := LoadConfig(loader)
				require.NoError(t, err)
				cluster := newClusterConfig(cfg)
				require.NotNil(t, cluster.RetryPolicy)

				// Attempts includes the initial execution. Exercise the policy
				// installed on the cluster, including the zero-retry boundary.
				for attempts := 1; attempts <= tt.want+1; attempts++ {
					require.Equal(
						t,
						attempts <= tt.want,
						cluster.RetryPolicy.Attempt(retryableQuery{attempts: attempts}),
						"attempts=%d", attempts,
					)
				}
			},
		)
	}
}

func TestNewClusterConfigReconnectionPolicy(t *testing.T) {
	for _, tt := range []struct {
		name    string
		retries uint16
		base    time.Duration
		cap     time.Duration
	}{
		{
			name:    "default",
			retries: 3,
			base:    2 * time.Second,
			cap:     time.Minute,
		},
		{
			name:    "custom",
			retries: 7,
			base:    3 * time.Second,
			cap:     45 * time.Second,
		},
	} {
		t.Run(
			tt.name,
			func(t *testing.T) {
				cfg := Config{
					Hosts:                  []string{"127.0.0.1"},
					ReconnectMaxRetries:    tt.retries,
					ReconnectionJitterBase: tt.base,
					ReconnectionJitterCap:  tt.cap,
				}
				cfg.applyDefault()
				cluster := newClusterConfig(cfg)
				rp, ok := cluster.ReconnectionPolicy.(*gocql.ExponentialReconnectionPolicy)
				require.True(t, ok)
				require.Equal(t, int(tt.retries), rp.GetMaxRetries())
				require.Positive(t, rp.GetMaxRetries())
				require.Equal(t, tt.base, rp.InitialInterval)
				require.Equal(t, tt.cap, rp.MaxInterval)
			},
		)
	}
}

func TestScyllaOpenPublishesConfiguredRetryPolicy(t *testing.T) {
	cfg := Config{Hosts: []string{"127.0.0.1"}, NumRetries: 7}
	// Invalid serial consistency fails driver validation before any network
	// connection. This checks the open path without requiring a live cluster.
	cfg.SerialConsistency = gocql.One
	db, open, closeDB := NewScylla(cfg, nil)
	t.Cleanup(closeDB)
	require.Error(t, open())
	require.Nil(t, db.Session())

	rp, ok := db.IdempotentRetryPolicy().(*gocql.SimpleRetryPolicy)
	require.True(t, ok)
	require.Equal(t, 7, rp.NumRetries)
}
