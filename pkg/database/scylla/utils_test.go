/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package scylla

import (
	"context"
	"errors"
	"fmt"
	"net"
	"syscall"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
)

type requestError struct{ code int }

func (e requestError) Code() int       { return e.code }
func (e requestError) Message() string { return "CQL request failed" }
func (e requestError) Error() string   { return e.Message() }

func TestIsInfraError(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil",
			err:  nil,
			want: false,
		},
		{
			name: "canceled",
			err:  context.Canceled,
			want: false,
		},
		{
			name: "deadline",
			err:  context.DeadlineExceeded,
			want: true,
		},
		{
			name: "no hosts",
			err:  gocql.ErrNoHosts,
			want: true,
		},
		{
			name: "no connections started",
			err:  gocql.ErrNoConnectionsStarted,
			want: true,
		},
		{
			name: "no connections started",
			err:  gocql.ErrNoConnectionsStarted,
			want: true,
		},
		{
			name: "no connections",
			err:  gocql.ErrNoConnections,
			want: true,
		},
		{
			name: "no connections in pool",
			err:  gocql.ErrNoConnectionsInPool,
			want: true,
		},
		{
			name: "host down",
			err:  gocql.ErrHostDown,
			want: true,
		},
		{
			name: "no pool",
			err:  gocql.ErrNoPool,
			want: true,
		},
		{
			name: "host query failed",
			err:  gocql.ErrHostQueryFailed,
			want: true,
		},
		{
			name: "timeout no response",
			err:  gocql.ErrTimeoutNoResponse,
			want: true,
		},
		{
			name: "connection closed",
			err:  gocql.ErrConnectionClosed,
			want: true,
		},
		{
			name: "no streams",
			err:  gocql.ErrNoStreams,
			want: true,
		},
		{
			name: "connection refused",
			err:  &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
			want: true,
		},
		{
			name: "network cancellation",
			err:  &net.OpError{Op: "read", Net: "tcp", Err: context.Canceled},
		},
		{
			name: "not found",
			err:  gocql.ErrNotFound,
		},
		{
			name: "application error",
			err:  errors.New("invalid message"),
		},
	} {
		t.Run(
			tt.name,
			func(t *testing.T) {
				require.Equal(t, tt.want, IsInfraError(tt.err))
				if tt.err != nil {
					err := fmt.Errorf("query failed: %w", tt.err)
					require.Equal(t, tt.want, IsInfraError(err))
				}
			},
		)
	}
}

func TestIsInfraErrorCQLCodes(t *testing.T) {
	for _, tt := range []struct {
		name string
		code int
		want bool
	}{
		{
			name: "server",
			code: gocql.ErrCodeServer,
			want: true,
		},
		{
			name: "unavailable",
			code: gocql.ErrCodeUnavailable,
			want: true,
		},
		{
			name: "overloaded",
			code: gocql.ErrCodeOverloaded,
			want: true,
		},
		{
			name: "bootstrapping",
			code: gocql.ErrCodeBootstrapping,
			want: true,
		},
		{
			name: "write timeout",
			code: gocql.ErrCodeWriteTimeout,
			want: true,
		},
		{
			name: "read timeout",
			code: gocql.ErrCodeReadTimeout,
			want: true,
		},
		{
			name: "read failure",
			code: gocql.ErrCodeReadFailure,
			want: true,
		},
		{
			name: "write failure",
			code: gocql.ErrCodeWriteFailure,
			want: true,
		},
		{
			name: "protocol",
			code: gocql.ErrCodeProtocol,
		},
		{
			name: "bad credentials",
			code: gocql.ErrCodeCredentials,
		},
		{
			name: "syntax",
			code: gocql.ErrCodeSyntax,
		},
		{
			name: "unauthorized",
			code: gocql.ErrCodeUnauthorized,
		},
		{
			name: "invalid",
			code: gocql.ErrCodeInvalid,
		},
		{
			name: "config",
			code: gocql.ErrCodeConfig,
		},
		{
			name: "already exists",
			code: gocql.ErrCodeAlreadyExists,
		},
		{
			name: "unprepared",
			code: gocql.ErrCodeUnprepared,
		},
		{
			name: "function failure",
			code: gocql.ErrCodeFunctionFailure,
		},
		{
			name: "unknown",
			code: 0x7fff,
		},
	} {
		t.Run(
			tt.name,
			func(t *testing.T) {
				err := requestError{code: tt.code}
				require.Equal(t, tt.want, IsInfraError(err))
				require.Equal(t, tt.want, IsInfraError(fmt.Errorf("query failed: %w", err)))
			},
		)
	}
}
