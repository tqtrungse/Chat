/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package scylla

import (
	"context"
	"errors"
	"net"

	"github.com/gocql/gocql"
)

// CQL native-protocol error codes.
//
// gocql does not export these codes (see errors.go in gocql). Therefore, this
// package defines the codes again. The values come from the Cassandra native
// protocol specification. They are the same in all protocol versions. They are
// not an implementation detail of gocql.
//
// Use these codes to classify a gocql.RequestError with its Code() method.
// This way, the classification does not depend on the exported gocql struct
// type (if any) that wraps a given code.
const (
	cqlErrServer        = 0x0000 // The server has an internal error.
	cqlErrUnavailable   = 0x1000 // Too few replicas are alive for the consistency level.
	cqlErrOverloaded    = 0x1001 // The node rejects load.
	cqlErrBootstrapping = 0x1002 // The node is not ready.
	cqlErrWriteTimeout  = 0x1100
	cqlErrReadTimeout   = 0x1200
	cqlErrReadFailure   = 0x1300
	cqlErrWriteFailure  = 0x1500
)

// IsInfraError reports whether err shows a failure in the cluster, the
// network, or a node. It reports false if the cause is a problem in our code:
// a bad query shape, a schema error, or an authentication error.
//
// Use the result to decide what to do with the error:
//   - You can retry an infra error with backoff, if the query is idempotent.
//     An infra error is also a health signal for the circuit breaker.
//   - A non-infra error is a bug. Fix the bug. A retry does not change the
//     result.
//
// This function works with wrapped errors. Examples are the errors from gocqlx
// and the errors that fmt.Errorf wraps with %w. The function uses errors.Is and
// errors.As. It does not use a direct type switch.
func IsInfraError(err error) bool {
	if err == nil {
		return false
	}

	switch {
	case errors.Is(err, context.Canceled):
		// The caller cancelled the request. This is not a problem in the
		// cluster.
		return false
	case errors.Is(err, context.DeadlineExceeded):
		// Our own deadline expired before the cluster gave an answer.
		// This is an infra error.
		return true
	case errors.Is(err, gocql.ErrNoHosts),
		errors.Is(err, gocql.ErrNoConnectionsStarted),
		errors.Is(err, gocql.ErrNoConnections),
		errors.Is(err, gocql.ErrNoConnectionsInPool),
		errors.Is(err, gocql.ErrHostDown),
		errors.Is(err, gocql.ErrNoPool),
		errors.Is(err, gocql.ErrHostQueryFailed),
		errors.Is(err, gocql.ErrTimeoutNoResponse),
		errors.Is(err, gocql.ErrConnectionClosed),
		errors.Is(err, gocql.ErrNoStreams):
		return true
	}

	// The server returned an error at the level of the CQL protocol.
	reqErr, ok := errors.AsType[gocql.RequestError](err)
	if ok {
		switch reqErr.Code() {
		case cqlErrServer,
			cqlErrUnavailable,
			cqlErrOverloaded,
			cqlErrBootstrapping,
			cqlErrWriteTimeout,
			cqlErrReadTimeout,
			cqlErrReadFailure,
			cqlErrWriteFailure:
			return true
		default:
			// These codes are Syntax, Unauthorized, Invalid, Config,
			// Bad_credentials, Protocol, Already_exists, Unprepared, and
			// Function_failure. Each one shows a bug in our code, or a
			// wrong permission or schema. None of them shows a problem in
			// the cluster.
			return false
		}
	}

	// The failure is below the CQL protocol. The driver did not reach a node.
	// Examples are a refused dial, an i/o timeout, and a reset connection.
	var netErr net.Error
	return errors.As(err, &netErr)
}
