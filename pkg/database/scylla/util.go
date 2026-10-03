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

package scylla

import (
	"context"
	"errors"
	"net"

	"github.com/gocql/gocql"
)

// CQL native-protocol error codes. gocql keeps these unexported (see its
// errors.go), so they're reproduced here from the Cassandra native protocol
// spec (stable across protocol versions, not a gocql implementation detail)
// to classify a gocql.RequestError by Code() without depending on which of
// gocql's own exported struct types (if any) wraps a given code.
const (
	cqlErrServer        = 0x0000 // generic internal server error
	cqlErrUnavailable   = 0x1000 // not enough replicas alive to meet CL
	cqlErrOverloaded    = 0x1001 // node shedding load
	cqlErrBootstrapping = 0x1002 // node not ready yet
	cqlErrWriteTimeout  = 0x1100
	cqlErrReadTimeout   = 0x1200
	cqlErrReadFailure   = 0x1300
	cqlErrWriteFailure  = 0x1500
)

// IsInfraError reports whether err reflects a cluster/network/node-level
// failure rather than a query-shape, schema, or auth problem in our own
// code. Infra errors are candidates for retry-with-backoff (on an
// idempotent query) and a circuit-breaker health signal; non-infra errors
// are bugs to fix — retrying them changes nothing.
//
// Works through wrapped errors (gocqlx or your own fmt.Errorf("%w", ...))
// since it matches via errors.Is/errors.As, not a direct type switch.
func IsInfraError(err error) bool {
	if err == nil {
		return false
	}

	switch {
	case errors.Is(err, context.Canceled):
		// Caller gave up; not a cluster problem.
		return false
	case errors.Is(err, context.DeadlineExceeded):
		// Our own deadline tripped waiting on the cluster — treat as infra.
		return true
	case errors.Is(err, gocql.ErrNoHosts),
		errors.Is(err, gocql.ErrNoConnectionsStarted),
		errors.Is(err, gocql.ErrHostQueryFailed),
		errors.Is(err, gocql.ErrTimeoutNoResponse),
		errors.Is(err, gocql.ErrTooManyTimeouts),
		errors.Is(err, gocql.ErrConnectionClosed),
		errors.Is(err, gocql.ErrNoStreams):
		return true
	}

	// CQL protocol-level error returned by the server.
	var reqErr gocql.RequestError
	if errors.As(err, &reqErr) {
		switch reqErr.Code() {
		case cqlErrServer, cqlErrUnavailable, cqlErrOverloaded,
			cqlErrBootstrapping, cqlErrWriteTimeout, cqlErrReadTimeout,
			cqlErrReadFailure, cqlErrWriteFailure:
			return true
		default:
			// Syntax / Unauthorized / Invalid / Config / Bad_credentials /
			// Protocol / Already_exists / Unprepared / Function_failure —
			// a bug or a permissions/schema mismatch in our own code, not
			// a cluster problem.
			return false
		}
	}

	// Below the CQL protocol: raw network failure while reaching a node
	// (dial refused, i/o timeout, connection reset).
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	return false
}
