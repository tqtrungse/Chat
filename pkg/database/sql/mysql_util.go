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

package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
)

// MySQL server error codes (ER_*) that indicate server/network/connection
// trouble rather than a query-shape, schema, or permission problem in our
// own code. See
// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
//
// If you're on a cluster with its own failure modes (Galera, Group
// Replication, ProxySQL failover), extend this list with the relevant
// codes for that topology — these cover a standalone primary + read
// replicas only.
const (
	mysqlErrConCountError           = 1040 // ER_CON_COUNT_ERROR - too many connections, server overloaded
	mysqlErrBadHost                 = 1042 // ER_BAD_HOST_ERROR - can't resolve/reach client host
	mysqlErrHandshake               = 1043 // ER_HANDSHAKE_ERROR - handshake failed
	mysqlErrServerShutdown          = 1053 // ER_SERVER_SHUTDOWN - server shutting down
	mysqlErrHostIsBlocked           = 1129 // ER_HOST_IS_BLOCKED - host blocked after repeated connection errors
	mysqlErrNetReadError            = 1158 // ER_NET_READ_ERROR
	mysqlErrNetReadInterrupted      = 1159 // ER_NET_READ_INTERRUPTED
	mysqlErrNetErrorOnWrite         = 1160 // ER_NET_ERROR_ON_WRITE
	mysqlErrNetWriteInterrupted     = 1161 // ER_NET_WRITE_INTERRUPTED
	mysqlErrOptionPreventsStatement = 1290 // ER_OPTION_PREVENTS_STATEMENT - server (or connection role) is read-only
)

// buildMySqlDSN builds a properly escaped MySQL DSN by copying
// go-sql-driver/mysql's Config/FormatDSN  instead of hand-rolled fmt.Sprintf,
// so a username or  password containing '@', ':' or '/' can't break DSN parsing.
func buildMySqlDSN(
	user, passwd, addr, dbName string,
	multiStatements bool,
) string {
	cfg := gomysql.Config{
		User:            user,
		Passwd:          passwd,
		Net:             "tcp",
		Addr:            addr,
		DBName:          dbName,
		Loc:             time.UTC,
		MultiStatements: multiStatements,
		ParseTime:       true,
		Params: map[string]string{
			"charset": "utf8mb4",
		},
	}
	return cfg.FormatDSN()
}

// IsMySQLInfraError reports whether err reflects MySQL server/network/connection
// trouble rather than a query-shape, schema, or auth problem in our own
// code. Infra errors are candidates for retry-with-backoff (on an
// idempotent statement) and a circuit-breaker health signal; non-infra
// errors are bugs to fix — retrying them changes nothing.
//
// role matters for exactly one code: 1290 (read-only). On RolePrimary it's
// unexpected — the primary demoted itself or is mid-failover — a real
// topology-change signal worth retrying against a freshly re-resolved
// primary. On RoleReplica it's normal whenever a write leaks into the read
// pool — an app-side routing bug, not a sign that replica is unhealthy —
// so it's excluded there to avoid tripping that replica's breaker for a
// bug in our own code. Pass the role of whichever connection/pool produced
// err.
//
// Deliberately excluded regardless of role: 1205 (lock wait timeout) and
// 1213 (deadlock). Both mean the server is healthy but two transactions
// collided — the right response is to retry the transaction itself, not to
// count it against the server's health. Wire those into a separate retry
// check if you need that distinction; folding them in here would trip the
// breaker on ordinary contention, not on an actual outage.
//
// Works through wrapped errors (GORM passes the raw driver error through
// on tx.Error without wrapping it further, and this also matches your own
// fmt.Errorf("%w", ...) wrapping) since it uses errors.Is/errors.As, not a
// direct type switch.
func IsMySQLInfraError(role Role, err error) bool {
	if err == nil {
		return false
	}

	switch {
	case errors.Is(err, context.Canceled):
		// Caller gave up; not a server problem.
		return false
	case errors.Is(err, context.DeadlineExceeded):
		// Our own deadline tripped waiting on the server — treat as infra.
		return true
	case errors.Is(err, driver.ErrBadConn),
		errors.Is(err, gomysql.ErrInvalidConn):
		// Connection judged broken by database/sql or the driver itself —
		// the standard signal that it's safe to retry on a new connection.
		return true
	case errors.Is(err, sql.ErrConnDone), errors.Is(err, sql.ErrTxDone):
		// Using a connection/transaction after it was returned/committed —
		// a bug in our own connection handling, not a server problem.
		return false
	}

	// Server-returned MySQL error: classify by code.
	var myErr *gomysql.MySQLError
	if errors.As(err, &myErr) {
		switch myErr.Number {
		case mysqlErrOptionPreventsStatement:
			// See the role discussion in the doc comment above.
			return role == RolePrimary
		case mysqlErrConCountError, mysqlErrBadHost, mysqlErrHandshake,
			mysqlErrServerShutdown, mysqlErrHostIsBlocked,
			mysqlErrNetReadError, mysqlErrNetReadInterrupted,
			mysqlErrNetErrorOnWrite, mysqlErrNetWriteInterrupted:
			return true
		default:
			// Syntax error, unknown column/table, duplicate key, access
			// denied, and similar — a bug or a data/schema mismatch in our
			// own code, not a server problem.
			return false
		}
	}

	// Below the MySQL protocol: raw network failure while reaching the
	// server (dial refused, i/o timeout, connection reset).
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	return false
}
