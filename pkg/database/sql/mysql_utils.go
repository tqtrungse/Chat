/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"strings"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
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

// mysqlDialector scopes GORM's version detection to the caller's startup
// context while retaining the driver's version-specific compatibility flags.
type mysqlDialector struct {
	*gormmysql.Dialector
	ctx context.Context
}

func newMySQLDialector(ctx context.Context, pool *sql.DB) gorm.Dialector {
	return &mysqlDialector{
		Dialector: &gormmysql.Dialector{
			Config: &gormmysql.Config{Conn: pool},
		},
		ctx: ctx,
	}
}

// Initialize
//
// GORM need to fetch MySQL version to generate appropriate SQL.
// GORM uses default context.Background(), so input context with cancel or deadline will not be used.
// This function will override fetching version step of GORM with input context.
func (d *mysqlDialector) Initialize(db *gorm.DB) error {
	connPool := d.Conn
	d.Conn = initializationPool{
		ConnPool: connPool,
		ctx:      d.ctx,
	}
	defer func() {
		// Runtime queries must use their own contexts and the original pool.
		d.Conn = connPool
		db.ConnPool = connPool
	}()
	return d.Dialector.Initialize(db)
}

// buildMySqlDSN builds a properly escaped MySQL DSN by copying
// go-sql-driver/mysql's Config/FormatDSN  instead of hand-rolled fmt.Sprintf,
// so a username or  password containing '@', ':' or '/' can't break DSN parsing.
func buildMySqlDSN(
	user string,
	passwd string,
	addr string,
	dbName string,
	dialTimeout time.Duration,
	readTimeout time.Duration,
	writeTimeout time.Duration,
	multiStatements bool,
) string {
	cfg := gomysql.NewConfig()
	cfg.User = user
	cfg.Passwd = passwd
	cfg.Net = "tcp"
	cfg.Addr = addr
	cfg.DBName = dbName
	cfg.Loc = time.UTC
	cfg.Params = map[string]string{"charset": "utf8mb4"}
	cfg.Timeout = dialTimeout
	cfg.ReadTimeout = readTimeout
	cfg.WriteTimeout = writeTimeout
	cfg.MultiStatements = multiStatements
	cfg.ParseTime = true

	return cfg.FormatDSN()
}

// IsMySQLInfraError reports whether err is a recognized MySQL server, network,
// connection, or topology failure suitable for a circuit-breaker health signal.
// A true result does not guarantee that retrying is safe; retry decisions must
// also account for idempotency and transaction state. A false result does not
// mean success or that the error can be ignored: callers must still handle it.
//
// Caller cancellation and deadlines are excluded: they can expire before a
// query reaches the server, including while waiting for a pool connection.
// Driver/network timeouts remain infrastructure errors.
//
// This classification assumes a writable primary and replicas used for reads.
// Pass the role of the actual pool that produced err, which may differ from
// the requested route when reads fall back to the primary.
// Role affects only error 1290 (ER_OPTION_PREVENTS_STATEMENT), which covers
// multiple server restrictions. Only messages identifying --read-only or
// --super-read-only are treated as infra errors on RolePrimary: they may
// indicate demotion, failover, or misconfiguration, not proof of failover.
// On RoleReplica, rejecting a client write is expected protection and usually
// indicates incorrect application routing, not an unhealthy read pool.
// Restrictions such as --secure-file-priv and unrecognized 1290 messages are
// excluded for every role. Read-only detection depends on the message format.
//
// Deliberately excluded regardless of role: 1205 (lock wait timeout) and
// 1213 (deadlock). They indicate lock contention and do not by themselves
// establish a server outage. Handle transaction recovery/retry separately.
//
// Wrapped errors are supported through errors.Is and errors.AsType.
func IsMySQLInfraError(role Role, err error) bool {
	if err == nil {
		return false
	}

	switch {
	case errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		// Check before net.Error: context.DeadlineExceeded also implements it.
		return false

	case errors.Is(err, driver.ErrBadConn),
		errors.Is(err, gomysql.ErrInvalidConn):
		// Connection judged broken by database/sql or the driver itself —
		// the standard signal that it's safe to retry on a new connection.
		return true

	case errors.Is(err, sql.ErrConnDone),
		errors.Is(err, sql.ErrTxDone):
		// Using a connection/transaction after it was returned/committed —
		// a bug in our own connection handling, not a server problem.
		return false
	}

	// Server-returned MySQL error: classify by code.
	myErr, ok := errors.AsType[*gomysql.MySQLError](err)
	if ok {
		switch myErr.Number {
		case mysqlErrOptionPreventsStatement:
			// Error 1290 covers multiple server options, including --read-only,
			// --super-read-only, and --secure-file-priv. Only a recognized
			// read-only restriction on the primary is classified as a possible
			// topology/configuration problem. Replica write rejection, other
			// restrictions, and unrecognized messages are excluded.
			if role != RolePrimary {
				return false
			}

			msg := strings.ToLower(myErr.Message)
			msg = strings.ReplaceAll(msg, "_", "-")

			return strings.Contains(msg, "--read-only option") ||
				strings.Contains(msg, "--super-read-only option")

		case mysqlErrConCountError,
			mysqlErrBadHost,
			mysqlErrHandshake,
			mysqlErrServerShutdown,
			mysqlErrHostIsBlocked,
			mysqlErrNetReadError,
			mysqlErrNetReadInterrupted,
			mysqlErrNetErrorOnWrite,
			mysqlErrNetWriteInterrupted:
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
	return errors.As(err, &netErr)
}
