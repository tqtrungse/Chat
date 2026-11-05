/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"xxx/pkg/log"

	gomysql "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
	"gorm.io/plugin/dbresolver"
)

// MySQL server error codes (ER_*). Each code shows trouble in the server, the
// network, or the connection. None of them shows a problem in our own code,
// such as a bad query shape, a schema error, or a wrong permission.
//
// For the full list of codes, see
// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
//
// These codes cover one standalone primary and its read replicas. A cluster
// has more failure modes of its own. Examples are Galera, Group Replication,
// and a ProxySQL failover. Add the codes of that topology to this list.
const (
	mysqlErrConCountError           = 1040 // ER_CON_COUNT_ERROR. The server has too many connections.
	mysqlErrBadHost                 = 1042 // ER_BAD_HOST_ERROR. The server cannot find the client host.
	mysqlErrHandshake               = 1043 // ER_HANDSHAKE_ERROR. The handshake failed.
	mysqlErrServerShutdown          = 1053 // ER_SERVER_SHUTDOWN. The server goes down.
	mysqlErrHostIsBlocked           = 1129 // ER_HOST_IS_BLOCKED. The server blocked the host after many connection errors.
	mysqlErrNetReadError            = 1158 // ER_NET_READ_ERROR
	mysqlErrNetReadInterrupted      = 1159 // ER_NET_READ_INTERRUPTED
	mysqlErrNetErrorOnWrite         = 1160 // ER_NET_ERROR_ON_WRITE
	mysqlErrNetWriteInterrupted     = 1161 // ER_NET_WRITE_INTERRUPTED
	mysqlErrOptionPreventsStatement = 1290 // ER_OPTION_PREVENTS_STATEMENT. The server or the role is read-only.
)

// IsMySQLInfraError reports whether err is a known failure of the MySQL
// server, the network, the connection, or the topology. Use the result as a
// health signal for the circuit breaker.
//
// A true result is not a promise that a retry is safe. The caller must also
// look at the idempotency of the query and at the state of the transaction. A
// false result is not a promise of success, and the caller must still handle
// the error.
//
// The function excludes a cancel and a deadline of the caller. Both can expire
// before the query reaches the server, and even while the caller waits for a
// connection from the pool. A timeout of the driver or the network stays an
// infra error.
//
// This classification expects one primary that accepts writes, and replicas
// that serve reads. Pass the role of the pool that gave err. That role can
// differ from the route of the request, because a read can fall back to the
// primary.
//
// The role changes the result only for error 1290
// (ER_OPTION_PREVENTS_STATEMENT), which covers several restrictions of the
// server:
//   - On RolePrimary, only a message about --read-only or --super-read-only is
//     an infra error. Such a message can show a demotion, a failover, or a
//     wrong configuration. It is not a proof of a failover.
//   - On RoleReplica, the server rejects a client write by design. That
//     rejection usually shows a wrong route in the application, and not an
//     unhealthy read pool.
//   - For every role, the function excludes --secure-file-priv and each
//     message that it does not know. The detection of the read-only state
//     depends on the format of the message.
//
// The function also excludes 1205 (lock wait timeout) and 1213 (deadlock) for
// every role. Both show contention on a lock. Neither one establishes an
// outage of the server. Handle the recovery or the retry of a transaction
// apart from this function.
//
// The function works with wrapped errors through errors.Is and errors.AsType.
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
		// database/sql or the driver found this connection broken. This is
		// the standard signal to retry on a new connection.
		return true

	case errors.Is(err, sql.ErrConnDone),
		errors.Is(err, sql.ErrTxDone):
		// Our code used a connection after it gave that connection back, or
		// a transaction after the commit. This is a bug in our code, and not
		// a problem in the server.
		return false
	}

	// The server returned a MySQL error. Classify it by its code.
	myErr, ok := errors.AsType[*gomysql.MySQLError](err)
	if ok {
		switch myErr.Number {
		case mysqlErrOptionPreventsStatement:
			// Error 1290 covers several options of the server. Examples are
			// --read-only, --super-read-only, and --secure-file-priv. This
			// code accepts only a known read-only restriction on the primary.
			// It excludes a write rejection on a replica, each other
			// restriction, and each message that it does not know.
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
			// Examples are a syntax error, an unknown column or table, a
			// duplicate key, and a refused access. Each one shows a bug in
			// our code, or a wrong schema or data. None of them shows a
			// problem in the server.
			return false
		}
	}

	// The failure is below the MySQL protocol. The driver did not reach the
	// server. Examples are a refused dial, an i/o timeout, and a reset
	// connection.
	var netErr net.Error
	return errors.As(err, &netErr)
}

type mysql struct {
	db       atomic.Pointer[gorm.DB]
	primary  *sql.DB
	replicas []*sql.DB
	logger   *log.Logger
	openPool openPoolFunc
	isClose  atomic.Bool
}

// NewMySql makes a DB that reports its statements through logger. A nil logger
// turns the statement log off.
func NewMySql(logger *log.Logger) DB {
	return &mysql{
		logger:   logger,
		openPool: openPool,
	}
}

func (m *mysql) Open(ctx context.Context, cfg Config) (func() error, error) {
	cfg.applyDefault()

	var (
		primary  *sql.DB
		replicas []*sql.DB
		ok       bool
		err      error
	)
	defer func() {
		if ok {
			return
		}

		if primary != nil {
			_ = primary.Close()
		}

		for _, r := range replicas {
			_ = r.Close()
		}
	}()

	primary, err = m.openPool(
		ctx,
		"mysql",
		buildMySqlDSN(
			cfg.User,
			cfg.Password,
			cfg.PrimaryAddr,
			cfg.DBName,
			cfg.DialTimeout,
			cfg.ReadTimeout,
			cfg.WriteTimeout,
			false,
		),
		cfg.Pool,
	)
	if err != nil {
		return nil, fmt.Errorf("open source pool: %w", err)
	}

	primaryDialector := newMySQLDialector(ctx, primary)
	db, err := gorm.Open(
		primaryDialector,
		&gorm.Config{
			// GORM wraps each single Create, Update and Delete in a
			// transaction of its own. InnoDB already makes one statement
			// atomic, and a caller that needs several statements to be atomic
			// asks for that through Transaction. The default would therefore
			// add a BEGIN and a COMMIT round trip to each write for nothing.
			SkipDefaultTransaction: true,
			// openPool already verifies each connection with PingContext.
			DisableAutomaticPing: true,
			// Without this field GORM takes logger.Default, which writes
			// plain text to os.Stdout outside the sink of pkg/log.
			Logger: m.buildLogger(cfg),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("gorm.Open: %w", err)
	}

	resolverCfg := dbresolver.Config{
		Sources: []gorm.Dialector{primaryDialector},
		Policy:  dbresolver.RandomPolicy{},
	}

	replicas = make([]*sql.DB, 0, len(cfg.ReplicaAddrs))
	for i, replicaAddr := range cfg.ReplicaAddrs {
		replica, repErr := m.openPool(
			ctx,
			"mysql",
			buildMySqlDSN(
				cfg.User,
				cfg.Password,
				replicaAddr,
				cfg.DBName,
				cfg.DialTimeout,
				cfg.ReadTimeout,
				cfg.WriteTimeout,
				false,
			),
			cfg.Pool,
		)
		if repErr != nil {
			return nil, fmt.Errorf("open replica pool %d (%s): %w", i, replicaAddr, repErr)
		}

		replicas = append(replicas, replica)
		resolverCfg.Replicas = append(resolverCfg.Replicas, newMySQLDialector(ctx, replica))
	}

	err = db.Use(dbresolver.Register(resolverCfg))
	if err != nil {
		return nil, fmt.Errorf("register dbresolver: %w", err)
	}

	err = ctx.Err()
	if err != nil {
		return nil, err
	}

	ok = true
	m.primary = primary
	m.replicas = replicas
	m.db.Store(db)
	return m.Close, err
}

func (m *mysql) Close() error {
	if !m.isClose.CompareAndSwap(false, true) {
		return nil
	}

	var err error
	if m.primary != nil {
		err = errors.Join(err, m.primary.Close())
	}

	for _, replica := range m.replicas {
		err = errors.Join(err, replica.Close())
	}

	m.primary = nil
	m.replicas = nil
	m.db.Store(nil)
	return err
}

func (m *mysql) Transaction(
	ctx context.Context,
	source TxSource,
	fn func(ctx context.Context) error,
) error {
	if m.isClose.Load() {
		return ErrDBClosed
	}

	db := m.db.Load()
	if db == nil {
		return ErrDBNotOpen
	}

	switch source {
	case TxSourcePrimary:
		db = db.Clauses(dbresolver.Write)
	case TxSourceReplicas:
		db = db.Clauses(dbresolver.Read)
	default:
		return ErrInvalidTxSource
	}

	return db.
		WithContext(ctx).
		Transaction(func(tx *gorm.DB) error {
			return fn(AttachDB(ctx, tx))
		})
}

func (m *mysql) WithDB(
	ctx context.Context,
	source TxSource,
	fn func(ctx context.Context) error,
) error {
	if m.isClose.Load() {
		return ErrDBClosed
	}

	db := m.db.Load()
	if db == nil {
		return ErrDBNotOpen
	}

	switch source {
	case TxSourcePrimary:
		db = db.Clauses(dbresolver.Write)
	case TxSourceReplicas:
		db = db.Clauses(dbresolver.Read)
	default:
		return ErrInvalidTxSource
	}

	dbCtx := AttachDB(ctx, db.WithContext(ctx))
	return fn(dbCtx)
}

// buildLogger gives the bridge from GORM to the logger of this instance.
//
// The component logger shares the sink of m.logger, so Close must not close
// it: the owner of m.logger closes that sink.
func (m *mysql) buildLogger(cfg Config) gormlogger.Interface {
	if m.logger == nil {
		return gormlogger.Discard
	}
	return newGormLogger(
		m.logger,
		cfg.SlowThreshold,
		cfg.LogParameters,
	)
}

// buildMySqlDSN makes a MySQL DSN with the correct escape of each value.
//
// It uses Config and FormatDSN from go-sql-driver/mysql, and not a
// fmt.Sprintf of its own. Therefore, a username or a password that holds '@',
// ':' or '/' cannot break the DSN.
func buildMySqlDSN(
	user string,
	password string,
	addr string,
	dbName string,
	dialTimeout time.Duration,
	readTimeout time.Duration,
	writeTimeout time.Duration,
	multiStatements bool,
) string {
	cfg := gomysql.NewConfig()
	cfg.User = user
	cfg.Passwd = password
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
