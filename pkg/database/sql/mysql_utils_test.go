/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	gomysql "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestIsMySQLInfraError(t *testing.T) {
	for _, tc := range []struct {
		name string
		role Role
		err  error
		want bool
	}{
		{
			name: "nil",
		},
		{
			name: "caller_canceled",
			err:  context.Canceled,
		},
		{
			name: "caller_deadline",
			err:  context.DeadlineExceeded,
		},
		{
			name: "bad_connection",
			err:  driver.ErrBadConn,
			want: true,
		},
		{
			name: "invalid_connection",
			err:  gomysql.ErrInvalidConn,
			want: true,
		},
		{
			name: "connection_done",
			err:  sql.ErrConnDone,
		},
		{
			name: "transaction_done",
			err:  sql.ErrTxDone,
		},
		{
			name: "network_timeout",
			err: &net.OpError{
				Op:  "read",
				Net: "tcp",
				Err: os.ErrDeadlineExceeded,
			},
			want: true,
		},
		{
			name: "server_shutdown",
			err:  &gomysql.MySQLError{Number: mysqlErrServerShutdown},
			want: true,
		},
		{
			name: "primary_read_only",
			role: RolePrimary,
			err: &gomysql.MySQLError{
				Number:  mysqlErrOptionPreventsStatement,
				Message: "The MySQL server is running with the --read-only option so it cannot execute this statement",
			},
			want: true,
		},
		{
			name: "replica_read_only",
			role: RoleReplica,
			err: &gomysql.MySQLError{
				Number:  mysqlErrOptionPreventsStatement,
				Message: "The MySQL server is running with the --read-only option so it cannot execute this statement",
			},
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				require.Equal(t, tc.want, IsMySQLInfraError(tc.role, tc.err))
				if tc.err != nil {
					require.Equal(t, tc.want, IsMySQLInfraError(tc.role, fmt.Errorf("query: %w", tc.err)))
				}
			},
		)
	}
}

func TestMySQLDialectorPreservesVersionCompatibility(t *testing.T) {
	pool, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pool.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	mock.
		ExpectQuery("^SELECT VERSION\\(\\)$").
		WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("5.7.44"))
	mock.ExpectClose()

	dialector := newMySQLDialector(
		t.Context(),
		pool,
	).(*mysqlDialector)

	db, err := gorm.Open(
		dialector,
		&gorm.Config{DisableAutomaticPing: true},
	)

	require.NoError(t, err)
	require.True(t, dialector.DontSupportRenameColumn)
	require.True(t, dialector.DontSupportForShareClause)
	require.Same(t, pool, dialector.Conn)
	require.Same(t, pool, db.ConnPool)
}

func TestBuildMySQLDSNTimeouts(t *testing.T) {
	dsn := buildMySqlDSN(
		"user",
		"password",
		"localhost:3306",
		"test",
		time.Second,
		2*time.Second,
		3*time.Second,
		false,
	)
	cfg, err := gomysql.ParseDSN(dsn)
	require.NoError(t, err)
	require.Equal(t, time.Second, cfg.Timeout)
	require.Equal(t, 2*time.Second, cfg.ReadTimeout)
	require.Equal(t, 3*time.Second, cfg.WriteTimeout)
}
