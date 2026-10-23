/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package sql

import (
	"context"
	stdsql "database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestMySQL_OpenWithoutReplicas verifies that an empty replica configuration
// opens exactly one primary SQL pool. The initial GORM handle, dbresolver's
// source route, and its read fallback must all reuse that same *sql.DB;
// initializing dbresolver must not create a second pool for the primary.
func TestMySQL_OpenWithoutReplicas(t *testing.T) {
	for _, tc := range []struct {
		name     string
		replicas []string
	}{
		{name: "nil"},
		{name: "empty", replicas: []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			primary, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
			require.NoError(t, err)

			db := NewMySql().(*mysql)
			var closeDB func() error
			t.Cleanup(func() {
				if closeDB != nil {
					require.NoError(t, closeDB())
				} else {
					require.NoError(t, primary.Close())
				}
				require.NoError(t, mock.ExpectationsWereMet())
			})

			openedPools := 0
			db.openPool = func(context.Context, string, string, PoolConfig) (*stdsql.DB, error) {
				openedPools++
				return primary, nil
			}

			// Both initializations detect the version using the startup context.
			// Neither should ping: openPool already performs PingContext.
			for range 2 {
				mock.
					ExpectQuery("^SELECT VERSION\\(\\)$").
					WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.36"))
			}
			for _, value := range []int{42, 43} {
				mock.
					ExpectQuery("^SELECT \\?$").
					WithArgs(value).
					WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(value))
			}
			mock.ExpectClose()

			startupCtx, cancelStartup := context.WithCancel(t.Context())
			defer cancelStartup()
			closeDB, err = db.Open(
				startupCtx,
				Config{
					User:         "root",
					DBName:       "root",
					PrimaryAddr:  "primary:3306",
					ReplicaAddrs: tc.replicas,
				},
			)
			require.NoError(t, err)
			require.NotNil(t, closeDB)
			cancelStartup() // Runtime queries must not retain the startup context.
			require.Equal(t, 1, openedPools, "Open must create only the primary pool")
			require.Same(t, primary, db.primary)
			require.Empty(t, db.replicas)
			gormDB := db.db.Load()
			require.NotNil(t, gormDB)
			require.Contains(t, gormDB.Plugins, "gorm:db_resolver")

			// Pointer identity checks the underlying SQL pool, rather than
			// counting GORM handles or initialization queries.
			gormPool, err := gormDB.DB()
			require.NoError(t, err)
			require.Same(t, primary, gormPool, "the initial GORM handle must reuse the primary pool")

			for _, route := range []struct {
				name   string
				source TxSource
				value  int
			}{
				{name: "primary", source: TxSourcePrimary, value: 42},
				{name: "read_falls_back_to_primary", source: TxSourceReplicas, value: 43},
			} {
				t.Run(route.name, func(t *testing.T) {
					// With zero replicas, dbresolver's write and read routes must
					// select the existing primary pool, including during a query.
					var got int
					err := db.WithDB(t.Context(), route.source, func(ctx context.Context) error {
						conn, err := ExtractDB(ctx)
						if err != nil {
							return err
						}
						gormDB := conn.(*gorm.DB)
						selectedPool, err := gormDB.DB()
						require.NoError(t, err)
						require.Same(t, primary, selectedPool, "dbresolver must reuse the primary SQL pool")

						query := gormDB.Raw("SELECT ?", route.value).Scan(&got)
						require.Same(t, primary, query.Statement.ConnPool, "the query must execute on the original primary pool")
						return query.Error
					})
					require.NoError(t, err)
					require.Equal(t, route.value, got)
				})
			}
		})
	}
}

func TestMySQL_OpenVersionDetectionDeadline(t *testing.T) {
	for _, phase := range []string{"primary", "resolver_source", "resolver_replica"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			primary, primaryMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
			require.NoError(t, err)
			replica, replicaMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = primary.Close()
				_ = replica.Close()
				require.NoError(t, primaryMock.ExpectationsWereMet())
				require.NoError(t, replicaMock.ExpectationsWereMet())
			})

			expectVersion := func(mock sqlmock.Sqlmock, delayed bool) {
				q := mock.
					ExpectQuery("^SELECT VERSION\\(\\)$").
					WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.36"))
				if delayed {
					q.WillDelayFor(time.Second)
				}
			}
			expectVersion(primaryMock, phase == "primary")
			if phase != "primary" {
				expectVersion(primaryMock, phase == "resolver_source")
			}
			if phase == "resolver_replica" {
				expectVersion(replicaMock, true)
			}
			primaryMock.ExpectClose()
			replicaMock.ExpectClose()

			db := NewMySql().(*mysql)
			opened := 0
			db.openPool = func(context.Context, string, string, PoolConfig) (*stdsql.DB, error) {
				opened++
				if opened == 1 {
					return primary, nil
				}
				return replica, nil
			}
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
			defer cancel()
			closeDB, err := db.Open(ctx, Config{ReplicaAddrs: []string{"replica:3306"}})
			// sqlmock returns ErrCancelled only when the query's context expires.
			// A post-initialization ctx.Err check alone cannot satisfy this test.
			require.ErrorIs(t, err, sqlmock.ErrCancelled)
			require.Nil(t, closeDB)
			require.Nil(t, db.db.Load())
			require.Nil(t, db.primary)
			require.Empty(t, db.replicas)
		})
	}
}

func TestMySQL_CloseFnClosesAllPools(t *testing.T) {
	for _, closeFails := range []bool{false, true} {
		name := "success"
		if closeFails {
			name = "close_errors"
		}

		t.Run(
			name,
			func(t *testing.T) {
				t.Parallel()

				primary, primaryMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
				require.NoError(t, err)

				replica, replicaMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
				require.NoError(t, err)
				t.Cleanup(func() {
					_ = primary.Close()
					_ = replica.Close()
					require.NoError(t, primaryMock.ExpectationsWereMet())
					require.NoError(t, replicaMock.ExpectationsWereMet())
				})

				for range 2 {
					primaryMock.
						ExpectQuery("^SELECT VERSION\\(\\)$").
						WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.36"))
				}
				replicaMock.
					ExpectQuery("^SELECT VERSION\\(\\)$").
					WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.36"))

				opened := 0
				db := NewMySql().(*mysql)
				db.openPool = func(context.Context, string, string, PoolConfig) (*stdsql.DB, error) {
					opened++
					if opened == 1 {
						return primary, nil
					}
					return replica, nil
				}
				closeDB, err := db.Open(
					t.Context(),
					Config{ReplicaAddrs: []string{"replica:3306"}},
				)
				require.NoError(t, err)
				require.NotNil(t, closeDB)
				require.Equal(t, 2, opened)

				primaryErr := errors.New("primary close failed")
				replicaErr := errors.New("replica close failed")
				primaryClose := primaryMock.ExpectClose()
				replicaClose := replicaMock.ExpectClose()
				if closeFails {
					primaryClose.WillReturnError(primaryErr)
					replicaClose.WillReturnError(replicaErr)
				}
				err = closeDB()
				if closeFails {
					require.ErrorIs(t, err, primaryErr)
					require.ErrorIs(t, err, replicaErr)
				} else {
					require.NoError(t, err)
				}
				require.Nil(t, db.db.Load())
				require.Nil(t, db.primary)
				require.Empty(t, db.replicas)

				called := false
				fn := func(context.Context) error {
					called = true
					return nil
				}
				require.ErrorIs(t, db.WithDB(t.Context(), TxSourcePrimary, fn), ErrDBClosed)
				require.ErrorIs(t, db.Transaction(t.Context(), TxSourcePrimary, fn), ErrDBClosed)
				require.False(t, called, "cleanup must prevent new operations")
			},
		)
	}
}

func TestMySQL_OpenPoolFailure(t *testing.T) {
	db := NewMySql().(*mysql)
	wantErr := errors.New("pool unavailable")
	db.openPool = func(context.Context, string, string, PoolConfig) (*stdsql.DB, error) {
		return nil, wantErr
	}
	closeDB, err := db.Open(t.Context(), Config{})
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, closeDB)
	require.Nil(t, db.db.Load())
}

func TestMySQL_WithDBRoutesQueries(t *testing.T) {
	for _, route := range []struct {
		name   string
		source TxSource
	}{
		{
			name:   "primary",
			source: TxSourcePrimary,
		},
		{
			name:   "replica",
			source: TxSourceReplicas,
		},
	} {
		for _, raw := range []bool{false, true} {
			queryKind := "builder"
			if raw {
				queryKind = "raw"
			}

			t.Run(
				route.name+"/"+queryKind,
				func(t *testing.T) {
					t.Parallel()

					f := newMySQLRoutingFixture(t, true)
					pool, mock := f.primary, f.primaryMock
					if route.source == TxSourceReplicas {
						pool = f.replica
						mock = f.replicaMock
					}
					// Only the selected pool accepts this query. The other pool has
					// no query expectations, so incorrect routing fails the test.
					mock.
						ExpectQuery("^SELECT .*value.* FROM .*routing_probe.* WHERE id = \\?$").
						WithArgs(42).
						WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(101))

					var got int
					err := f.db.WithDB(
						t.Context(),
						route.source,
						func(ctx context.Context) error {
							handle, err := ExtractDB(ctx)
							if err != nil {
								return err
							}

							var (
								db    = handle.(*gorm.DB)
								query *gorm.DB
							)

							if raw {
								query = db.
									Raw("SELECT value FROM routing_probe WHERE id = ?", 42).
									Scan(&got)
							} else {
								query = db.
									Table("routing_probe").
									Select("value").
									Where("id = ?", 42).
									Scan(&got)
							}
							require.Same(t, pool, query.Statement.ConnPool)
							return query.Error
						},
					)
					require.NoError(t, err)
					require.Equal(t, 101, got)
				},
			)
		}
	}
}

func TestMySQL_TransactionRoutesQueriesAndCompletion(t *testing.T) {
	for _, route := range []struct {
		name       string
		source     TxSource
		hasReplica bool
	}{
		{
			name:       "primary",
			source:     TxSourcePrimary,
			hasReplica: true,
		},
		{
			name:       "replica",
			source:     TxSourceReplicas,
			hasReplica: true,
		},
		{
			name:   "replica_falls_back_to_primary",
			source: TxSourceReplicas,
		},
	} {
		for _, outcome := range []string{
			"commit",
			"callback_error",
			"query_error",
		} {
			t.Run(
				route.name+"/"+outcome,
				func(t *testing.T) {
					t.Parallel()

					var (
						f    = newMySQLRoutingFixture(t, route.hasReplica)
						mock = f.primaryMock
					)
					if route.source == TxSourceReplicas && route.hasReplica {
						mock = f.replicaMock
					}

					wantErr := errors.New("transaction failed")
					mock.ExpectBegin()
					for _, id := range []int{41, 42} {
						query := mock.
							ExpectQuery("^SELECT value FROM routing_probe WHERE id = \\?$").
							WithArgs(id)

						if outcome == "query_error" && id == 42 {
							query.WillReturnError(wantErr)
						} else {
							query.WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(id + 100))
						}
					}
					if outcome == "commit" {
						mock.ExpectCommit()
					} else {
						mock.ExpectRollback()
					}

					err := f.db.Transaction(
						t.Context(),
						route.source,
						func(ctx context.Context) error {
							handle, err := ExtractDB(ctx)
							if err != nil {
								return err
							}

							tx := handle.(*gorm.DB)
							conn, ok := tx.Statement.ConnPool.(*stdsql.Tx)
							require.True(t, ok, "callback must receive a SQL transaction")
							for _, id := range []int{41, 42} {
								var got int
								query := tx.
									Raw("SELECT value FROM routing_probe WHERE id = ?", id).
									Scan(&got)

								require.Same(
									t,
									conn,
									query.Statement.ConnPool,
									"queries must stay in the same transaction",
								)

								if query.Error != nil {
									return query.Error
								}
								require.Equal(t, id+100, got)
							}
							if outcome == "callback_error" {
								return wantErr
							}
							return nil
						},
					)
					if outcome == "commit" {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, wantErr)
					}
				},
			)
		}
	}
}

type mysqlRoutingFixture struct {
	db          DB
	primary     *stdsql.DB
	primaryMock sqlmock.Sqlmock
	replica     *stdsql.DB
	replicaMock sqlmock.Sqlmock
}

func newMySQLRoutingFixture(t *testing.T, withReplica bool) *mysqlRoutingFixture {
	t.Helper()

	var (
		f   = &mysqlRoutingFixture{}
		err error
	)

	f.primary, f.primaryMock, err = sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)

	var closeDB func() error
	t.Cleanup(func() {
		if closeDB != nil {
			f.primaryMock.ExpectClose()
			if f.replica != nil {
				f.replicaMock.ExpectClose()
			}
			require.NoError(t, closeDB())
		} else {
			_ = f.primary.Close()
			if f.replica != nil {
				_ = f.replica.Close()
			}
		}

		require.NoError(t, f.primaryMock.ExpectationsWereMet())
		if f.replica != nil {
			require.NoError(t, f.replicaMock.ExpectationsWereMet())
		}
	})

	for range 2 {
		f.primaryMock.
			ExpectQuery("^SELECT VERSION\\(\\)$").
			WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.36"))
	}

	cfg := Config{PrimaryAddr: "primary:3306"}
	if withReplica {
		f.replica, f.replicaMock, err = sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)

		cfg.ReplicaAddrs = []string{"replica:3306"}
		f.replicaMock.
			ExpectQuery("^SELECT VERSION\\(\\)$").
			WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.36"))
	}

	var (
		opened = 0
		db     = NewMySql().(*mysql)
	)
	db.openPool = func(context.Context, string, string, PoolConfig) (*stdsql.DB, error) {
		opened++
		if opened == 1 {
			return f.primary, nil
		}
		return f.replica, nil
	}
	closeDB, err = db.Open(t.Context(), cfg)
	require.NoError(t, err)
	require.NotNil(t, closeDB)
	f.db = db
	return f
}
