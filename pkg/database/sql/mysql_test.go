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
	stdsql "database/sql"
	"testing"

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
			t.Cleanup(func() {
				if db.primary != nil {
					require.NoError(t, db.Close())
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

			// GORM initializes once in Open and again when dbresolver registers
			// its source dialector. Each initialization checks the version and
			// pings, but these two checks do not imply two separate SQL pools.
			for range 2 {
				mock.ExpectQuery("^SELECT VERSION\\(\\)$").
					WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("8.0.36"))
				mock.ExpectPing()
			}
			for _, value := range []int{42, 43} {
				mock.ExpectQuery("^SELECT \\?$").
					WithArgs(value).
					WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow(value))
			}
			mock.ExpectClose()

			err = db.Open(
				t.Context(),
				Config{
					User:         "root",
					DBName:       "root",
					PrimaryAddr:  "primary:3306",
					ReplicaAddrs: tc.replicas,
				},
			)
			require.NoError(t, err)
			require.Equal(t, 1, openedPools, "Open must create only the primary pool")
			require.Same(t, primary, db.primary)
			require.Empty(t, db.replicas)
			require.Contains(t, db.db.Plugins, "gorm:db_resolver")

			// Pointer identity checks the underlying SQL pool, rather than
			// counting GORM handles or initialization queries.
			gormPool, err := db.db.DB()
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
