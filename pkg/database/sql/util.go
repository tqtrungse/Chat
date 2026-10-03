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
	"errors"
	"fmt"
)

// Role identifies which kind of connection produced the error. A 1290
// (read-only) error means something different depending on which pool hit
// it — see IsInfraError.
type Role int

const (
	// RolePrimary is the single read-write connection/pool.
	RolePrimary Role = 1

	// RoleReplica is any read-only replica connection/pool. Use this value
	// for every replica — IsInfraError doesn't need to know which one.
	RoleReplica Role = 2
)

func AttachDB(ctx context.Context, db any) context.Context {
	return context.WithValue(ctx, "db", db)
}

func ExtractDB(dbCtx context.Context) (any, error) {
	if val := dbCtx.Value("db"); val != nil {
		return val, nil
	}
	return nil, ErrExtractDB
}

// openPool opens and tunes a *sql.DB.
func openPool(
	ctx context.Context,
	driverName,
	dsn string,
	cfg PoolConfig,
) (*sql.DB, error) {
	sqlDB, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}

	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	if err = sqlDB.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping database: %w", errors.Join(err, sqlDB.Close()))
	}

	return sqlDB, nil
}
