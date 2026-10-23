/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

type ctxKey int

const dbKey ctxKey = 1

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
	return context.WithValue(ctx, dbKey, db)
}

func ExtractDB(dbCtx context.Context) (any, error) {
	if val := dbCtx.Value(dbKey); val != nil {
		return val, nil
	}
	return nil, ErrExtractDB
}

type initializationPool struct {
	gorm.ConnPool
	ctx context.Context
}

// QueryRowContext
//
// Implement interface of GORM.
func (p initializationPool) QueryRowContext(_ context.Context, query string, args ...any) *sql.Row {
	return p.ConnPool.QueryRowContext(p.ctx, query, args...)
}

// openPool opens and tunes a *sql.DB.
func openPool(
	ctx context.Context,
	driverName string,
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

	err = sqlDB.PingContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("ping database: %w", errors.Join(err, sqlDB.Close()))
	}

	return sqlDB, nil
}
