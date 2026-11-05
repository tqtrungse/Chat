/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

type initializationPool struct {
	gorm.ConnPool
	ctx context.Context
}

// QueryRowContext implements the ConnPool interface of GORM.
//
// It ignores the context of the caller and uses the startup context instead.
// Therefore the version detection of GORM keeps the deadline of that startup
// context.
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
