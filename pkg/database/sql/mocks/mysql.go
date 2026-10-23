/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package mocks

import (
	"context"
	"database/sql"
	"testing"

	database "xxx/pkg/database/sql"

	"github.com/DATA-DOG/go-sqlmock"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

type mysql struct {
	sqlDB *sql.DB
	db    *gorm.DB
}

func NewMysql(t *testing.T) (database.DB, sqlmock.Sqlmock) {
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	return &mysql{sqlDB: sqlDB}, mock
}

func (m *mysql) Open(_ context.Context, cfg database.Config) (func() error, error) {
	gormDB, err := gorm.Open(
		gormmysql.New(
			gormmysql.Config{
				Conn: m.sqlDB,
				// The driver otherwise runs SELECT VERSION() during
				// Initialize to choose its version-specific compatibility
				// flags. No server stands behind sqlmock, so every test would
				// have to expect that query. The flags keep the default of a
				// server that supports each feature.
				SkipInitializeWithVersion: true,
			},
		),
		&gorm.Config{
			// The mock keeps the value of the production handle. A different
			// value makes GORM wrap each write here in a transaction, but not
			// in production. Each test of a write then expects a BEGIN and a
			// COMMIT that production never sends.
			SkipDefaultTransaction: true,
			// Without this field GORM takes logger.Default, which writes
			// plain text to the os.Stdout of the test.
			Logger: gormlogger.Discard,
		},
	)
	if err != nil {
		return nil, err
	}

	sqlDB, err := gormDB.DB()
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxIdleConns(cfg.Pool.MaxIdleConns)
	sqlDB.SetMaxOpenConns(cfg.Pool.MaxOpenConns)
	sqlDB.SetConnMaxIdleTime(cfg.Pool.ConnMaxIdleTime)
	sqlDB.SetConnMaxLifetime(cfg.Pool.ConnMaxLifetime)

	m.db = gormDB
	return m.Close, nil
}

func (m *mysql) Close() error {
	sqlDB, err := m.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

func (m *mysql) Transaction(
	ctx context.Context,
	source database.TxSource,
	fn func(ctx context.Context) error,
) error {
	db := m.db

	switch source {
	case database.TxSourcePrimary,
		database.TxSourceReplicas:
		return db.
			WithContext(ctx).
			Transaction(func(tx *gorm.DB) error {
				return fn(database.AttachDB(ctx, tx))
			})

	default:
		return database.ErrInvalidTxSource
	}
}

func (m *mysql) WithDB(
	ctx context.Context,
	source database.TxSource,
	fn func(ctx context.Context) error,
) error {
	db := m.db

	switch source {
	case database.TxSourcePrimary,
		database.TxSourceReplicas:
		return fn(database.AttachDB(ctx, db.WithContext(ctx)))

	default:
		return database.ErrInvalidTxSource
	}
}
