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

package mock

import (
	"context"
	"database/sql"
	"testing"

	database "xxx/pkg/database/sql"

	"github.com/DATA-DOG/go-sqlmock"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
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
	return &mysql{
		sqlDB: sqlDB,
	}, mock
}

func (m *mysql) Open(_ context.Context, cfg database.Config) error {
	gormDB, err := gorm.Open(
		gormmysql.New(gormmysql.Config{
			Conn:                      m.sqlDB,
			SkipInitializeWithVersion: true,
		}),
		&gorm.Config{
			SkipDefaultTransaction: true,
		},
	)
	if err != nil {
		return err
	}

	sqlDB, err := gormDB.DB()
	if err != nil {
		return err
	}
	sqlDB.SetMaxIdleConns(cfg.Pool.MaxIdleConns)
	sqlDB.SetMaxOpenConns(cfg.Pool.MaxOpenConns)
	sqlDB.SetConnMaxIdleTime(cfg.Pool.ConnMaxIdleTime)
	sqlDB.SetConnMaxLifetime(cfg.Pool.ConnMaxLifetime)

	m.db = gormDB
	return nil
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
	case database.TxSourcePrimary, database.TxSourceReplicas:
	default:
		return database.ErrInvalidTxSource
	}

	return db.
		WithContext(ctx).
		Transaction(func(tx *gorm.DB) error {
			return fn(database.AttachDB(ctx, tx))
		})
}
