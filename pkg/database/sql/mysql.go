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

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/plugin/dbresolver"
)

type mysql struct {
	db       *gorm.DB
	primary  *sql.DB
	replicas []*sql.DB
}

func NewMySql() DB {
	return new(mysql)
}

func (m *mysql) Open(ctx context.Context, cfg Config) error {
	if m.db != nil {
		return ErrReopenDB
	}

	primary, err := openPool(
		ctx,
		"mysql",
		buildMySqlDSN(
			cfg.User,
			cfg.Passwd,
			cfg.PrimaryAddr,
			cfg.DBName,
			cfg.MultiStatements,
		),
		cfg.Pool,
	)
	if err != nil {
		return fmt.Errorf("open source pool: %w", err)
	}

	primaryDialector := gormmysql.New(gormmysql.Config{
		Conn: primary,
	})

	db, err := gorm.Open(
		primaryDialector,
		&gorm.Config{
			SkipDefaultTransaction: true,
		},
	)
	if err != nil {
		return fmt.Errorf("gorm.Open: %w", errors.Join(err, primary.Close()))
	}

	resolverCfg := dbresolver.Config{
		Sources: []gorm.Dialector{
			primaryDialector,
		},
		Policy: dbresolver.RandomPolicy{},
	}

	replicas := make([]*sql.DB, 0, len(cfg.ReplicaAddrs))
	for i, replicaAddr := range cfg.ReplicaAddrs {
		replica, repErr := openPool(
			ctx,
			"mysql",
			buildMySqlDSN(
				cfg.User,
				cfg.Passwd,
				replicaAddr,
				cfg.DBName,
				cfg.MultiStatements,
			),
			cfg.Pool,
		)
		if repErr != nil {
			repErr = errors.Join(repErr, primary.Close())

			for _, r := range replicas {
				repErr = errors.Join(repErr, r.Close())
			}

			return fmt.Errorf("open replica pool %d (%s): %w", i, replicaAddr, repErr)
		}

		replicas = append(replicas, replica)
		resolverCfg.Replicas = append(
			resolverCfg.Replicas,
			gormmysql.New(gormmysql.Config{
				Conn: replica,
			}),
		)
	}

	if err = db.Use(dbresolver.Register(resolverCfg)); err != nil {
		err = errors.Join(err, primary.Close())

		for _, replica := range replicas {
			err = errors.Join(err, replica.Close())
		}

		return fmt.Errorf("register dbresolver: %w", err)
	}

	m.db = db.Set("gorm:table_options", "ENGINE=InnoDB")
	m.primary = primary
	m.replicas = replicas

	return nil
}

func (m *mysql) Close() error {
	var err error

	if m.primary != nil {
		err = errors.Join(err, m.primary.Close())
	}
	for _, replica := range m.replicas {
		err = errors.Join(err, replica.Close())
	}

	m.db = nil
	m.primary = nil
	m.replicas = nil

	return err
}

func (m *mysql) Transaction(
	ctx context.Context,
	source TxSource,
	fn func(ctx context.Context) error,
) error {
	db := m.db

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
