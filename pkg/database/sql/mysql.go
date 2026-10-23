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
	"sync/atomic"

	"gorm.io/gorm"
	"gorm.io/plugin/dbresolver"
)

type mysql struct {
	db       atomic.Pointer[gorm.DB]
	primary  *sql.DB
	replicas []*sql.DB
	openPool openPoolFunc
	isClose  atomic.Bool
}

func NewMySql() DB {
	return &mysql{openPool: openPool}
}

func (m *mysql) Open(ctx context.Context, cfg Config) (func() error, error) {
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
			cfg.Passwd,
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
			SkipDefaultTransaction: true,
			// openPool already verifies each connection using PingContext.
			DisableAutomaticPing: true,
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
				cfg.Passwd,
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
