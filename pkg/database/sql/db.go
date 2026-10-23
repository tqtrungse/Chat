/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package sql

import (
	"context"
	"database/sql"
)

type openPoolFunc func(context.Context, string, string, PoolConfig) (*sql.DB, error)

type TxSource uint8

const (
	TxSourcePrimary  TxSource = 1
	TxSourceReplicas TxSource = 2
)

// Role identifies the kind of connection that gave the error.
//
// A 1290 (read-only) error has a different meaning for each pool. For the
// details, see IsMySQLInfraError.
type Role int

const (
	RoleUnspecified Role = 0

	// RolePrimary is the single read-write connection or pool.
	RolePrimary Role = 1

	// RoleReplica is any read-only replica connection or pool. Use this value
	// for every replica. IsMySQLInfraError does not need the name of one
	// replica.
	RoleReplica Role = 2
)

type CommandFunc func(context.Context) error

type DB interface {
	// Open initializes the DB. It returns closeFn, the function that cleans up.
	// Open applies defaults to a copy of cfg.
	//
	// Call Open one time for each instance, during initialization.
	// After a successful Open, call closeFn one time during cleanup.
	// Call closeFn after all DB operations are complete.
	//
	// Open and closeFn are not thread-safe. Do not call them at the same time
	// as each other. Do not call them at the same time as DB operations.
	//
	// If Open fails, it cleans up each pool that it created.
	// It then returns a nil closeFn.
	Open(ctx context.Context, cfg Config) (closeFn func() error, err error)

	// Transaction runs fn in a transaction on the selected source.
	//
	// The repository operations in fn must use the handle that is attached to
	// the callback context. Get the handle with ExtractDB.
	//
	// Do not call Transaction or WithDB in fn. These wrappers start from the
	// root DB. They do not reuse a transaction that is already in ctx.
	Transaction(
		ctx context.Context,
		source TxSource,
		fn func(ctx context.Context) error,
	) error

	// WithDB puts a DB handle in the callback context.
	// Use it for one independent read query that does not need a transaction.
	// WithDB does not start a transaction.
	//
	// Get the handle in repository code with ExtractDB.
	//
	// Do not call WithDB in a Transaction callback. Use the transaction handle
	// that is already attached to that context.
	WithDB(
		ctx context.Context,
		source TxSource,
		fn func(ctx context.Context) error,
	) error
}
