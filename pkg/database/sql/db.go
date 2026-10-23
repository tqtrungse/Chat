/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
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

type CommandFunc func(context.Context) error

type DB interface {
	// Open initializes the DB and returns its cleanup function. Call Open only
	// once per instance during initialization. After a successful Open, call
	// closeFn only once during cleanup, after all DB operations have finished.
	// Open and closeFn are not thread-safe; do not call them concurrently with
	// each other or with DB operations. If Open fails, it cleans up any pools
	// it created and returns a nil closeFn.
	Open(ctx context.Context, cfg Config) (closeFn func() error, err error)

	// Transaction runs fn in a transaction on the selected source. Repository
	// operations must use the handle attached to the callback context via
	// ExtractDB. Do not call Transaction or WithDB inside fn: these wrappers
	// start from the root DB and do not reuse a transaction already in ctx.
	Transaction(
		ctx context.Context,
		source TxSource,
		fn func(ctx context.Context) error,
	) error

	// WithDB provides a DB handle in the callback context for an independent,
	// single-step read query without starting a transaction. Repository code
	// obtains the handle via ExtractDB. Do not call WithDB inside a Transaction
	// callback; use the transaction handle already attached to that context.
	WithDB(
		ctx context.Context,
		source TxSource,
		fn func(ctx context.Context) error,
	) error
}
