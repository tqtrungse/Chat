/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package sql

import "context"

type ctxKey int

const dbKey ctxKey = 1

func AttachDB(ctx context.Context, db any) context.Context {
	return context.WithValue(ctx, dbKey, db)
}

func ExtractDB(dbCtx context.Context) (any, error) {
	if val := dbCtx.Value(dbKey); val != nil {
		return val, nil
	}
	return nil, ErrExtractDB
}
