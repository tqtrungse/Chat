/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package sql

import (
	"context"
	"database/sql"

	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// mysqlDialector gives the version detection of GORM the startup context of
// the caller. It keeps the compatibility flags of the driver for each version.
type mysqlDialector struct {
	*gormmysql.Dialector
	ctx context.Context
}

func newMySQLDialector(ctx context.Context, pool *sql.DB) gorm.Dialector {
	return &mysqlDialector{
		Dialector: &gormmysql.Dialector{
			Config: &gormmysql.Config{Conn: pool},
		},
		ctx: ctx,
	}
}

// Initialize gives the version detection of GORM the context of the caller.
//
// GORM reads the version of the MySQL server to make the correct SQL. For that
// read, GORM uses context.Background() and ignores the context of the caller.
// Therefore, a cancel or a deadline of the caller has no effect.
//
// This method replaces that step with the context of the caller.
func (d *mysqlDialector) Initialize(db *gorm.DB) error {
	connPool := d.Conn
	d.Conn = initializationPool{
		ConnPool: connPool,
		ctx:      d.ctx,
	}
	defer func() {
		// Runtime queries must use their own contexts and the original pool.
		d.Conn = connPool
		db.ConnPool = connPool
	}()
	return d.Dialector.Initialize(db)
}
