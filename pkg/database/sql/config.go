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
	"time"

	"xxx/pkg"
)

type Config struct {
	User            string
	Passwd          string
	DBName          string
	PrimaryAddr     string
	ReplicaAddrs    []string
	MultiStatements bool
	Pool            PoolConfig
}

func (c *Config) Load(loader pkg.ConfigLoader) {
	if loader == nil {
		return
	}
	c.User = loader.GetString("SQL_DATABASE_USER")
	c.Passwd = loader.GetString("SQL_DATABASE_PASSWORD")
	c.DBName = loader.GetString("SQL_DATABASE_NAME")
	c.PrimaryAddr = loader.GetString("SQL_DATABASE_PRIMARY_ADDRESS")
	c.ReplicaAddrs = loader.GetStringSlice("SQL_DATABASE_REPLICA_ADDRESSES")
	c.MultiStatements = loader.GetBool("SQL_DATABASE_MULTI_STATEMENTS")
	c.Pool.Load(loader)
}

type PoolConfig struct {
	// MaxOpenConns sets the maximum number of open connections to the database.
	//
	// If MaxIdleConns is greater than 0 and the new MaxOpenConns is less than MaxIdleConns,
	// then MaxIdleConns will be reduced to match the new MaxOpenConns limit.
	// If n <= 0, then there is no limit on the number of open connections.
	//
	// Default: 0 (unlimited)
	MaxOpenConns int

	// MaxIdleConns sets the maximum number of connections in the idle connection pool.
	//
	// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns, then the new
	// MaxIdleConns will be reduced to match the MaxOpenConns limit.
	//
	// If n <= 0, no idle connections are retained.
	//
	// Default: 4
	MaxIdleConns int

	// ConnMaxLifetime sets the maximum amount of time a connection may be reused.
	//
	// Expired connections may be closed lazily before reuse.
	//
	// If d <= 0, connections are not closed due to a connection's age.
	ConnMaxLifetime time.Duration

	// ConnMaxIdleTime sets the maximum amount of time a connection may be idle.
	//
	// Expired connections may be closed lazily before reuse.
	//
	// If d <= 0, connections are not closed due to a connection's idle time.
	ConnMaxIdleTime time.Duration
}

func (c *PoolConfig) Load(loader pkg.ConfigLoader) {
	if loader == nil {
		return
	}
	c.MaxOpenConns = loader.GetInt("SQL_DATABASE_MAX_OPEN_CONNS")
	c.MaxIdleConns = loader.GetInt("SQL_DATABASE_MAX_IDLE_CONNS")
	c.ConnMaxLifetime = loader.GetDuration("SQL_DATABASE_CONN_MAX_LIFETIME")
	c.ConnMaxIdleTime = loader.GetDuration("SQL_DATABASE_CONN_MAX_IDLE_TIME")
}
