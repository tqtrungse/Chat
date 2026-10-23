/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package sql

import (
	"strings"
	"time"

	"xxx/pkg/config"
)

const (
	defaultDialTimeout  = 3 * time.Second
	defaultReadTimeout  = 10 * time.Second
	defaultWriteTimeout = 5 * time.Second
)

type Config struct {
	User         string
	Passwd       string
	DBName       string
	PrimaryAddr  string
	ReplicaAddrs []string
	// DialTimeout limits connection establishment. Load defaults to 3s when
	// the key is absent; an explicitly configured 0 disables the timeout.
	DialTimeout time.Duration
	// ReadTimeout limits network reads. Load defaults to 10s when the key is
	// absent; an explicitly configured 0 disables the timeout.
	ReadTimeout time.Duration
	// WriteTimeout limits network writes. Load defaults to 5s when the key is
	// absent; an explicitly configured 0 disables the timeout.
	WriteTimeout time.Duration
	Pool         PoolConfig
}

func (c *Config) Load(loader config.Loader) {
	c.DialTimeout = defaultDialTimeout
	c.ReadTimeout = defaultReadTimeout
	c.WriteTimeout = defaultWriteTimeout

	if loader == nil {
		return
	}
	if loader.IsSet("SQL_DATABASE_DIAL_TIMEOUT") {
		c.DialTimeout = loader.GetDuration("SQL_DATABASE_DIAL_TIMEOUT")
	}
	if loader.IsSet("SQL_DATABASE_READ_TIMEOUT") {
		c.ReadTimeout = loader.GetDuration("SQL_DATABASE_READ_TIMEOUT")
	}
	if loader.IsSet("SQL_DATABASE_WRITE_TIMEOUT") {
		c.WriteTimeout = loader.GetDuration("SQL_DATABASE_WRITE_TIMEOUT")
	}

	c.User = strings.TrimSpace(loader.GetString("SQL_DATABASE_USER"))
	c.Passwd = loader.GetString("SQL_DATABASE_PASSWORD")
	c.DBName = strings.TrimSpace(loader.GetString("SQL_DATABASE_NAME"))
	c.PrimaryAddr = strings.TrimSpace(loader.GetString("SQL_DATABASE_PRIMARY_ADDRESS"))
	c.ReplicaAddrs = config.SplitList(loader.GetString("SQL_DATABASE_REPLICA_ADDRESSES"))
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

func (c *PoolConfig) Load(loader config.Loader) {
	if loader == nil {
		return
	}
	c.MaxOpenConns = loader.GetInt("SQL_DATABASE_MAX_OPEN_CONNS")
	c.MaxIdleConns = loader.GetInt("SQL_DATABASE_MAX_IDLE_CONNS")
	c.ConnMaxLifetime = loader.GetDuration("SQL_DATABASE_CONN_MAX_LIFETIME")
	c.ConnMaxIdleTime = loader.GetDuration("SQL_DATABASE_CONN_MAX_IDLE_TIME")
}
