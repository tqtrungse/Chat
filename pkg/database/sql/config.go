/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package sql

import (
	"errors"
	"strings"
	"time"

	"xxx/pkg/config"
)

const (
	defaultDialTimeout   = 3 * time.Second
	defaultReadTimeout   = 10 * time.Second
	defaultWriteTimeout  = 5 * time.Second
	defaultSlowThreshold = 200 * time.Millisecond
)

// Config contains the connection, timeout, and log settings of the database.
//
// For DialTimeout, ReadTimeout, WriteTimeout, and SlowThreshold, the value
// zero gets the default. A negative value disables the limit. The MySQL driver
// and the statement logger use no limit for a value that is not positive.
type Config struct {
	// User is the username for authentication.
	//
	// Required.
	User string

	// Password is password of user for authentication.
	Password string

	// DB name is database name.
	//
	// Required.
	DBName string

	// PrimaryAddr is the network address of the primary database server.
	//
	// Required.
	PrimaryAddr string

	// ReplicaAddrs is the list of network addresses of the replica database
	// servers.
	ReplicaAddrs []string

	// DialTimeout is the maximum time to open a TCP connection to a server.
	//
	// Default: 3s
	DialTimeout time.Duration

	// ReadTimeout is the maximum time for one network read from a server.
	//
	// Default: 10s
	ReadTimeout time.Duration

	// WriteTimeout is the maximum time for one network write to a server.
	//
	// Default: 5s
	WriteTimeout time.Duration

	// SlowThreshold is the time limit for a statement.
	// The logger writes a statement to the log as slow if it takes longer
	// than this time. A negative value disables the slow statement log.
	//
	// Default: 200ms
	SlowThreshold time.Duration

	// LogParameters controls the parameters in the logged SQL.
	// If it is true, the logged SQL includes the parameters of each statement.
	// Do not set this field to true in production. The parameters contain
	// user data.
	LogParameters bool

	// Pool is the configuration of the database connection pool.
	Pool PoolConfig
}

// LoadConfig reads the database settings from loader and applies defaults. A nil
// loader is an empty source. LoadConfig returns an error if a required field is empty.
func LoadConfig(loader config.Loader) (Config, error) {
	var c Config
	if loader != nil {
		c.User = strings.TrimSpace(loader.GetString("SQL_DATABASE_USER"))
		c.Password = loader.GetString("SQL_DATABASE_PASSWORD")
		c.DBName = strings.TrimSpace(loader.GetString("SQL_DATABASE_NAME"))
		c.PrimaryAddr = strings.TrimSpace(loader.GetString("SQL_DATABASE_PRIMARY_ADDRESS"))
		c.ReplicaAddrs = config.SplitList(loader.GetString("SQL_DATABASE_REPLICA_ADDRESSES"))
		c.LogParameters = loader.GetBool("SQL_DATABASE_LOG_PARAMETERS")
		c.DialTimeout = loader.GetDuration("SQL_DATABASE_DIAL_TIMEOUT")
		c.ReadTimeout = loader.GetDuration("SQL_DATABASE_READ_TIMEOUT")
		c.WriteTimeout = loader.GetDuration("SQL_DATABASE_WRITE_TIMEOUT")
		c.SlowThreshold = loader.GetDuration("SQL_DATABASE_SLOW_THRESHOLD")
	}
	c.applyDefault()

	c.Pool = loadPoolConfig(loader)
	err := c.validate()
	if err != nil {
		return Config{}, err
	}
	return c, nil
}

// applyDefault puts the default value in each limit that is zero. A negative
// limit stays negative, because it disables that limit.
func (c *Config) applyDefault() {
	if c.DialTimeout == 0 {
		c.DialTimeout = defaultDialTimeout
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
	if c.SlowThreshold == 0 {
		c.SlowThreshold = defaultSlowThreshold
	}
}

// validate returns an error if a required field is empty.
func (c *Config) validate() error {
	if len(c.PrimaryAddr) == 0 {
		return errors.New("SQL_DATABASE_PRIMARY_ADDRESS is required")
	}
	if len(c.User) == 0 {
		return errors.New("SQL_DATABASE_USER is required")
	}
	if len(c.DBName) == 0 {
		return errors.New("SQL_DATABASE_NAME is required")
	}
	return nil
}

type PoolConfig struct {
	// MaxOpenConns sets the maximum number of open connections to the database.
	//
	// If MaxIdleConns is greater than 0, and the new MaxOpenConns is less than
	// MaxIdleConns, the pool reduces MaxIdleConns to the limit of the new
	// MaxOpenConns.
	//
	// If n <= 0, the number of open connections has no limit.
	//
	// Default: 0 (unlimited)
	MaxOpenConns int

	// MaxIdleConns sets the maximum number of connections in the idle connection pool.
	//
	// If MaxOpenConns is greater than 0, but less than the new MaxIdleConns,
	// the pool reduces MaxIdleConns to the limit of MaxOpenConns.
	//
	// If n <= 0, the pool keeps no idle connection.
	MaxIdleConns int

	// ConnMaxLifetime sets the maximum time that a connection can stay in use.
	//
	// The pool can close an expired connection later, before the next use.
	//
	// If d <= 0, the age of a connection never closes that connection.
	ConnMaxLifetime time.Duration

	// ConnMaxIdleTime sets the maximum time that a connection can stay idle.
	//
	// The pool can close an expired connection later, before the next use.
	//
	// If d <= 0, the idle time of a connection never closes that connection.
	ConnMaxIdleTime time.Duration
}

// loadPoolConfig reads the pool settings from loader. A nil loader is an empty
// source. The zero value of each field is the default of database/sql.
func loadPoolConfig(loader config.Loader) PoolConfig {
	var c PoolConfig
	if loader != nil {
		c.MaxOpenConns = loader.GetInt("SQL_DATABASE_MAX_OPEN_CONNS")
		c.MaxIdleConns = loader.GetInt("SQL_DATABASE_MAX_IDLE_CONNS")
		c.ConnMaxLifetime = loader.GetDuration("SQL_DATABASE_CONN_MAX_LIFETIME")
		c.ConnMaxIdleTime = loader.GetDuration("SQL_DATABASE_CONN_MAX_IDLE_TIME")
	}
	return c
}
