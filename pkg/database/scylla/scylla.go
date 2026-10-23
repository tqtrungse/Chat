/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package scylla

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3"
)

var ErrNilHosts = errors.New("hosts is nil")

type Scylla struct {
	session      atomic.Pointer[gocqlx.Session]
	cfg          Config
	idempotentRP gocql.RetryPolicy
	openOne      sync.Once
	openErr      error
}

func NewScylla(cfg Config) (
	s *Scylla,
	openFn func() error,
	closeFn func(),
) {
	s = &Scylla{cfg: cfg}
	return s, s.open, s.close
}

func (s *Scylla) Session() *gocqlx.Session {
	return s.session.Load()
}

func (s *Scylla) IdempotentRetryPolicy() gocql.RetryPolicy {
	return s.idempotentRP
}

func (s *Scylla) open() error {
	if len(s.cfg.Hosts) == 0 {
		return ErrNilHosts
	}
	s.openOne.Do(s.doOpen)
	return s.openErr
}

func (s *Scylla) close() {
	session := s.session.Swap(nil)
	if session != nil {
		session.Close()
	}
}

func (s *Scylla) doOpen() {
	cluster := newClusterConfig(s.cfg)
	s.idempotentRP = cluster.RetryPolicy

	session, err := gocqlx.WrapSession(gocql.NewSession(*cluster))
	if err == nil {
		s.session.Store(&session)
	}
	s.openErr = err
}

func newClusterConfig(cfg Config) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(cfg.Hosts...)
	cluster.Keyspace = cfg.Keyspace
	cluster.NumConns = cfg.NumConns
	cluster.ConnectTimeout = cfg.ConnectTimeout
	cluster.Timeout = cfg.Timeout
	cluster.Consistency = cfg.Consistency
	cluster.SerialConsistency = cfg.SerialConsistency
	cluster.ReconnectionPolicy = &gocql.ExponentialReconnectionPolicy{
		MaxRetries:      int(cfg.ReconnectMaxRetries),
		InitialInterval: cfg.ReconnectionJitterBase,
		MaxInterval:     cfg.ReconnectionJitterCap,
	}
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: cfg.User,
		Password: cfg.Passwd,
	}
	if len(cfg.CAPath) > 0 {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 cfg.CAPath,
			EnableHostVerification: true,
		}
	}
	if len(cfg.LocalDC) == 0 {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	} else {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(cfg.LocalDC))
	}
	if cfg.Compression {
		cluster.Compressor = &gocql.SnappyCompressor{}
	}

	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{
		NumRetries: cfg.NumRetries,
	}
	return cluster
}
