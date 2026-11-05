/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package scylla

import (
	"errors"
	"sync"
	"sync/atomic"

	"xxx/pkg/log"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3"
)

var ErrNilHosts = errors.New("hosts is nil")

type Scylla struct {
	session      atomic.Pointer[gocqlx.Session]
	cfg          Config
	logger       *log.Logger
	idempotentRP gocql.RetryPolicy
	openOne      sync.Once
	openErr      error
}

// NewScylla makes a Scylla with this configuration. It makes no connection to
// the cluster.
//
// The logger receives the log of the gocql driver. A nil logger discards that
// log.
//
// Call openFn one time to open the cluster. Call closeFn one time to close it.
// NewScylla applies defaults to a copy of cfg.
func NewScylla(cfg Config, logger *log.Logger) (
	s *Scylla,
	openFn func() error,
	closeFn func(),
) {
	cfg.applyDefault()

	s = &Scylla{
		cfg:    cfg,
		logger: logger,
	}
	return s, s.open, s.close
}

// Session gives the session of the cluster.
//
// The result is nil before openFn opens the cluster. The result is nil again
// after closeFn closes the session.
func (s *Scylla) Session() *gocqlx.Session {
	return s.session.Load()
}

// IdempotentRetryPolicy gives the retry policy that newClusterConfig made
// from the configuration. Attach this policy only to a query that is safe to
// repeat.
//
// The result is nil before openFn opens the cluster.
func (s *Scylla) IdempotentRetryPolicy() gocql.RetryPolicy {
	return s.idempotentRP
}

// open opens the cluster and gives the result of that attempt. NewScylla
// gives this method to the caller as openFn.
//
// open gives ErrNilHosts if the configuration holds no host. The first call
// opens the cluster. Each later call gives the error of that first call.
func (s *Scylla) open() error {
	err := s.cfg.validate()
	if err != nil {
		return err
	}
	s.openOne.Do(s.doOpen)
	return s.openErr
}

// close closes the session and clears it. A second call does nothing.
// NewScylla gives this method to the caller as closeFn.
//
// close does not open the cluster again. A later call to openFn gives the
// error of the first call, and Session gives nil.
func (s *Scylla) close() {
	session := s.session.Swap(nil)
	if session != nil {
		session.Close()
	}
}

// doOpen makes the cluster configuration and opens one session. The sync.Once
// in open calls it one time.
//
// doOpen keeps the error of the attempt in openErr. It stores the session only
// if the attempt gives no error.
func (s *Scylla) doOpen() {
	cluster := newClusterConfig(s.cfg)
	// Without this field gocql takes its own defaultLogger, which writes
	// plain text to os.Stdout outside the sink of pkg/log.
	cluster.Logger = newGocqlLogger(s.logger)
	s.idempotentRP = cluster.RetryPolicy

	session, err := gocqlx.WrapSession(gocql.NewSession(*cluster))
	if err == nil {
		s.session.Store(&session)
	}
	s.openErr = err
}

// newClusterConfig makes the cluster configuration of gocql from cfg.
//
// The host selection policy is always token aware. It is also DC aware if
// cfg.LocalDC holds a name.
//
// A CAPath in cfg starts encryption in transit, and gocql then verifies the
// host. newClusterConfig sets no logger, because doOpen sets that field.
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
