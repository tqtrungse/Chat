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

package scylla

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3"
)

var ErrNilHosts = errors.New("hosts is nil")

const (
	defaultNumRetries                    = 3
	defaultReconnectionBackoffJitterBase = 2 * time.Second
	defaultReconnectionBackoffJitterCap  = 60 * time.Second
)

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
	if cfg.NumRetries < 0 {
		cfg.NumRetries = defaultNumRetries
	}
	if cfg.ReconnectionBackoffJitterBase <= 0 {
		cfg.ReconnectionBackoffJitterBase = defaultReconnectionBackoffJitterBase
	}
	if cfg.ReconnectionBackoffJitterCap <= 0 {
		cfg.ReconnectionBackoffJitterCap = defaultReconnectionBackoffJitterCap
	}
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
	cluster := gocql.NewCluster(s.cfg.Hosts...)
	cluster.Keyspace = s.cfg.Keyspace
	cluster.NumConns = s.cfg.NumConns
	cluster.ConnectTimeout = s.cfg.ConnectTimeout
	cluster.Timeout = s.cfg.Timeout
	cluster.Consistency = s.cfg.Consistency
	cluster.SerialConsistency = s.cfg.SerialConsistency
	cluster.ReconnectionPolicy = &gocql.ExponentialReconnectionPolicy{
		MaxRetries:      0, // 0 = unbounded
		InitialInterval: s.cfg.ReconnectionBackoffJitterBase,
		MaxInterval:     s.cfg.ReconnectionBackoffJitterCap,
	}
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: s.cfg.User,
		Password: s.cfg.Passwd,
	}
	if len(s.cfg.CAPath) > 0 {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 s.cfg.CAPath,
			EnableHostVerification: true,
		}
	}
	if len(s.cfg.LocalDC) == 0 {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	} else {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(s.cfg.LocalDC))
	}
	if s.cfg.Compression {
		cluster.Compressor = &gocql.SnappyCompressor{}
	}

	session, err := gocqlx.WrapSession(gocql.NewSession(*cluster))
	if err == nil {
		s.session.Store(&session)
	}
	s.openErr = err
	s.idempotentRP = &gocql.SimpleRetryPolicy{NumRetries: s.cfg.NumRetries}
}
