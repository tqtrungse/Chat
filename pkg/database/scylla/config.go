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
	"time"

	"xxx/pkg"

	"github.com/gocql/gocql"
)

type Config struct {
	// Hosts are a list of IP addresses or hostnames of the nodes in the database
	// cluster to connect to
	//
	// It is required.
	Hosts []string

	User   string
	Passwd string

	// Keyspace is the initial keyspace.
	//
	// Optional.
	Keyspace string

	// LocalDC is the local datacenter name used by the host-selection policy.
	// The policy combines DC-aware and token-aware routing: it prefers
	// replicas in the local DC and, when the partition key is known, routes
	// queries directly to a replica that owns the partition to avoid an
	// unnecessary coordinator hop.
	//
	// Set this to the DC configured on the Scylla nodes (for example, "dc1").
	// For a single-DC cluster, it may be left empty.
	//
	// Default: ""
	LocalDC string

	// NumConns is the number of connections per Scylla shard.
	// Scylla partitions each node into independent shards, with each partition
	// owned by one shard. The shard-aware driver routes queries directly to the
	// owning shard, so throughput scales primarily with shards and nodes rather
	// than with a large number of connections per shard.
	//
	// Default: 2
	NumConns int

	// NumRetries governs automatic retries of failed queries. Mark a query as
	// idempotent only when repeating the exact statement cannot produce an
	// additional side effect.
	//
	// Default: 3
	NumRetries int

	// ConnectTimeout limits the time spent during connection setup.
	// During initial connection setup, internal queries, AUTH requests will
	// return an error if the client does not receive a response within the
	// ConnectTimeout period.
	// ConnectTimeout is applied to the connection setup queries independently.
	// ConnectTimeout also limits the duration of dialing a new TCP connection
	// in case there is no Dialer nor HostDialer configured.
	//
	// Default: 11s
	ConnectTimeout time.Duration

	// Timeout defines the maximum time to wait for a single server response.
	//
	// When a session creates a Query or Batch, it inherits this timeout as
	// the request timeout.
	//
	// Important notes:
	// 1. This value should be greater than the server timeout for all queries
	//    you execute. Otherwise, you risk creating retry storms: the server
	//    may still be processing the request while the client times out and retries.
	// 2. This timeout does not apply during initial connection setup.
	//    For that, see ConnectTimeout.
	//
	// Default: 11s
	Timeout time.Duration

	// Consistency decides how many replicas must participate in or acknowledge a
	// request before the coordinator returns the result.
	//
	// Default: gocql.Quorum (use gocql.LocalQuorum for multi-DC clusters)
	Consistency gocql.Consistency

	// SerialConsistency decides how lightweight transaction level.
	//
	// Default: gocql.Serial (use gocql.LocalSerial for multi-DC clusters)
	SerialConsistency gocql.SerialConsistency

	ReconnectionBackoffJitterBase time.Duration
	ReconnectionBackoffJitterCap  time.Duration

	// CAPath enables encryption in transit.
	//
	// Default: ""
	CAPath string

	// Compression keeps frame compression on.
	//
	// Default: False
	Compression bool
}

func (c *Config) Load(loader pkg.ConfigLoader) {
	if loader == nil {
		return
	}
	c.Hosts = loader.GetStringSlice("SCYLLA_DATABASE_HOSTS")
	c.User = loader.GetString("SCYLLA_DATABASE_USER")
	c.Passwd = loader.GetString("SCYLLA_DATABASE_PASSWORD")
	c.Keyspace = loader.GetString("SCYLLA_DATABASE_KEYSPACE")
	c.LocalDC = loader.GetString("SCYLLA_DATABASE_LOCAL_DC")
	c.NumConns = loader.GetInt("SCYLLA_DATABASE_NUM_CONNS")
	c.NumRetries = loader.GetInt("SCYLLA_DATABASE_NUM_RETRIES")
	c.ConnectTimeout = loader.GetDuration("SCYLLA_DATABASE_CONNECT_TIMEOUT")
	c.Timeout = loader.GetDuration("SCYLLA_DATABASE_TIMEOUT")
	c.Consistency = gocql.Consistency(loader.GetUint16("SCYLLA_DATABASE_CONSISTENCY"))
	c.SerialConsistency = gocql.SerialConsistency(loader.GetUint16("SCYLLA_DATABASE_SERIAL_CONSISTENCY"))
	c.ReconnectionBackoffJitterBase = loader.GetDuration("SCYLLA_DATABASE_RECONNECTION_BACKOFF_JITTER_BASE")
	c.ReconnectionBackoffJitterCap = loader.GetDuration("SCYLLA_DATABASE_RECONNECTION_BACKOFF_JITTER_CAP")
	c.CAPath = loader.GetString("SCYLLA_DATABASE_CA_PATH")
	c.Compression = loader.GetBool("SCYLLA_DATABASE_COMPRESSION")
}
