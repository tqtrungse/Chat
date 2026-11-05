/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package scylla

import (
	"strings"
	"time"

	"xxx/pkg/config"

	"github.com/gocql/gocql"
)

const (
	defaultNumRetries             = 3
	defaultReconnectMaxRetries    = 3
	defaultReconnectionJitterBase = 2 * time.Second
	defaultReconnectionJitterCap  = 60 * time.Second
	defaultConnectTimeout         = 11 * time.Second
	defaultTimeout                = 11 * time.Second
)

type Config struct {
	// Hosts is the list of IP addresses or host names of the nodes in the
	// database cluster. The driver connects to these nodes.
	//
	// Required.
	Hosts []string

	// User is the username for authentication.
	User string

	// Passwd is the password for authentication.
	Passwd string

	// Keyspace is the keyspace that the session uses at the start.
	//
	// Optional.
	Keyspace string

	// LocalDC is the name of the local data center.
	// The host-selection policy uses this name.
	//
	// The policy does two things:
	// 1. It selects replicas in the local DC first (DC-aware routing).
	// 2. If the driver knows the partition key, it sends the query directly to
	//    a replica that owns the partition (token-aware routing).
	//    This removes an extra hop through a coordinator node.
	//
	// Set this field to the DC name that is configured on the Scylla nodes
	// (for example, "dc1"). If the cluster has only one DC, you can leave
	// this field empty.
	//
	// Default: ""
	LocalDC string

	// NumConns is the number of connections for each Scylla shard.
	// Scylla divides each node into independent shards. One shard owns each
	// partition. The shard-aware driver sends each query directly to the
	// shard that owns the partition. Throughput increases when you add shards
	// and nodes. It does not increase much when you add connections for each
	// shard.
	//
	// Default: 2
	NumConns int

	// NumRetries is the number of automatic retries for a failed query.
	// Mark a query as idempotent only if a repeat of the same statement
	// cannot cause an additional side effect.
	//
	// Default: 3
	//
	// LoadConfig sets the default only if the key is absent. NewScylla keeps
	// the value zero.
	NumRetries int

	// ConnectTimeout is the maximum time for connection setup.
	// The driver applies it to each setup query separately (for example,
	// internal queries and AUTH requests). A setup query returns an error if
	// the server does not respond in this time.
	// The driver also uses it as the time limit to open a new TCP connection
	// if you do not set a Dialer or HostDialer.
	//
	// Default: 11s
	ConnectTimeout time.Duration

	// Timeout is the maximum time to wait for one response from the server.
	// A Query or Batch that a session creates gets this value as its
	// request timeout.
	//
	// Important:
	// 1. This value must be larger than the server timeout of each query that
	//    you run. If it is smaller, a retry storm can occur: the client
	//    times out and retries while the server still processes the request.
	// 2. This timeout does not apply to connection setup. For connection
	//    setup, see ConnectTimeout.
	//
	// Default: 11s
	Timeout time.Duration

	// Consistency is the number of replicas that must take part in a request
	// or acknowledge it. The coordinator returns the result only after this
	// condition is true.
	//
	// Default: gocql.Quorum
	// For a cluster with more than one DC, use gocql.LocalQuorum.
	//
	// LoadConfig sets the default only if the key is absent. NewScylla keeps
	// the value zero (gocql.Any).
	Consistency gocql.Consistency

	// SerialConsistency is the consistency level for the serial phase of
	// lightweight transactions.
	//
	// Default: gocql.Serial
	// For a cluster with more than one DC, use gocql.LocalSerial.
	SerialConsistency gocql.SerialConsistency

	// ReconnectMaxRetries is the maximum number of attempts to reconnect to
	// a node.
	//
	// Default: 3
	ReconnectMaxRetries uint16

	// ReconnectionJitterBase is the base delay between reconnection attempts.
	//
	// Default: 2s
	ReconnectionJitterBase time.Duration

	// ReconnectionJitterCap is the maximum delay between reconnection
	// attempts.
	//
	// Default: 60s
	ReconnectionJitterCap time.Duration

	// CAPath is the path to the CA certificate file.
	// If you set this field, the driver encrypts data in transit.
	//
	// Default: ""
	CAPath string

	// Compression controls frame compression.
	// Set it to true to compress frames.
	//
	// Default: false
	Compression bool
}

// LoadConfig reads the cluster settings from loader and applies defaults. A nil
// loader is an empty source. LoadConfig returns ErrNilHosts if Hosts is empty.
func LoadConfig(loader config.Loader) (Config, error) {
	var c Config
	if loader != nil {
		c.Hosts = config.SplitList(loader.GetString("SCYLLA_DATABASE_HOSTS"))
		c.User = strings.TrimSpace(loader.GetString("SCYLLA_DATABASE_USER"))
		c.Passwd = loader.GetString("SCYLLA_DATABASE_PASSWORD")
		c.Keyspace = strings.TrimSpace(loader.GetString("SCYLLA_DATABASE_KEYSPACE"))
		c.LocalDC = strings.TrimSpace(loader.GetString("SCYLLA_DATABASE_LOCAL_DC"))
		c.NumConns = loader.GetInt("SCYLLA_DATABASE_NUM_CONNS")
		c.NumRetries = loader.GetInt("SCYLLA_DATABASE_NUM_RETRIES")
		c.ConnectTimeout = loader.GetDuration("SCYLLA_DATABASE_CONNECT_TIMEOUT")
		c.Timeout = loader.GetDuration("SCYLLA_DATABASE_TIMEOUT")
		c.Consistency = gocql.Consistency(loader.GetUint16("SCYLLA_DATABASE_CONSISTENCY"))
		c.SerialConsistency = gocql.SerialConsistency(loader.GetUint16("SCYLLA_DATABASE_SERIAL_CONSISTENCY"))
		c.ReconnectMaxRetries = loader.GetUint16("SCYLLA_DATABASE_RECONNECTION_MAX_RETRIES")
		c.ReconnectionJitterBase = loader.GetDuration("SCYLLA_DATABASE_RECONNECTION_JITTER_BASE")
		c.ReconnectionJitterCap = loader.GetDuration("SCYLLA_DATABASE_RECONNECTION_JITTER_CAP")
		c.CAPath = strings.TrimSpace(loader.GetString("SCYLLA_DATABASE_CA_PATH"))
		c.Compression = loader.GetBool("SCYLLA_DATABASE_COMPRESSION")
	}

	// A configured zero disables retries or selects gocql.Any.
	// Thus only an absent key gets the default.
	unset := func(key string) bool { return loader == nil || !loader.IsSet(key) }
	if c.NumRetries == 0 && unset("SCYLLA_DATABASE_NUM_RETRIES") {
		c.NumRetries = defaultNumRetries
	}
	if c.Consistency == 0 && unset("SCYLLA_DATABASE_CONSISTENCY") {
		c.Consistency = gocql.Quorum
	}
	c.applyDefault()

	err := c.validate()
	if err != nil {
		return Config{}, err
	}
	return c, nil
}

// applyDefault puts the default value in each field that is zero or negative.
// LoadConfig sets the NumRetries and Consistency defaults, because zero is a valid value.
func (c *Config) applyDefault() {
	if c.NumConns <= 0 {
		c.NumConns = 2
	}
	if c.NumRetries < 0 {
		c.NumRetries = defaultNumRetries
	}
	if c.ConnectTimeout <= 0 {
		c.ConnectTimeout = defaultConnectTimeout
	}
	if c.Timeout <= 0 {
		c.Timeout = defaultTimeout
	}
	if c.SerialConsistency == 0 {
		c.SerialConsistency = gocql.Serial
	}
	if c.ReconnectMaxRetries == 0 {
		c.ReconnectMaxRetries = defaultReconnectMaxRetries
	}
	if c.ReconnectionJitterBase <= 0 {
		c.ReconnectionJitterBase = defaultReconnectionJitterBase
	}
	if c.ReconnectionJitterCap <= 0 {
		c.ReconnectionJitterCap = defaultReconnectionJitterCap
	}
}

// validate returns an error if a required field is empty.
func (c *Config) validate() error {
	if len(c.Hosts) == 0 {
		return ErrNilHosts
	}
	return nil
}
