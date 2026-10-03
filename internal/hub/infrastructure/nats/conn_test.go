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

package nats

import (
	"net"
	"sync/atomic"
	"testing"
	"time"
	
	"xxx/pkg/log"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// runTestServer starts a fresh embedded JetStream-enabled nats-server and
// tears it down via t.Cleanup.
func runTestServer(t *testing.T) *natsserver.Server {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	s, err := natsserver.NewServer(opts)
	require.NoError(t, err)

	go s.Start()
	require.True(t, s.ReadyForConnections(4*time.Second), "nats test server did not become ready")
	t.Cleanup(s.Shutdown)
	return s
}

func TestDefaultConnConfig(t *testing.T) {
	cc := DefaultConnConfig()
	require.Equal(t, -1, cc.MaxReconnects)
	require.Equal(t, 2*time.Second, cc.ReconnectWait)
	require.NotEmpty(t, cc.Name)
}

func TestConnect_ValidationErrors(t *testing.T) {
	cc := DefaultConnConfig()
	logger := newTestLogger(t)
	flag := atomic.Bool{}
	noop := func() {}

	tests := []struct {
		name         string
		logger       *log.Logger
		shuttingDown *atomic.Bool
		onFatalClose func()
	}{
		{"nil logger", nil, &flag, noop},
		{"nil shuttingDown", logger, nil, noop},
		{"nil onFatalClose", logger, &flag, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// An unreachable address is fine here — validation happens
			// before nats.Connect is ever attempted.
			nc, err := Connect("nats://127.0.0.1:1", cc, tt.logger, tt.shuttingDown, tt.onFatalClose)
			require.Error(t, err)
			require.Nil(t, nc)
		})
	}
}

func TestConnect_UnreachableServer(t *testing.T) {
	cc := DefaultConnConfig()
	flag := atomic.Bool{}
	nc, err := Connect("nats://127.0.0.1:1", cc, newTestLogger(t), &flag, func() {})
	require.Error(t, err)
	require.Nil(t, nc)
}

func TestConnect_Success_And_GracefulClose(t *testing.T) {
	s := runTestServer(t)
	cc := DefaultConnConfig()
	cc.ReconnectWait = 50 * time.Millisecond

	var shuttingDown atomic.Bool
	fatalCh := make(chan struct{}, 1)
	nc, err := Connect(s.ClientURL(), cc, newTestLogger(t), &shuttingDown, func() {
		select {
		case fatalCh <- struct{}{}:
		default:
		}
	})
	require.NoError(t, err)
	require.Equal(t, nats.CONNECTED, nc.Status())

	// Simulate a deliberate shutdown: set the flag *before* closing, exactly
	// as the real shutdown path in main.go must do.
	shuttingDown.Store(true)
	nc.Close()

	select {
	case <-fatalCh:
		require.Fail(t, "onFatalClose fired on a graceful (flagged) shutdown")
	case <-time.After(300 * time.Millisecond):
		// expected: no fatal callback on graceful close
	}
}

func TestConnect_UnexpectedClose_TriggersOnFatalClose(t *testing.T) {
	s := runTestServer(t)
	cc := DefaultConnConfig()

	var shuttingDown atomic.Bool // deliberately left false
	fatalCh := make(chan struct{}, 1)
	nc, err := Connect(s.ClientURL(), cc, newTestLogger(t), &shuttingDown, func() {
		select {
		case fatalCh <- struct{}{}:
		default:
		}
	})
	require.NoError(t, err)

	// Closing without having flagged a deliberate shutdown simulates the
	// connection dying on its own (e.g. a fatal, non-retryable server error).
	nc.Close()

	select {
	case <-fatalCh:
		// expected
	case <-time.After(2 * time.Second):
		require.Fail(t, "onFatalClose was not called on an unflagged close")
	}
}

// TestConnect_DisconnectAndReconnect kills the backing server out from under
// an established connection and brings an equivalent one back up on the same
// port, verifying the client observes the disconnect and then reconnects.
// This is the slowest/most integration-heavy test in the suite — if it's
// ever flaky in CI, widen the require.Eventually windows before disabling it.
func TestConnect_DisconnectAndReconnect(t *testing.T) {
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	s, err := natsserver.NewServer(opts)
	require.NoError(t, err)

	go s.Start()
	require.True(t, s.ReadyForConnections(4*time.Second))
	port := s.Addr().(*net.TCPAddr).Port

	cc := DefaultConnConfig()
	cc.ReconnectWait = 100 * time.Millisecond
	cc.MaxReconnects = -1

	var shuttingDown atomic.Bool
	nc, err := Connect(s.ClientURL(), cc, newTestLogger(t), &shuttingDown, func() {})
	require.NoError(t, err)
	t.Cleanup(func() {
		shuttingDown.Store(true)
		nc.Close()
	})

	s.Shutdown()
	require.Eventually(t, func() bool {
		return nc.Status() != nats.CONNECTED
	}, 2*time.Second, 20*time.Millisecond, "client did not observe the server going away")

	opts2 := *opts
	opts2.Port = port
	s2, err := natsserver.NewServer(&opts2)
	require.NoError(t, err)

	go s2.Start()
	require.True(t, s2.ReadyForConnections(4*time.Second))
	t.Cleanup(s2.Shutdown)

	require.Eventually(t, func() bool {
		return nc.Status() == nats.CONNECTED
	}, 5*time.Second, 50*time.Millisecond, "client did not reconnect once the server came back")
}
