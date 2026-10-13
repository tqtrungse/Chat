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

package connection

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"xxx/internal/hub/domain/message"
	shareddevice "xxx/internal/shared/device"
	sharedticket "xxx/internal/shared/ticket"
	"xxx/pkg/log"
	"xxx/pkg/nio"
	workerpool "xxx/pkg/pool/worker"
)

type testSigner struct {
	verify func(identityPub, payload, sign []byte) error
}

func (s testSigner) Verify(identityPub, payload, sign []byte) error {
	if s.verify != nil {
		return s.verify(identityPub, payload, sign)
	}
	return nil
}

type testTicketVerifier struct {
	open        func(deviceID uint64, ticket []byte) (*sharedticket.Ticket, error)
	checkExpiry func(ticket *sharedticket.Ticket, now time.Time) error
}

func (v testTicketVerifier) Open(deviceID uint64, ticket []byte) (*sharedticket.Ticket, error) {
	if v.open != nil {
		return v.open(deviceID, ticket)
	}
	return nil, nil
}

func (v testTicketVerifier) CheckExpiry(ticket *sharedticket.Ticket, now time.Time) error {
	if v.checkExpiry != nil {
		return v.checkExpiry(ticket, now)
	}
	return nil
}

type testDistributedCache struct {
	add     func(context.Context, []shareddevice.ID) (map[shareddevice.ID][]message.Date, error)
	del     func(context.Context, []shareddevice.ID) error
	isInfra func(error) bool
}

func (c *testDistributedCache) ReapHub(context.Context) error { return nil }

func (c *testDistributedCache) BatchAddDevices(ctx context.Context, ids []shareddevice.ID) (map[shareddevice.ID][]message.Date, error) {
	if c.add != nil {
		return c.add(ctx, ids)
	}
	return nil, nil
}

func (c *testDistributedCache) BatchDelDevices(ctx context.Context, ids []shareddevice.ID) error {
	if c.del != nil {
		return c.del(ctx, ids)
	}
	return nil
}

func (c *testDistributedCache) ListHubsByDevices(context.Context, []shareddevice.ID) ([]uint64, error) {
	return nil, nil
}

func (c *testDistributedCache) IsInfraError(err error) bool {
	return c.isInfra != nil && c.isInfra(err)
}

type testCircuitBreaker struct {
	execute func(context.Context, func(context.Context) error) error
}

func (cb testCircuitBreaker) Execute(ctx context.Context, fn func(context.Context) error) error {
	if cb.execute != nil {
		return cb.execute(ctx, fn)
	}
	return fn(ctx)
}

func (testCircuitBreaker) State() CbState { return CbStateClosed }

func newTestLogger(t *testing.T) *log.Logger {
	t.Helper()

	logger, closeLogger := log.NewLogger(&log.FileConfig{
		Filename:      filepath.Join(t.TempDir(), "connection-test.log"),
		BufferSize:    1024,
		FlushInterval: time.Hour,
	})
	t.Cleanup(func() { _ = closeLogger() })
	return logger
}

func newTestPool(t *testing.T) (*workerpool.Pool, func()) {
	t.Helper()
	pool, closePool := workerpool.New(workerpool.Config{NumWorkers: 1}, nil)
	t.Cleanup(closePool)
	return pool, closePool
}

type testConnState struct {
	mu         sync.RWMutex
	ctx        any
	closeCount atomic.Int32
}

type testConn struct {
	nio.Conn
	state        *testConnState
	asyncWriteFn func([]byte, nio.AsyncCallback) error
}

func newTestConn() (*testConn, *testConnState) {
	state := new(testConnState)
	conn := &testConn{state: state}
	return conn, state
}

func (c *testConn) Context() any {
	c.state.mu.RLock()
	defer c.state.mu.RUnlock()
	return c.state.ctx
}

func (c *testConn) SetContext(ctx any) {
	c.state.mu.Lock()
	c.state.ctx = ctx
	c.state.mu.Unlock()
}

func (c *testConn) Close() error {
	c.state.closeCount.Add(1)
	return nil
}

func (c *testConn) AsyncWrite(packet []byte, callback nio.AsyncCallback) error {
	if c.asyncWriteFn == nil {
		return errors.New("unexpected asynchronous write")
	}
	return c.asyncWriteFn(packet, callback)
}

func newTestPresenceSync(
	t *testing.T,
	cfg PresenceSyncConfig,
	cache DistributedCache,
	cb CircuitBreaker,
	logger *log.Logger,
	pool *workerpool.Pool,
) (*presenceSync, context.CancelFunc) {
	t.Helper()
	rootCtx, cancel := context.WithCancel(context.Background())
	syncer := newPresenceSync(rootCtx, cfg, cache, cb, logger, pool)
	t.Cleanup(func() {
		cancel()
		syncer.batcher.WaitDone()
	})
	return syncer, cancel
}
