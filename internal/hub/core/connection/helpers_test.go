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
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"xxx/pkg/log"
	"xxx/pkg/nio"
)

func newTestLogger(t *testing.T) *log.Logger {
	t.Helper()

	logger, closeLogger := log.NewLogger(nil)
	t.Cleanup(func() { _ = closeLogger() })
	return logger
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
