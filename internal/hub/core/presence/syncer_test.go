/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package presence

import (
	"context"
	"errors"
	"testing"
	"time"

	presencemock "xxx/internal/hub/core/presence/mocks"
	"xxx/internal/hub/domain/message"
	shareddevice "xxx/internal/shared/device"

	"xxx/pkg/batcher"
	"xxx/pkg/breaker"
	breakermocks "xxx/pkg/breaker/mocks"
	"xxx/pkg/log"
	workerpool "xxx/pkg/pool/worker"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// ------------
// Test helpers
// ------------

func newTestLogger(t *testing.T) *log.Logger {
	t.Helper()

	logger, closeLogger := log.NewLogger(nil)
	t.Cleanup(func() { _ = closeLogger() })
	return logger
}

func newTestPool(t *testing.T) (*workerpool.Pool, func()) {
	t.Helper()
	pool, closePool := workerpool.New(workerpool.Config{NumWorkers: 1}, nil)
	t.Cleanup(closePool)
	return pool, closePool
}

// runThrough is the behavior of the circuit breaker when it is in the closed state.
func runThrough(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

type testEnv struct {
	dCache         *presencemock.MockDistributedCache
	circuitBreaker *breakermocks.MockCircuit
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	ctrl := gomock.NewController(t)
	return &testEnv{
		dCache:         presencemock.NewMockDistributedCache(ctrl),
		circuitBreaker: breakermocks.NewMockCircuit(ctrl),
	}
}

func (e *testEnv) newSyncerCore(t *testing.T, retryAttempts uint32) *Syncer {
	t.Helper()
	return &Syncer{
		dCache:            e.dCache,
		circuitBreaker:    e.circuitBreaker,
		retryAttempts:     retryAttempts,
		backoffJitterBase: time.Nanosecond,
		backoffJitterCap:  time.Nanosecond,
		logger:            newTestLogger(t),
	}
}

// -----
// retry
// -----

func TestSyncer_RetryDistCacheOperationRetriesOnlyInfrastructureErrors(t *testing.T) {
	env := newTestEnv(t)
	transientErr := errors.New("redis unavailable")
	attempts := 0

	// Passes the breaker 3 times; IsInfraError is only prompted after the first 2 errors.
	env.circuitBreaker.
		EXPECT().
		Execute(
			gomock.Any(),
			gomock.Any(),
		).
		DoAndReturn(runThrough).
		Times(3)

	env.dCache.
		EXPECT().
		IsInfraError(gomock.Any()).
		DoAndReturn(func(err error) bool { return errors.Is(err, transientErr) }).
		Times(2)

	syncer := env.newSyncerCore(t, 3)
	err := syncer.retryDistCacheOperation(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 3 {
			return transientErr
		}
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}

func TestSyncer_RetryDistCacheOperationStopsForNonInfrastructureError(t *testing.T) {
	env := newTestEnv(t)
	wantErr := errors.New("invalid cache request")
	attempts := 0

	env.circuitBreaker.
		EXPECT().
		Execute(
			gomock.Any(),
			gomock.Any(),
		).
		DoAndReturn(runThrough).
		Times(1)

	env.dCache.
		EXPECT().
		IsInfraError(wantErr).
		Return(false).
		Times(1)

	syncer := env.newSyncerCore(t, 5)
	err := syncer.retryDistCacheOperation(
		context.Background(),
		func(context.Context) error {
			attempts++
			return wantErr
		},
	)

	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, attempts)
}

func TestSyncer_RetryDistCacheOperationStopsForOpenBreaker(t *testing.T) {
	env := newTestEnv(t)

	// Not EXPECT IsInfraError: calling it will fail.
	env.circuitBreaker.
		EXPECT().
		Execute(
			gomock.Any(),
			gomock.Any(),
		).
		Return(breaker.ErrOpen).
		Times(1)

	syncer := env.newSyncerCore(t, 3)
	err := syncer.retryDistCacheOperation(
		context.Background(),
		func(context.Context) error {
			t.Fatal("operation must not run while breaker is open")
			return nil
		},
	)
	require.ErrorIs(t, err, breaker.ErrOpen)
}

func TestSyncer_RetryDistCacheOperationDoesNotRunAfterCancellation(t *testing.T) {
	env := newTestEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Nothing is EXPECTED: cb.Execute and IsInfraError are not called.
	syncer := env.newSyncerCore(t, 2)
	err := syncer.retryDistCacheOperation(
		ctx,
		func(context.Context) error {
			t.Fatal("operation must not run for a canceled context")
			return nil
		},
	)
	require.ErrorIs(t, err, context.Canceled)
}

func TestSyncer_RetryDistCacheOperationDoesNotRetryPanic(t *testing.T) {
	env := newTestEnv(t)
	attempts := 0

	// Panic occurs in the fn running through the breaker; it is not classified as infra.
	env.circuitBreaker.
		EXPECT().
		Execute(
			gomock.Any(),
			gomock.Any(),
		).
		DoAndReturn(runThrough).
		Times(1)
	syncer := env.newSyncerCore(t, 4)

	err := syncer.retryDistCacheOperation(
		context.Background(),
		func(context.Context) error {
			attempts++
			panic("cache panic")
		},
	)

	var panicErr *cacheOpPanicError
	require.ErrorAs(t, err, &panicErr)
	require.Equal(t, "cache panic", panicErr.value)
	require.Equal(t, 1, attempts)
}

// -----
// flush
// -----

func TestSyncer_FlushDeduplicatesAndSendsResults(t *testing.T) {
	env := newTestEnv(t)
	pool, _ := newTestPool(t)

	env.circuitBreaker.
		EXPECT().
		Execute(
			gomock.Any(),
			gomock.Any(),
		).
		DoAndReturn(runThrough).
		AnyTimes()

	env.dCache.
		EXPECT().
		BatchAddDevices(
			gomock.Any(),
			gomock.Eq([]shareddevice.ID{2}),
		).
		Return(
			map[shareddevice.ID][]message.Date(nil),
			nil,
		).
		Times(1)

	env.dCache.
		EXPECT().
		BatchDelDevices(
			gomock.Any(),
			gomock.Eq([]shareddevice.ID{1}),
		).
		Return(nil).
		Times(1)

	syncer := env.newSyncerCore(t, 1)
	syncer.pool = pool

	type result struct {
		idx int
		err error
	}
	results := make(chan result, 3)
	reqs := []batcher.Req[deviceOp]{
		{Data: deviceOp{DeviceID: 1, Timestamp: 10, Op: addDeviceOp}, OnResult: func(_ any, err error) { results <- result{0, err} }},
		{Data: deviceOp{DeviceID: 1, Timestamp: 11, Op: delDeviceOp}, OnResult: func(_ any, err error) { results <- result{1, err} }},
		{Data: deviceOp{DeviceID: 2, Timestamp: 20, Op: addDeviceOp}, OnResult: func(_ any, err error) { results <- result{2, err} }},
	}

	syncer.flush(context.Background(), reqs)

	got := map[int]error{}
	for range len(reqs) {
		item := <-results
		got[item.idx] = item.err
	}
	require.ErrorIs(t, got[0], errSuperseded)
	require.NoError(t, got[1])
	require.NoError(t, got[2])
}

func TestSyncer_FlushResolvesDeleteWhenWorkerPoolIsClosed(t *testing.T) {
	env := newTestEnv(t)
	pool, closePool := newTestPool(t)
	closePool()

	// The pool is closed, so the delete will never reach the store/breaker: nothing will be EXPECTed.
	syncer := env.newSyncerCore(t, 1)
	syncer.pool = pool

	var got error
	reqs := []batcher.Req[deviceOp]{
		{Data: deviceOp{DeviceID: 9, Timestamp: 1, Op: delDeviceOp}, OnResult: func(_ any, err error) { got = err }},
	}
	syncer.flush(context.Background(), reqs)
	require.ErrorIs(t, got, workerpool.ErrPoolClosed)
}

// -----------
// constructor
// -----------

func TestNewSyncer_UsesRetryDefaults(t *testing.T) {
	env := newTestEnv(t)
	pool, _ := newTestPool(t)
	rootCtx, cancel := context.WithCancel(context.Background())

	syncer := NewSyncer(
		rootCtx,
		Config{
			BatchSize: 1,
			Linger:    time.Hour,
		},
		env.dCache,
		env.circuitBreaker,
		newTestLogger(t),
		pool,
	)
	t.Cleanup(func() {
		cancel()
		syncer.batcher.WaitDone()
	})

	require.EqualValues(t, defaultRetryAttempts, syncer.retryAttempts)
	require.Equal(t, defaultBackoffJitterBase, syncer.backoffJitterBase)
	require.Equal(t, defaultBackoffJitterCap, syncer.backoffJitterCap)
}
