/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package presence

import (
	"context"
	"errors"
	"testing"
	"time"

	"xxx/internal/hub/core/presence/mocks"
	"xxx/internal/hub/domain/message"
	"xxx/internal/shared/device"

	"xxx/pkg/batcher"
	"xxx/pkg/breaker"
	breakermocks "xxx/pkg/breaker/mocks"
	"xxx/pkg/jitter"
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

	logger, closeLogger, loggerErr := log.NewLogger(nil)
	require.NoError(t, loggerErr)
	t.Cleanup(func() { _ = closeLogger() })
	return logger
}

func newTestPool(t *testing.T) (*workerpool.Pool, func()) {
	t.Helper()

	poolConfig := workerpool.Config{NumWorkers: 1}
	poolConfig.Load(nil)
	pool, closePool := workerpool.New(poolConfig, nil)
	t.Cleanup(closePool)
	return pool, closePool
}

// runThrough is the behavior of the circuit breaker when it is in the closed state.
func runThrough(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

type testEnv struct {
	dCache         *mocks.MockDistributedCache
	circuitBreaker *breakermocks.MockCircuit
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	return &testEnv{
		dCache:         mocks.NewMockDistributedCache(ctrl),
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

func TestSyncer_RetryDistCacheOperationRejectsInvalidJitter(t *testing.T) {
	for _, tc := range []struct {
		name      string
		base, cap time.Duration
		want      error
	}{
		{
			name: "negative_base",
			base: -time.Second,
			cap:  time.Second,
			want: jitter.ErrBaseNegative,
		},
		{
			name: "base_over_cap",
			base: 2 * time.Second,
			cap:  time.Second,
			want: jitter.ErrBaseOverCap,
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				var (
					env    = newTestEnv(t)
					syncer = env.newSyncerCore(t, 3)
					called = false
				)

				syncer.backoffJitterBase = tc.base
				syncer.backoffJitterCap = tc.cap
				err := syncer.retryDistCacheOperation(
					t.Context(),
					func(context.Context) error {
						called = true
						return nil
					},
				)
				require.ErrorIs(t, err, tc.want)
				require.False(t, called)
				// No breaker or Redis calls are expected for invalid configuration.
			},
		)
	}
}

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
		DoAndReturn(func(err error) bool {
			return errors.Is(err, transientErr)
		}).
		Times(2)

	syncer := env.newSyncerCore(t, 3)
	err := syncer.retryDistCacheOperation(
		t.Context(),
		func(context.Context) error {
			attempts++
			if attempts < 3 {
				return transientErr
			}
			return nil
		},
	)

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
		t.Context(),
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
		t.Context(),
		func(context.Context) error {
			require.Fail(t, "operation must not run while breaker is open")
			return nil
		},
	)
	require.ErrorIs(t, err, breaker.ErrOpen)
}

func TestSyncer_RetryDistCacheOperationDoesNotRunAfterCancellation(t *testing.T) {
	env := newTestEnv(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// Nothing is EXPECTED: cb.Execute and IsInfraError are not called.
	syncer := env.newSyncerCore(t, 2)
	err := syncer.retryDistCacheOperation(
		ctx,
		func(context.Context) error {
			require.Fail(t, "operation must not run for a canceled context")
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
		t.Context(),
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
			gomock.Eq([]device.ID{2}),
		).
		Return(
			map[device.ID][]message.Date(nil),
			nil,
		).
		Times(1)

	env.dCache.
		EXPECT().
		BatchDelDevices(
			gomock.Any(),
			gomock.Eq([]device.ID{1}),
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
		batcher.NewReq(
			deviceOp{DeviceID: 1, Timestamp: 10, Op: addDeviceOp},
			func(_ any, err error) { results <- result{0, err} },
		),
		batcher.NewReq(
			deviceOp{DeviceID: 1, Timestamp: 11, Op: delDeviceOp},
			func(_ any, err error) { results <- result{1, err} },
		),
		batcher.NewReq(
			deviceOp{DeviceID: 2, Timestamp: 20, Op: addDeviceOp},
			func(_ any, err error) { results <- result{2, err} },
		),
	}
	syncer.flush(t.Context(), reqs)
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

	// The pool is closed, so to delete will never reach the store/breaker: nothing will be EXPECTed.
	syncer := env.newSyncerCore(t, 1)
	syncer.pool = pool

	var got error
	reqs := []batcher.Req[deviceOp]{
		batcher.NewReq(
			deviceOp{DeviceID: 9, Timestamp: 1, Op: delDeviceOp},
			func(_ any, err error) { got = err },
		),
	}
	syncer.flush(t.Context(), reqs)
	require.ErrorIs(t, got, workerpool.ErrPoolClosed)
}

// -----------
// constructor
// -----------

func TestNewSyncer_UsesLoadedRetryDefaults(t *testing.T) {
	env := newTestEnv(t)
	pool, _ := newTestPool(t)
	rootCtx, cancel := context.WithCancel(t.Context())

	cfg := Config{
		BatchSize: 1,
		Linger:    time.Hour,
	}
	cfg.Load(nil)

	syncer := NewSyncer(
		rootCtx,
		cfg,
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
