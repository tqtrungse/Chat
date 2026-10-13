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
	"testing"
	"time"

	"xxx/internal/hub/domain/message"
	shareddevice "xxx/internal/shared/device"

	"xxx/pkg/batcher"
	workerpool "xxx/pkg/pool/worker"

	"github.com/stretchr/testify/require"
)

func newTestPresenceSyncCore(
	t *testing.T,
	cache DistributedCache,
	cb CircuitBreaker,
	retryAttempts uint32,
) *presenceSync {
	t.Helper()
	return &presenceSync{
		dCache:            cache,
		cb:                cb,
		retryAttempts:     retryAttempts,
		backoffJitterBase: time.Nanosecond,
		backoffJitterCap:  time.Nanosecond,
		logger:            newTestLogger(t),
	}
}

func TestPresenceSync_RetryDistCacheOperationRetriesOnlyInfrastructureErrors(t *testing.T) {
	transientErr := errors.New("redis unavailable")
	attempts := 0
	breakerCalls := 0
	cache := &testDistributedCache{isInfra: func(err error) bool { return errors.Is(err, transientErr) }}
	cb := testCircuitBreaker{execute: func(ctx context.Context, fn func(context.Context) error) error {
		breakerCalls++
		return fn(ctx)
	}}
	syncer := newTestPresenceSyncCore(t, cache, cb, 3)

	err := syncer.retryDistCacheOperation(context.Background(), func(context.Context) error {
		attempts++
		if attempts < 3 {
			return transientErr
		}
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, 3, attempts)
	require.Equal(t, 3, breakerCalls)
}

func TestPresenceSync_RetryDistCacheOperationStopsForNonInfrastructureError(t *testing.T) {
	wantErr := errors.New("invalid cache request")
	attempts := 0
	infraChecks := 0
	cache := &testDistributedCache{isInfra: func(error) bool { infraChecks++; return false }}
	syncer := newTestPresenceSyncCore(t, cache, testCircuitBreaker{}, 5)

	err := syncer.retryDistCacheOperation(context.Background(), func(context.Context) error {
		attempts++
		return wantErr
	})

	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, attempts)
	require.Equal(t, 1, infraChecks)
}

func TestPresenceSync_RetryDistCacheOperationStopsForOpenBreaker(t *testing.T) {
	breakerCalls := 0
	cache := &testDistributedCache{isInfra: func(error) bool {
		t.Fatal("open breaker errors must not be classified or retried")
		return false
	}}
	cb := testCircuitBreaker{execute: func(context.Context, func(context.Context) error) error {
		breakerCalls++
		return ErrCbOpen
	}}
	syncer := newTestPresenceSyncCore(t, cache, cb, 3)

	err := syncer.retryDistCacheOperation(context.Background(), func(context.Context) error {
		t.Fatal("operation must not run while breaker is open")
		return nil
	})

	require.ErrorIs(t, err, ErrCbOpen)
	require.Equal(t, 1, breakerCalls)
}

func TestPresenceSync_RetryDistCacheOperationDoesNotRunAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	breakerCalls := 0
	syncer := newTestPresenceSyncCore(t, &testDistributedCache{}, testCircuitBreaker{execute: func(context.Context, func(context.Context) error) error {
		breakerCalls++
		return nil
	}}, 2)

	err := syncer.retryDistCacheOperation(ctx, func(context.Context) error {
		t.Fatal("operation must not run for a canceled context")
		return nil
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, breakerCalls)
}

func TestPresenceSync_RetryDistCacheOperationDoesNotRetryPanic(t *testing.T) {
	attempts := 0
	cache := &testDistributedCache{isInfra: func(error) bool {
		t.Fatal("panic errors must not be classified as infrastructure errors")
		return false
	}}
	syncer := newTestPresenceSyncCore(t, cache, testCircuitBreaker{}, 4)

	err := syncer.retryDistCacheOperation(context.Background(), func(context.Context) error {
		attempts++
		panic("cache panic")
	})

	var panicErr *cacheOpPanicError
	require.ErrorAs(t, err, &panicErr)
	require.Equal(t, "cache panic", panicErr.value)
	require.Equal(t, 1, attempts)
}

func TestPresenceSync_FlushDeduplicatesAndSendsResults(t *testing.T) {
	pool, _ := newTestPool(t)
	var (
		added   []shareddevice.ID
		deleted []shareddevice.ID
	)
	cache := &testDistributedCache{
		add: func(_ context.Context, ids []shareddevice.ID) (map[shareddevice.ID][]message.Date, error) {
			added = append([]shareddevice.ID(nil), ids...)
			return nil, nil
		},
		del: func(_ context.Context, ids []shareddevice.ID) error {
			deleted = append([]shareddevice.ID(nil), ids...)
			return nil
		},
	}
	syncer := newTestPresenceSyncCore(t, cache, testCircuitBreaker{}, 1)
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

	require.ElementsMatch(t, []shareddevice.ID{2}, added)
	require.ElementsMatch(t, []shareddevice.ID{1}, deleted)
	got := map[int]error{}
	for range len(reqs) {
		item := <-results
		got[item.idx] = item.err
	}
	require.ErrorIs(t, got[0], errSuperseded)
	require.NoError(t, got[1])
	require.NoError(t, got[2])
}

func TestPresenceSync_FlushResolvesDeleteWhenWorkerPoolIsClosed(t *testing.T) {
	pool, closePool := newTestPool(t)
	closePool()
	syncer := newTestPresenceSyncCore(t, &testDistributedCache{}, testCircuitBreaker{}, 1)
	syncer.pool = pool
	var got error
	reqs := []batcher.Req[deviceOp]{
		{Data: deviceOp{DeviceID: 9, Timestamp: 1, Op: delDeviceOp}, OnResult: func(_ any, err error) { got = err }},
	}

	syncer.pool = pool
	syncer.flush(context.Background(), reqs)

	require.ErrorIs(t, got, workerpool.ErrPoolClosed)
}

func TestNewPresenceSync_UsesRetryDefaults(t *testing.T) {
	logger := newTestLogger(t)
	pool, _ := newTestPool(t)
	syncer, _ := newTestPresenceSync(t, PresenceSyncConfig{BatchSize: 1, Linger: time.Hour}, &testDistributedCache{}, testCircuitBreaker{}, logger, pool)

	require.EqualValues(t, defaultRetryAttempts, syncer.retryAttempts)
	require.Equal(t, defaultBackoffJitterBase, syncer.backoffJitterBase)
	require.Equal(t, defaultBackoffJitterCap, syncer.backoffJitterCap)
}
