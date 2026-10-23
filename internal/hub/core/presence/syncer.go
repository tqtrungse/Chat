/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package presence

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"xxx/internal/shared/device"

	"xxx/pkg/batcher"
	"xxx/pkg/bit"
	"xxx/pkg/breaker"
	"xxx/pkg/jitter"
	"xxx/pkg/log"
	workerpool "xxx/pkg/pool/worker"

	"go.uber.org/zap"
)

type cacheOpPanicError struct {
	value any
}

// Error describes the recovered cache-operation panic.
func (e *cacheOpPanicError) Error() string {
	return fmt.Sprintf("redis operation panic: %v", e.value)
}

// Syncer keeps the shared cache (device -> owning hub) in sync with
// the connections currently held by this hub.
//
// It batches add/delete ops per device, protects the cache behind a
// circuit breaker, and retries only infrastructure errors with
// decorrelated jitter. This is the single place that owns cache-sync
// resilience; lifecycle code only submits operations and handles results.
type Syncer struct {
	dCache            DistributedCache
	circuitBreaker    breaker.Circuit
	retryAttempts     uint32
	backoffJitterBase time.Duration
	backoffJitterCap  time.Duration
	logger            *log.Logger
	pool              *workerpool.Pool
	batcher           *batcher.Batcher[deviceOp]
}

// NewSyncer creates a presence batcher partitioned by device ID, using the supplied
// worker pool and circuit breaker for cache operations.
func NewSyncer(
	ctx context.Context,
	cfg Config,
	dCache DistributedCache,
	circuitBreaker breaker.Circuit,
	logger *log.Logger,
	pool *workerpool.Pool,
) *Syncer {
	s := &Syncer{
		dCache:            dCache,
		circuitBreaker:    circuitBreaker,
		retryAttempts:     cfg.RetryAttempts,
		backoffJitterBase: cfg.BackoffJitterBase,
		backoffJitterCap:  cfg.BackoffJitterCap,
		logger:            logger,
		pool:              pool,
	}

	s.batcher, _ = batcher.New(
		ctx,
		batcher.Config[deviceOp]{
			BatchSize: normalizeBatchSize(cfg.BatchSize),
			Linger:    cfg.Linger,
			Hash:      func(op deviceOp) uint64 { return bit.Mix64(op.DeviceID.Uint64()) },
			Flush:     s.flush,
		},
	)
	return s
}

// Add queues a timestamped device registration and reports the eventual batch result
// through onResult. An immediate submission error is returned to the caller.
func (s *Syncer) Add(
	ctx context.Context,
	id device.ID,
	ts int64,
	onResult func(any, error),
) error {
	return s.pool.Submit(func(_ *workerpool.Context) {
		err := s.batcher.Submit(
			ctx,
			deviceOp{
				DeviceID:  id,
				Timestamp: ts,
				Op:        addDeviceOp,
			},
			func(resp any, err error) {
				onResult(resp, err)
			},
		)
		if err != nil {
			onResult(nil, err)
		}
	})
}

// Del queues a timestamped device removal. A removal superseded by a newer
// operation completes successfully; immediate worker submission failures are returned.
func (s *Syncer) Del(
	ctx context.Context,
	id device.ID,
	ts int64,
	onResult func(error),
) error {
	return s.pool.Submit(func(_ *workerpool.Context) {
		err := s.batcher.Submit(
			ctx,
			deviceOp{
				DeviceID:  id,
				Timestamp: ts,
				Op:        delDeviceOp,
			},
			func(_ any, err error) {
				if errors.Is(err, errSuperseded) {
					// A newer Add has won: nothing left to delete
					err = nil
				}
				onResult(err)
			},
		)
		if err != nil {
			onResult(err)
		}
	})
}

// flush selects the latest operation per device, executes cache batches with
// retries, and resolves all callbacks before the partition processes another batch.
func (s *Syncer) flush(ctx context.Context, reqs []batcher.Req[deviceOp]) {
	if len(reqs) == 0 {
		return
	}

	isLatest, addDevices, delDevices := dedupeDeviceOps(reqs)

	// Resolve requests that were superseded by a newer operation.
	for idx, req := range reqs {
		if isLatest[idx] {
			continue
		}
		req.OnResult(nil, errSuperseded)
	}

	var wg sync.WaitGroup

	// DELETE is executed through the worker pool so cache connection-pool
	// concurrency can be utilized.
	//
	// We must wait for the worker before returning from flush(). The
	// partition must not start flushing the next batch until this batch
	// has completed.
	if len(delDevices) > 0 {
		wg.Add(1)

		err := s.pool.Submit(func(_ *workerpool.Context) {
			defer wg.Done()

			err := s.retryDistCacheOperation(
				ctx,
				func(ctx2 context.Context) error {
					return s.dCache.BatchDelDevices(ctx2, delDevices)
				},
			)
			sendResult(reqs, isLatest, delDeviceOp, err)
		})

		if err != nil {
			wg.Done()
			s.logger.Error("cache.BatchDelDevices", zap.Error(err))
			sendResult(reqs, isLatest, delDeviceOp, err)
		}
	}

	// ADD is executed synchronously in the partition goroutine.
	//
	// This is safe because a DeviceID can only belong to one of
	// addDevices or delDevices after deduplication.
	if len(addDevices) > 0 {
		err := s.retryDistCacheOperation(
			ctx,
			func(ctx2 context.Context) error {
				_, err := s.dCache.BatchAddDevices(ctx2, addDevices)
				return err
			},
		)
		if err != nil {
			s.logger.Error("cache.BatchAddDevices", zap.Error(err))
		}
		sendResult(reqs, isLatest, addDeviceOp, err)
	}

	// This is important.
	//
	// flush() must not return while the DEL worker is still executing.
	// Otherwise, the next batch in the same partition could start and
	// overtake this batch.
	wg.Wait()
}

// retryDistCacheOperation executes a Redis operation with:
//   - circuit breaker protection
//   - retry only for infrastructure errors
//   - decorrelated jitter between retries
//
// maxAttempts includes the initial attempt.
//
// The function is intentionally synchronous: flush() must not return until
// the operation has completed or permanently failed, otherwise a later batch
// in the same partition could overtake this one.
func (s *Syncer) retryDistCacheOperation(
	ctx context.Context,
	fn func(context.Context) error,
) error {
	delay, err := jitter.New(s.backoffJitterBase, s.backoffJitterCap)
	if err != nil {
		return fmt.Errorf("create cache retry jitter: %w", err)
	}

	var (
		maxAttempts = s.retryAttempts
		lastErr     error
	)

	for attempt := range maxAttempts {
		err = ctx.Err()
		if err != nil {
			return err
		}

		err = s.executeDistCacheAttempt(ctx, fn)
		if err == nil {
			return nil
		}

		lastErr = err

		// A panic is a programming/dependency failure, not a transient
		// infrastructure error. Never retry it.
		_, ok := errors.AsType[*cacheOpPanicError](err)
		if ok {
			return err
		}

		// The caller gave up.
		if errors.Is(err, context.Canceled) {
			return err
		}

		// The circuit breaker is open (or a half-open probe is already
		// running). Do not retry because another immediate Execute would
		// only fail with ErrOpen again.
		if errors.Is(err, breaker.ErrOpen) {
			return err
		}

		// Only infrastructure errors are retryable.
		if !s.dCache.IsInfraError(err) {
			return err
		}

		// No attempts remaining.
		if attempt == maxAttempts-1 {
			return lastErr
		}

		duration := delay.Next()
		timer := time.NewTimer(duration)

		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		}
	}

	return lastErr
}

// executeDistCacheAttempt runs one operation through the circuit breaker and
// converts a panic into a logged, non-retryable cacheOpPanicError.
func (s *Syncer) executeDistCacheAttempt(
	ctx context.Context,
	fn func(context.Context) error,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logger.Error(
				"cache operation panicked",
				zap.Any("panic", recovered),
				zap.Stack("stack"),
			)
			err = &cacheOpPanicError{value: recovered}
		}
	}()

	return s.circuitBreaker.Execute(ctx, fn)
}
