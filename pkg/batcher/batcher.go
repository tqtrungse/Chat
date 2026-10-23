/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package batcher

import (
	"context"
	"math/bits"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"xxx/pkg/cpu"
)

const (
	closedBit = uint64(1) << 63

	defaultBatchSize    = 100
	defaultLinger       = 50 * time.Millisecond
	defaultFlushTimeout = 30 * time.Second
)

// onceResult permits one call of fn at most.
// The caller that wins the atomic compare-and-swap calls fn.
// Other callers return and do not wait for fn.
//
// onceResult does not recover a panic from fn.
// The panic goes to the OnResult caller.
// See Config.Flush and Batcher.answer for the recovery rules.
type onceResult struct {
	fn   func(any, error)
	done atomic.Bool
}

func (o *onceResult) call(v any, err error) {
	if o.done.CompareAndSwap(false, true) {
		o.fn(v, err)
	}
}

// Req is a single submitted item. Data is the caller's payload.
type Req[T any] struct {
	Data T
	once *onceResult
}

// NewReq makes a request with the same result rules as Submit.
// It serves callers that build a batch without the batcher,
// such as a direct call of a Config.Flush implementation.
// A nil onResult gives a request whose OnResult does nothing.
func NewReq[T any](data T, onResult func(any, error)) Req[T] {
	req := Req[T]{Data: data}
	if onResult != nil {
		req.once = &onceResult{fn: onResult}
	}
	return req
}

// OnResult sends the request result to the callback from Submit.
// Concurrent calls are safe. One call at most reaches the callback.
// Other calls return and do not wait for the callback.
// OnResult does nothing if once is nil. A zero-value Req has no once.
//
// A callback panic goes to the OnResult caller.
// The panic still uses the one permitted call.
// safeFlush treats such a panic as a Flush panic if Flush calls OnResult directly.
// See Config.Flush for the callback recovery rules.
func (r Req[T]) OnResult(v any, err error) {
	if r.once != nil {
		r.once.call(v, err)
	}
}

// Batcher groups requests into batches by batchSize and linger.
//
// Requests with the same hash go to the same partition.
// A partition processes its requests one after another.
// The batch order follows the queue order in each partition.
// Concurrent Submit calls have no defined order.
//
// The partition loop calls flush synchronously.
// A later batch starts only after the current operation completes.
// Flush must not return before that point.
// If flush returns before its work completes, a later batch can complete first.
type Batcher[T any] struct {
	rootCtx      context.Context
	partitions   []partition[T]
	hash         func(T) uint64
	onPanic      func(*PanicError)
	flush        func(context.Context, []Req[T])
	flushTimeout time.Duration
	linger       time.Duration
	wg           sync.WaitGroup
	mtx          sync.Mutex
	closeErr     atomic.Pointer[CloseError]
	batchSize    int
}

// New makes a batcher with partitions.
// New starts one goroutine for each partition.
// The root context controls the life of these goroutines.
// Cancel the root context before a call to WaitDone.
func New[T any](rootCtx context.Context, cfg Config[T]) (*Batcher[T], error) {
	if rootCtx == nil {
		return nil, ErrCtxNil
	}
	if cfg.Hash == nil {
		return nil, ErrHashFuncNil
	}
	if cfg.Flush == nil {
		return nil, ErrFlushFuncNil
	}

	if cfg.BatchSize < 1 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.Linger <= 0 {
		cfg.Linger = defaultLinger
	}
	if cfg.QueueCapacity < 1 {
		cfg.QueueCapacity = cfg.BatchSize * 4
	}
	if cfg.Partitions < 1 {
		cfg.Partitions = runtime.GOMAXPROCS(0)
	}
	if cfg.FlushTimeout <= 0 {
		cfg.FlushTimeout = defaultFlushTimeout
	}

	b := &Batcher[T]{
		rootCtx:      rootCtx,
		hash:         cfg.Hash,
		partitions:   make([]partition[T], cfg.Partitions),
		onPanic:      cfg.OnPanic,
		flush:        cfg.Flush,
		flushTimeout: cfg.FlushTimeout,
		linger:       cfg.Linger,
		batchSize:    cfg.BatchSize,
	}

	for i := range b.partitions {
		b.partitions[i].incoming = make(chan Req[T], cfg.QueueCapacity)
	}

	b.wg.Add(len(b.partitions))
	for i := range b.partitions {
		go func(partitionIndex int) {
			defer b.wg.Done()
			b.runPartition(partitionIndex)
		}(i)
	}

	return b, nil
}

// Submit puts data in the partition queue that Hash selects.
// A nil return value shows that the queue accepted the request.
// Submit does not wait for the batch. Flush reports the result through onResult.
// Concurrent calls are safe.
//
// Give a ctx and an onResult callback that are not nil.
// The callback must recover its own panic.
// See Config.Flush for the callback recovery rules.
//
// If the queue is full, Submit waits for space.
// A stop of ctx or of the root context also ends that wait.
// An error return shows no queue acceptance and no callback from the batcher.
// A stop of ctx after queue acceptance does not remove or cancel the request.
// The select can choose either case if a stop and the queue become ready together.
// The root context controls batch execution and shutdown.
//
// The batcher rejects new requests in shutdown.
// It sends the rejection error to the onResult callback of each rejected request.
// The error matches ErrClosed and the root context error.
func (b *Batcher[T]) Submit(
	ctx context.Context,
	data T,
	onResult func(any, error),
) error {
	if ctx == nil {
		return ErrCtxNil
	}
	if onResult == nil {
		return ErrResultFuncNil
	}

	err := ctx.Err()
	if err != nil {
		return err
	}

	select {
	case <-b.rootCtx.Done():
		return b.closedErr()
	default:
	}

	part := &b.partitions[b.partitionIndex(data)]
	if !part.acquire() {
		return b.closedErr()
	}
	defer part.release()

	req := Req[T]{
		Data: data,
		once: &onceResult{fn: onResult},
	}

	select {
	case part.incoming <- req:
		return nil

	case <-ctx.Done():
		return ctx.Err()

	case <-b.rootCtx.Done():
		return b.closedErr()
	}
}

// WaitDone blocks until all partition loops exit.
// An active Flush call must return before its partition loop can exit.
// WaitDone does not wait for callbacks that Flush starts in other goroutines.
func (b *Batcher[T]) WaitDone() {
	b.wg.Wait()
}

// runPartition owns all batch state of one partition.
// Only this goroutine reads requests from the partition queue.
// The goroutine makes batches, controls the linger timer, and calls flush.
//
// A partition has one active flush at most, because it calls flush synchronously.
// This rule gives the order guarantee.
// If two requests with the same hash are in different batches,
// the later batch starts only after the earlier flush returns.
// Each partition has its own loop. Thus, partitions can flush at the same time.
func (b *Batcher[T]) runPartition(partIdx int) {
	var (
		part   = &b.partitions[partIdx]
		reqs   = make([]Req[T], 0, b.batchSize)
		timerC <-chan time.Time
	)

	// Each partition has one timer and resets it for each new batch.
	// Stop the timer before the first request.
	timer := time.NewTimer(b.linger)
	timer.Stop()
	defer timer.Stop()

	emit := func() {
		if len(reqs) == 0 {
			return
		}
		if timerC != nil {
			timer.Stop()
			timerC = nil
		}
		// Leave reqs for shutdown if the root context already stopped.
		// The stop can also come after this test or during Flush.
		if b.rootCtx.Err() != nil {
			return
		}

		b.safeFlush(reqs)

		// Flush must not keep this slice or its array (see Config.Flush).
		// Clear the references before reuse.
		// The GC can then free the payloads and the callback state.
		clear(reqs)
		reqs = reqs[:0]
	}

	for {
		// Handle the stop before the next select if the root context is already done.
		// A select can choose pending work even when the stop is ready.
		if b.rootCtx.Err() != nil {
			b.shutdown(partIdx, reqs)
			return
		}

		select {
		case <-b.rootCtx.Done():
			// The next iteration handles shutdown before the select.

		case <-timerC:
			timerC = nil
			emit()

		case req := <-part.incoming:
			reqs = append(reqs, req)
			if len(reqs) >= b.batchSize {
				emit()
			} else if len(reqs) == 1 {
				timer.Reset(b.linger)
				timerC = timer.C
			}
		}
	}
}

// safeFlush calls flush and recovers a panic on the same goroutine.
//
// A direct OnResult call inside flush can also cause this panic.
// safeFlush treats both cases as a Flush failure.
// safeFlush cannot recover a panic from another goroutine that flush starts.
// See Config.Flush for the callback recovery rules.
//
// flush receives a context that comes from the root context.
// That context carries flushTimeout as its deadline.
// A stop of the root context also stops it.
// The deadline applies to one flush call. The next batch gets a new deadline.
//
// The deadline does not stop flush.
// A flush that ignores its context can continue after the deadline and block its partition.
func (b *Batcher[T]) safeFlush(batch []Req[T]) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}

		pe := &PanicError{
			Value: r,
			Stack: debug.Stack(),
		}
		if b.onPanic != nil {
			func() {
				defer func() { _ = recover() }()
				b.onPanic(pe)
			}()
		}

		// onceResult skips a callback that already started, also after a panic in it.
		for _, req := range batch {
			b.answer(req, nil, pe)
		}
	}()

	ctx, cancel := context.WithTimeout(b.rootCtx, b.flushTimeout)
	defer cancel()
	b.flush(ctx, batch)
}

// shutdown closes admission.
// It sends a rejection error to each accepted request that Flush does not process.
//
// shutdown drains the queue while the admitted Submit calls finish.
//
// No Submit call can enqueue a request after shutdown returns.
func (b *Batcher[T]) shutdown(partIdx int, reqs []Req[T]) {
	part := &b.partitions[partIdx]
	part.close()

	err := b.closedErr()
	for _, req := range reqs {
		b.answer(req, nil, err)
	}

	// Drain the requests from Submit calls that got admission before close.
	// Wait for these callers to enqueue or to return before the final drain.
	var contention cpu.Backoff
	for {
		select {
		case req := <-part.incoming:
			b.answer(req, nil, err)
			contention.Reset()

		default:
			if part.activeCount() != 0 {
				contention.Snooze()
				continue
			}
			// Admission is closed and no call holds admission.
			// Drain the queue one last time.
			for {
				select {
				case req := <-part.incoming:
					b.answer(req, nil, err)

				default:
					return
				}
			}
		}
	}
}

// partitionIndex maps an item to a partition.
// The result depends on the hash value and the number of partitions only.
// Equal hashes always give the same partition.
// A stable Hash function keeps each key in one partition.
func (b *Batcher[T]) partitionIndex(item T) int {
	hi, _ := bits.Mul64(b.hash(item), uint64(len(b.partitions)))
	return int(hi)
}

func (b *Batcher[T]) answer(req Req[T], v any, err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if b.onPanic != nil {
			func() {
				defer func() { _ = recover() }()
				b.onPanic(&PanicError{
					Value: r,
					Stack: debug.Stack(),
				})
			}()
		}
	}()
	req.OnResult(v, err)
}

func (b *Batcher[T]) closedErr() error {
	closeErr := b.closeErr.Load()
	if closeErr != nil {
		return closeErr
	}

	b.mtx.Lock()
	closeErr = b.closeErr.Load()
	if closeErr != nil {
		b.mtx.Unlock()
		return closeErr
	}
	closeErr = &CloseError{err: b.rootCtx.Err()}
	b.closeErr.Store(closeErr)
	b.mtx.Unlock()
	return closeErr
}
