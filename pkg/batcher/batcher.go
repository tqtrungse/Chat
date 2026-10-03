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

package batcher

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"

	"xxx/pkg/backoff"
)

var (
	ErrClosed       = errors.New("batcher is closed")
	ErrHashFuncNil  = errors.New("hash function is nil")
	ErrFlushFuncNil = errors.New("flush function is nil")
)

const (
	closedBit = uint64(1) << 63

	defaultBatchSize     = 100
	defaultLinger        = 50 * time.Millisecond
	defaultQueueCapacity = defaultBatchSize * 4
)

type Req[T any] struct {
	Data     T
	OnResult func(any, error)
}

type Config[T any] struct {
	// BatchSize is the number of requests grouped into a single call to Flush.
	// Once a partition has accumulated BatchSize requests, it flushes
	// immediately, without waiting for Linger to elapse.
	//
	// Default: 100
	BatchSize int

	// Linger is the maximum time a non-empty partition batch waits for more
	// requests. A non-positive value disables the linger timer; such a batch is
	// flushed only when BatchSize is reached or the batcher's context is canceled.
	//
	// Default: 50ms
	Linger time.Duration

	// QueueCapacity is the size of the buffered channel each partition uses to
	// admit incoming requests before they are grouped into a batch. It bounds
	// how many Submit calls can be waiting ahead of the partition loop; once
	// it's full, Submit blocks (subject to ctx) until a slot frees up.
	//
	// QueueCapacity is independent of BatchSize: BatchSize controls how many
	// requests get flushed together, while QueueCapacity controls how much
	// backlog a partition can hold before applying backpressure on callers.
	// The value is rounded up to the nearest power of two.
	//
	// Default: 4 * BatchSize.
	QueueCapacity int

	// Partitions controls the number of independent batching loops.
	//
	// Default: runtime.GOMAXPROCS.
	Partitions int

	// Hash returns the partitioning hash for an item.
	// Items that must be ordered relative to each other must produce the same hash.
	//
	// It must be not nil.
	Hash func(T) uint64

	// Flush processes one completed batch. It is invoked synchronously from the
	// corresponding partition loop. flush must not retain requests after returning.
	//
	// It must be not nil.
	Flush func(context.Context, []Req[T])
}

// Batcher groups submitted operations into bounded batches and flushes them
// according to batchSize and linger.
//
// Requests with the same hash are routed to the same partition.
// A partition processes requests serially, so batches for the same key
// cannot overtake one another once they have been admitted to the queue.
//
// The flush function is called synchronously by the partition loop. Therefore,
// flush must not return until the batch's operation has reached the completion
// point required by the ordering guarantee. If flush only enqueues work into
// another asynchronous worker pool and returns immediately, a later batch can
// overtake the earlier one.
type Batcher[T any] struct {
	hash       func(T) uint64
	flush      func(ctx context.Context, reqs []Req[T])
	partitions []partition[T]
	batchSize  int
	linger     time.Duration
	wg         sync.WaitGroup
}

// New creates a partitioned operation batcher.
func New[T any](rootCtx context.Context, cfg Config[T]) (*Batcher[T], error) {
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
		cfg.QueueCapacity = defaultQueueCapacity
	}

	if cfg.Partitions < 1 {
		cfg.Partitions = runtime.GOMAXPROCS(0)
	}

	b := &Batcher[T]{
		hash:       cfg.Hash,
		flush:      cfg.Flush,
		partitions: make([]partition[T], cfg.Partitions),
		batchSize:  cfg.BatchSize,
		linger:     cfg.Linger,
	}

	for i := range b.partitions {
		b.partitions[i].incoming = make(chan Req[T], cfg.QueueCapacity)
	}

	b.wg.Add(len(b.partitions))
	for i := range b.partitions {
		go func(partitionIndex int) {
			defer b.wg.Done()
			b.runPartition(rootCtx, partitionIndex)
		}(i)
	}

	return b, nil
}

// Submit admits item into the batcher, routes it to its partition, and waits
// until that request's batch has completed or ctx is canceled. Submit is safe
// for concurrent callers.
//
// Cancellation has two distinct points: if ctx is canceled before the request
// is enqueued, the request is rejected; if it has already been enqueued, the
// request remains in the batch and only the caller stops waiting for its result.
// The latter is important because removing an accepted request from a channel
// safely would otherwise require additional coordination with the partition
// loop.
func (b *Batcher[T]) Submit(
	ctx context.Context,
	data T,
	onResult func(any, error),
) error {
	part := &b.partitions[b.partitionIndex(data)]
	if !part.acquire() {
		return ErrClosed
	}
	defer part.release()

	req := Req[T]{
		Data:     data,
		OnResult: onResult,
	}

	select {
	case part.incoming <- req:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitDone blocks until every partition loop has exited.
func (b *Batcher[T]) WaitDone() {
	b.wg.Wait()
}

// runPartition owns all mutable batching state for one partition. It is the
// only goroutine that reads from that partition's incoming queue, builds batches,
// starts/stops its linger timer, and invokes flush.
//
// At most one flush is active for a partition because flush is called
// synchronously. This is the core ordering guarantee: if two requests for the
// same hash arrive in different batches, the later batch cannot begin until the
// earlier batch's flush has returned. Different partitions have independent
// loops and may therefore flush concurrently.
func (b *Batcher[T]) runPartition(
	rootCtx context.Context,
	partitionIndex int,
) {
	var (
		buf    = make([]Req[T], 0, b.batchSize)
		part   = &b.partitions[partitionIndex]
		timer  *time.Timer
		timerC <-chan time.Time
	)

	flush := func() {
		if len(buf) == 0 {
			return
		}

		batch := buf
		buf = make([]Req[T], 0, b.batchSize)
		if timer != nil {
			timer.Stop()
			timer = nil
			timerC = nil
		}
		b.flush(rootCtx, batch)
	}

	for {
		select {
		case <-rootCtx.Done():
			part.close()

			for _, req := range buf {
				req.OnResult(nil, rootCtx.Err())
			}
			buf = nil
			if timer != nil {
				timer.Stop()
				timer = nil
				timerC = nil
			}

			// Drain requests from callers that acquired admission before
			// closedBit was set. The active count prevents returning before
			// those callers have either enqueued or returned.
			var bc backoff.Cpu
			for {
				select {
				case req := <-part.incoming:
					req.OnResult(nil, rootCtx.Err())
				default:
					if part.activeCount() != 0 {
						bc.Snooze()
						continue
					}
					return
				}
			}

		case <-timerC:
			timerC = nil
			flush()

		case req := <-part.incoming:
			buf = append(buf, req)
			if len(buf) == 1 && b.linger > 0 {
				timer = time.NewTimer(b.linger)
				timerC = timer.C
			}
			if len(buf) >= b.batchSize {
				flush()
			}
		}
	}
}

// partitionIndex maps an item to a deterministic partition.
//
// The mapping is intentionally not stable across batcher instances; persistence
// of partition assignment is not required for the in-memory ordering guarantee.
func (b *Batcher[T]) partitionIndex(item T) int {
	return int(b.hash(item) % uint64(len(b.partitions)))
}
