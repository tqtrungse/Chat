/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package worker

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"xxx/pkg/cpu"
	"xxx/pkg/os"
)

var (
	// ErrPoolClosed is returned by Submit once the pool has been closed.
	ErrPoolClosed = errors.New("worker: pool is closed")
	ErrTaskNil    = errors.New("task is nil")
)

const closedBit = uint64(1) << 63

// Pool is a work-stealing thread pool built on top of local
// (per-worker run queue — local_queue.go) and elastic
// (shared unbounded injection queue — elastic_queue.go)
type Pool struct {
	panicHandler        func(workerID int, recovered any, stack []byte)
	workers             []*worker
	global              *elastic[Task]
	globalCheckInterval uint32
	globalBatchSize     uint32
	_                   [cpu.CacheLineSize - 2*os.PtrSize - unsafe.Sizeof([]*worker{}) - 8]byte

	// The closed bit and number of active Submit calls share one word so
	// admission cannot race shutdown's check for outstanding submitters.
	submitState atomic.Uint64
	_           [cpu.CacheLineSize - unsafe.Sizeof(atomic.Uint64{})]byte

	tasksWg   sync.WaitGroup // outstanding submitted-or-spawned-but-unfinished tasks
	workersWg sync.WaitGroup // running worker goroutines, for close to join
	_         [cpu.CacheLineSize - 2*unsafe.Sizeof(sync.WaitGroup{})]byte

	wakeCh chan struct{}
	doneCh chan struct{}
}

// New creates a worker pool.
// cfg must be initialized with Config.Load before calling New.
//
// panicHandler, if non-nil, is invoked (from the recovering worker's own
// goroutine) whenever a Task panics. If unset, panics are reported
// via the standard log package. A panicking task never kills its worker.
//
// closeFn stops the pool: no Submit accepted after this call succeeds in
// setting the closed flag, workers finish whatever is already queued,
// and closeFn blocks until every worker goroutine has exited. Safe to
// call more than once — every caller blocks until shutdown is actually
// complete, not just the first one in.
func New(
	cfg Config,
	panicHandler func(workerID int, recovered any, stack []byte),
) (p *Pool, closeFn func()) {
	p = &Pool{
		panicHandler:        panicHandler,
		global:              newElastic[Task](),
		globalCheckInterval: cfg.GlobalCheckInterval,
		globalBatchSize:     uint32(cfg.GlobalBatchSize),
		wakeCh:              make(chan struct{}, cfg.NumWorkers),
		doneCh:              make(chan struct{}),
	}

	p.workers = make([]*worker, cfg.NumWorkers)
	for i := range p.workers {
		p.workers[i] = newWorker(i, p)
	}

	p.workersWg.Add(cfg.NumWorkers)
	for _, w := range p.workers {
		go w.run()
	}
	return p, p.close
}

// NumWorkers returns the number of worker goroutines in the pool.
func (p *Pool) NumWorkers() int {
	return len(p.workers)
}

// Submit enqueues task on the global queue. It returns ErrPoolClosed
// once close has been called (including when it races a concurrent
// close: either this call observes the pool already closed and is
// rejected outright, or it completes fully before close can proceed —
// never a task silently orphaned mid-shutdown).
//
// Safe to call from any goroutine, including from within a running Task
// — though Context.SpawnLocal is cheaper for that case.
func (p *Pool) Submit(task Task) error {
	if task == nil {
		return ErrTaskNil
	}

	// Fast rejection avoids modifying the shared counter after shutdown.
	if p.submitState.Load()&closedBit != 0 {
		return ErrPoolClosed
	}
	state := p.submitState.Add(1)
	defer p.submitState.Add(^uint64(0))
	// Check the result of Add, not just the earlier Load: close may have
	// set the bit in between. Such a caller must not add or publish a task,
	// even if shutdown has already observed zero active submitters.
	if state&closedBit != 0 {
		return ErrPoolClosed
	}

	p.tasksWg.Add(1)
	p.global.Push(task)
	p.wake()
	return nil
}

// Wait blocks until the pool's outstanding task counter reaches zero.
// It must not allow to called in Task.
func (p *Pool) Wait() {
	p.tasksWg.Wait()
}

// wake signals one parked worker, if any is currently blocked waiting.
// Never blocks: if wakeCh's buffer is full, every worker is already busy
// (none are parked to have left the buffer un-drained), so the signal is
// safe to drop — a busy worker re-scans the queues on its own next loop
// iteration regardless of whether it received a wake signal.
func (p *Pool) wake() {
	select {
	case p.wakeCh <- struct{}{}:
	default:
	}
}

func (p *Pool) close() {
	if p.submitState.Or(closedBit)&closedBit != 0 {
		// a concurrent first caller may still be shutting down
		p.workersWg.Wait()
		return
	}

	// All admitted Submit calls must finish Add/Push before Wait can
	// observe a zero task count. No new Submit can pass the closed bit.
	var contention cpu.Backoff
	for p.submitState.Load()&^closedBit != 0 {
		contention.Snooze()
	}

	// every submitted/spawned task has actually run, not just been queued
	p.tasksWg.Wait()

	// only now: nothing left anywhere, safe to exit workers
	close(p.doneCh)

	p.workersWg.Wait()
}
