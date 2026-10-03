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

package worker

import (
	"errors"
	"runtime"
	"sync"
	"unsafe"

	"xxx/pkg"
)

var (
	// ErrPoolClosed is returned by Submit once the pool has been closed.
	ErrPoolClosed = errors.New("worker: pool is closed")
	ErrTaskNil    = errors.New("task is nil")
)

// Pool is a work-stealing thread pool built on top of local
// (per-worker run queue — local_queue.go) and elastic
// (shared unbounded injection queue — elastic_queue.go)
type Pool struct {
	panicHandler        func(workerID int, recovered any, stack []byte)
	workers             []*worker
	global              *elastic[Task]
	globalCheckInterval uint32
	globalBatchSize     uint32
	_                   [pkg.CacheLineSize - 2*pkg.PtrSize - unsafe.Sizeof([]*worker{}) - 8]byte

	shutdownMu sync.RWMutex
	closed     bool
	_          [pkg.CacheLineSize - unsafe.Sizeof(sync.Mutex{}) - 1]byte

	tasksWg   sync.WaitGroup // outstanding submitted-or-spawned-but-unfinished tasks
	workersWg sync.WaitGroup // running worker goroutines, for close to join
	_         [pkg.CacheLineSize - 2*unsafe.Sizeof(sync.WaitGroup{})]byte

	wakeCh chan struct{}
	doneCh chan struct{}
}

// New creates a worker pool.
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
	if cfg.NumWorkers <= 0 {
		cfg.NumWorkers = runtime.GOMAXPROCS(0)
	}

	if cfg.GlobalCheckInterval <= 0 {
		cfg.GlobalCheckInterval = 64
	}

	if cfg.GlobalBatchSize <= 0 {
		cfg.GlobalBatchSize = 32
	}

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

	p.shutdownMu.RLock()
	defer p.shutdownMu.RUnlock()
	if p.closed {
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
	p.shutdownMu.Lock()
	if p.closed {
		p.shutdownMu.Unlock()

		// a concurrent first caller may still be shutting down
		p.workersWg.Wait()
		return
	}
	p.closed = true
	p.shutdownMu.Unlock()

	// every submitted/spawned task has actually run, not just been queued
	p.tasksWg.Wait()

	// only now: nothing left anywhere, safe to exit workers
	close(p.doneCh)

	p.workersWg.Wait()
}
