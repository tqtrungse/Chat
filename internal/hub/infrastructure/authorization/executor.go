/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

// Package authorization supplies a bounded, dedicated CPU executor for ticket
// and device-proof verification. It never runs a job on the caller's event loop.
package authorization

import (
	"errors"
	"sync/atomic"

	"xxx/pkg/cpu"
	"xxx/pkg/pool/worker"
)

var ErrOverloaded = errors.New("authorization executor is full")

// Executor bounds queued plus running jobs, even after their connection closes.
type Executor struct {
	pool     *worker.Pool
	maxJobs  int64
	inflight atomic.Int64
}

// New returns an executor and an idempotent shutdown function that drains jobs.
// workers and maxJobs must be positive; invalid configuration is a wiring error.
func New(workers int, maxJobs int) (*Executor, func()) {
	if workers <= 0 || maxJobs <= 0 {
		panic("authorization executor requires positive limits")
	}

	cfg := worker.Config{NumWorkers: workers}
	cfg.Load(nil)
	pool, closePool := worker.New(cfg, nil)

	return &Executor{
		pool:    pool,
		maxJobs: int64(maxJobs),
	}, closePool
}

func (e *Executor) Submit(job func()) error {
	if job == nil {
		return worker.ErrTaskNil
	}

	var contention cpu.Backoff
	for {
		n := e.inflight.Load()
		if n >= e.maxJobs {
			return ErrOverloaded
		}
		if e.inflight.CompareAndSwap(n, n+1) {
			break
		}
		// Another submitter or finishing job made progress. Back off between
		// retries without sleeping the caller's event loop or waiting for room.
		if contention.IsDone() {
			return ErrOverloaded
		}
		contention.Snooze()
	}

	err := e.pool.Submit(func(*worker.Context) {
		defer e.inflight.Add(-1)
		job()
	})
	if err != nil {
		e.inflight.Add(-1)
	}
	return err
}
