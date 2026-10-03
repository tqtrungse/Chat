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
	"log"
	"math/rand/v2"
	"runtime/debug"

	"xxx/pkg/hash"
)

// Task is a unit of work. ctx lets a running task spawn follow-up work:
//
//	p.Submit(func(ctx *worker.Context) {
//		heavy()
//		ctx.SpawnLocal(func(ctx *worker.Context) { lighter() })
//	})
//
// SpawnLocal lands the follow-up on the *same* worker's own local queue
// (cheap, single-owner, no contention on the shared queue) instead of
// round-tripping through the global queue like a top-level Submit would.
type Task func(ctx *Context)

// Context is handed to a running Task.
// It must only be used during execution of the Task.
// It must not allow to retain after Task returns.
type Context struct {
	w *worker
}

// SpawnLocal enqueues task on the local queue of the worker currently
// running the calling task. Falls back to the global queue automatically
// if the local queue is momentarily full (see local.Push's overflow).
func (c *Context) SpawnLocal(task Task) {
	c.w.pool.tasksWg.Add(1) // safe without any lock: the caller's own
	// task is itself a live, un-Done'd entry, so tasksWg can't be
	// observed at zero concurrently with this Add.
	c.w.local.Push(task, c.w.pool.global)
	c.w.pool.wake()
}

type worker struct {
	id    int
	pool  *Pool
	local *local[Task]
	// rng is only ever touched by this worker's own goroutine (inside
	// steal), so it needs no locking despite *rand.Rand not being safe
	// for concurrent use in general.
	ctx  Context
	rng  *rand.Rand
	tick uint32
}

func newWorker(id int, p *Pool) *worker {
	w := &worker{
		id:    id,
		pool:  p,
		local: newLocal[Task](),
		rng:   rand.New(rand.NewPCG(uint64(id)+1, hash.MakeSeed())),
	}
	w.ctx = Context{w: w}
	return w
}

func (w *worker) run() {
	defer w.pool.workersWg.Done()

	for {
		task, ok := w.findTask()
		if !ok {
			task, ok = w.parkUntilWork()
			if !ok {
				return // pool closed and fully drained
			}
		}
		w.exec(task)
	}
}

// exec runs task, recovering and reporting any panic so a single bad
// task can't take down this worker.
func (w *worker) exec(task Task) {
	defer w.pool.tasksWg.Done()
	defer func() {
		if r := recover(); r != nil {
			if panicHandler := w.pool.panicHandler; panicHandler != nil {
				panicHandler(w.id, r, debug.Stack())
			} else {
				log.Printf("worker pool: worker %d: task panicked: %v\n%s", w.id, r, debug.Stack())
			}
		}
	}()
	task(&w.ctx)
}

// findTask makes one non-blocking pass over every source of work, in
// priority order.
func (w *worker) findTask() (Task, bool) {
	w.tick++
	if w.tick%w.pool.globalCheckInterval == 0 {
		if t, ok := w.popGlobal(); ok {
			return t, true
		}
	}
	if t, ok := w.local.Pop(); ok {
		return t, true
	}
	if t, ok := w.popGlobal(); ok {
		return t, true
	}
	return w.steal()
}

// popGlobal pulls a batch off the global queue sized to fit whatever
// room is currently free in this worker's local queue: one task is
// returned to run now, the rest is seeded straight into the local queue.
func (w *worker) popGlobal() (Task, bool) {
	n := int(w.pool.globalBatchSize)
	if room := w.local.RemainingSlots(); room < n {
		n = room
	}
	if n == 0 {
		// Local is momentarily full (e.g. right after being on the
		// receiving end of a steal). Grab exactly one task to run
		// directly without touching the local queue at all.
		n = 1
	}

	batch, ok := w.pool.global.PopBatch(n)
	if !ok {
		return nil, false
	}

	task := batch[0]
	if rest := batch[1:]; len(rest) > 0 {
		// Safe without re-checking room: this worker is the local
		// queue's sole owner and hasn't pushed anything else since
		// the RemainingSlots() check above — concurrent stealers can
		// only ever free up MORE room (MoveTo removes from us), never
		// less.
		w.local.PushBatchNoOverflow(rest)
	}
	return task, true
}

// steal visits every other worker exactly once, starting from a random
// offset, trying to move roughly half of its local queue into ours.
func (w *worker) steal() (Task, bool) {
	workers := w.pool.workers
	n := len(workers)
	if n <= 1 {
		return nil, false
	}

	start := w.rng.IntN(n)
	for i := range n {
		victim := workers[(start+i)%n]
		if victim == w {
			continue
		}
		if task, ok := victim.local.MoveTo(w.local); ok {
			return task, true
		}
	}
	return nil, false
}

// parkUntilWork re-checks every source of work while holding pool.mu —
// pairing with Submit/wakeOne's own Lock/Signal/Unlock so no wakeup can
// be lost between the last lock-free attempt and going to sleep — and
// parks on the condvar if it really is empty.
func (w *worker) parkUntilWork() (Task, bool) {
	p := w.pool
	for {
		select {
		case <-p.wakeCh:
			if task, ok := w.findTask(); ok {
				return task, true
			}
			// spurious/redundant wake (someone else already took it,
			// or it was for a peer's local queue we didn't win the
			// steal race for) — loop back and wait again.
		case <-p.doneCh:
			return nil, false
		}
	}
}
