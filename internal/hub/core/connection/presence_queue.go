/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package connection

import (
	"errors"
	"sync"

	"xxx/internal/shared/device"
)

var ErrPresenceBusy = errors.New("device presence backlog is full")

const maxDevicePresenceBacklog = 64

type presenceTask func(done func()) error

// presenceQueue serializes completion, not just submission. Worker-pool
// scheduling must not let a stale Add finish after a newer Del or replacement.
// Entries are deleted when idle, so historical device IDs are not retained.
type presenceQueue struct {
	mu    sync.Mutex
	tasks map[device.ID][]presenceTask
}

// canAdd reports whether the device backlog has room for another registration.
func (q *presenceQueue) canAdd(id device.ID) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks[id]) < maxDevicePresenceBacklog
}

// append queues a device operation and returns a starter only when the queue was idle.
// The manager's per-device transition lock protects the canAdd/append pair.
// Del is always admitted: each accepted Add reserves room for final cleanup.
func (q *presenceQueue) append(id device.ID, task presenceTask) func() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.tasks == nil {
		q.tasks = make(map[device.ID][]presenceTask)
	}

	q.tasks[id] = append(q.tasks[id], task)
	if len(q.tasks[id]) == 1 {
		return func() error { return q.run(id, task) }
	}

	return nil
}

// run starts a presence task with a completion callback that advances its queue at most once.
func (q *presenceQueue) run(id device.ID, task presenceTask) error {
	var once sync.Once
	return task(func() { once.Do(func() { q.advance(id) }) })
}

// advance removes the completed task and starts the next one outside the queue lock.
func (q *presenceQueue) advance(id device.ID) {
	q.mu.Lock()
	tasks := q.tasks[id]
	tasks[0] = nil
	tasks = tasks[1:]
	if len(tasks) == 0 {
		delete(q.tasks, id)
		q.mu.Unlock()
		return
	}
	q.tasks[id] = tasks
	next := tasks[0]
	q.mu.Unlock()
	_ = q.run(id, next)
}
