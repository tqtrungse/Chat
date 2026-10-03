/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Modified from the original gnet source by tqtrungse@gmail.com in 2026.
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

package queue

import (
	"sync/atomic"

	"xxx/pkg/collection/mpsc"
)

// EventPriority is the priority of an event.
type EventPriority int

const (
	// HighPriority is for the tasks expected to be executed
	// as soon as possible.
	HighPriority EventPriority = iota
	// LowPriority is for the tasks that won't matter much
	// even if they are deferred a little bit.
	LowPriority
)

// Func is the callback function executed by poller.
type Func func(any) error

// Task is a wrapper that contains function and its argument.
type Task struct {
	Exec  Func
	Param any
}

// AsyncTaskQueue is a queue storing asynchronous tasks.
type AsyncTaskQueue interface {
	Enqueue(Task) bool
	Dequeue() (Task, bool)
	IsEmpty() bool
	Length() int32
}

type asyncTaskQueue struct {
	queue  *mpsc.Elastic[Task]
	length atomic.Int32
}

func NewAsyncTaskQueue() AsyncTaskQueue {
	queue, _ := mpsc.NewElastic[Task](1024)
	return &asyncTaskQueue{
		queue: queue,
	}
}

func (aq *asyncTaskQueue) Enqueue(task Task) bool {
	closed := aq.queue.Push(task)
	if !closed {
		aq.length.Add(1)
	}
	return closed
}

func (aq *asyncTaskQueue) Dequeue() (Task, bool) {
	task, ok := aq.queue.TryPop()
	if ok {
		aq.length.Add(-1)
	}
	return task, ok
}

func (aq *asyncTaskQueue) IsEmpty() bool {
	return aq.length.Load() == 0
}

func (aq *asyncTaskQueue) Length() int32 {
	return aq.length.Load()
}
