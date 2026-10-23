/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package queue

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestLockFreeQueue(t *testing.T) {
	const taskNum = 10000
	q := NewAsyncTaskQueue()
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		for range taskNum {
			q.Enqueue(Task{})
		}
		wg.Done()
	}()
	go func() {
		for range taskNum {
			q.Enqueue(Task{})
		}
		wg.Done()
	}()

	var counter atomic.Int32
	go func() {
		for {
			_, ok := q.Dequeue()
			if ok {
				counter.Add(1)
			}
			if !ok && counter.Load() == 2*taskNum {
				break
			}
		}
		wg.Done()
	}()
	wg.Wait()

	t.Logf("sent and received all %d tasks", 2*taskNum)
}
