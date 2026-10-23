/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

// Package testutil supplies a manually drained event loop for NIO tests.
package testutil

import (
	"context"
	"sync"
	"xxx/pkg/nio"
)

// Loop queues callbacks until Drain runs them in FIFO order. Execute is safe
// for concurrent producers; Drain must be called by one test owner at a time.
// Set Err before starting producers to simulate an unavailable event loop.
type Loop struct {
	nio.EventLoop
	mu    sync.Mutex
	queue []func()
	Err   error
}

func (l *Loop) Execute(ctx context.Context, fn nio.RunnableFunc) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.Err != nil {
		return l.Err
	}
	l.queue = append(l.queue, func() { _ = fn(ctx) })
	return nil
}
func (l *Loop) Drain() {
	for {
		l.mu.Lock()
		if len(l.queue) == 0 {
			l.mu.Unlock()
			return
		}
		fn := l.queue[0]
		l.queue = l.queue[1:]
		l.mu.Unlock()
		fn()
	}
}
