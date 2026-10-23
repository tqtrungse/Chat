/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package batcher

import (
	"context"
	"time"
)

type Config[T any] struct {
	// BatchSize is the maximum number of requests in one Flush call.
	// A partition calls Flush immediately when it has BatchSize requests.
	// It does not wait for Linger.
	//
	// Default: 100
	BatchSize int

	// Linger is the flush timer interval of a batch that is not empty.
	// The partition loop starts the timer at the first request of a batch.
	// The interval does not include the time in the queue.
	// A delay in the Go scheduler can cause a later flush.
	// The batcher replaces a value of zero or less with the default.
	//
	// Default: 50ms
	Linger time.Duration

	// QueueCapacity is the channel size of each partition.
	// The channel holds the accepted requests until the partition loop makes a batch.
	// If the channel is full, Submit waits for space.
	// A stop of ctx or of the root context also ends that wait.
	//
	// BatchSize sets how many requests go in one batch.
	// QueueCapacity sets how many requests wait before backpressure reaches the callers.
	// The batcher keeps a positive value. It does not round the value.
	//
	// Default: 4 * BatchSize
	QueueCapacity int

	// Partitions is the number of independent batch loops.
	//
	// Default: runtime.GOMAXPROCS(0)
	Partitions int

	// Hash returns the partition hash of an item.
	// Items that must keep their order must have the same hash.
	//
	// Submit calls Hash on the goroutine of the caller.
	// Hash must be fast.
	// Hash must return the same value for the same item.
	// Hash must not panic.
	//
	// Required. The value must not be nil.
	Hash func(T) uint64

	// Flush processes one batch. A batch can have fewer than BatchSize requests.
	// The partition loop calls Flush synchronously.
	// The ctx comes from the root context and carries the FlushTimeout deadline.
	// The root context and the deadline can both stop ctx before or during the call.
	//
	// Do not keep the request slice or its array after Flush returns.
	// The partition loop clears that array and uses it again.
	// To answer later, copy the Req or its OnResult value before Flush returns.
	// A copy shares the same callback state.
	//
	// Call OnResult one time for each request.
	// The call can come before or after Flush returns.
	// The batcher does not answer a request that Flush leaves out.
	// A caller then waits forever.
	// A later OnResult call does not reach the callback again.
	//
	// Complete the current operation before Flush returns if a later batch needs it.
	//
	// The batcher does not recover a callback panic inside Flush.
	// Such a panic stops Flush, and safeFlush treats it as a Flush panic.
	// To keep callback failures separate, recover the panic in each callback.
	//
	// After a Flush panic, safeFlush recovers it and calls OnPanic.
	// The batcher then calls OnResult(nil, *PanicError) for each request without an answer.
	// The partition continues if OnPanic follows its contract.
	//
	// Flush must not call runtime.Goexit (for example, t.FailNow).
	//
	// Required. The value must not be nil.
	Flush func(context.Context, []Req[T])

	// FlushTimeout is the deadline of one Flush call.
	// The batcher makes a new context from the root context for each call.
	// The deadline applies to one batch and not to the life of the batcher.
	// The batcher replaces a value of zero or less with the default.
	//
	// The deadline does not stop Flush. Flush must read its context to return early.
	// A Flush that ignores the context can continue after the deadline.
	// It then blocks its partition, because the partition loop calls Flush synchronously.
	//
	// Default: 30s
	FlushTimeout time.Duration

	// OnPanic receives the details of a panic from safeFlush or answer.
	// safeFlush calls it after a Flush panic.
	// A callback panic inside Flush is also a Flush panic.
	// answer calls it after a callback panic in shutdown or after a Flush panic.
	//
	// OnPanic must not panic or call runtime.Goexit.
	// safeFlush and answer both ignore a panic from OnPanic.
	// Partition goroutines can call OnPanic at the same time.
	// Use a lock for shared state in this hook.
	// Use OnPanic for logs and metrics.
	//
	// Optional.
	OnPanic func(*PanicError)
}
