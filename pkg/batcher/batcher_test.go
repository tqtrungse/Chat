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
	"fmt"
	"hash/fnv"
	"math/rand"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ------------
// Test helpers
// ------------

// testItem is the shared item type used across all tests. gid identifies the
// producing goroutine, for tests that need to reason about per-caller order.
type testItem struct {
	key string
	seq int
	gid int
}

// hashOf is a plain deterministic hash over key -- New no longer owns
// any internal randomization (the previous API's per-instance random seed is
// gone), so any stable hash works here.
func hashOf(i testItem) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(i.key))
	return h.Sum64()
}

// waitDone blocks until b.WaitDone() returns, or fails the test after timeout.
// Callers must cancel the batcher's context before calling this -- WaitDone()
// only returns once every partition loop has exited.
func waitDone(t *testing.T, b *Batcher[testItem], timeout time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		b.WaitDone()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		require.Fail(t, "batcher did not shut down (Done() did not return) in time")
	}
}

// submitSync bridges the callback-based Submit API back into a single
// blocking call, for tests that only care about the end-to-end outcome.
// It mirrors Submit's two cancellation points itself: if the request is
// rejected before being admitted, that error is returned directly; if it
// has already been admitted, the request stays in the batch and is still
// flushed regardless, but this call stops waiting for its OnResult once
// ctx is done, returning ctx.Err().
func submitSync(ctx context.Context, b *Batcher[testItem], data testItem) error {
	resultCh := make(chan error, 1)
	if err := b.Submit(ctx, data, func(_ any, err error) {
		resultCh <- err
	}); err != nil {
		return err
	}
	select {
	case err := <-resultCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// echoFlush replies nil to every request in the batch, optionally recording
// every batch it sees (guarded by a mutex) so tests can inspect batching
// behavior after the fact.
func echoFlush(mu *sync.Mutex, batches *[][]testItem) func(ctx context.Context, reqs []Req[testItem]) {
	return func(ctx context.Context, reqs []Req[testItem]) {
		items := make([]testItem, len(reqs))
		for i, r := range reqs {
			items[i] = r.Data
		}
		if mu != nil {
			mu.Lock()
			*batches = append(*batches, items)
			mu.Unlock()
		}
		for _, r := range reqs {
			r.OnResult(nil, nil)
		}
	}
}

// newTestBatcher constructs a batcher against a fresh, independently
// cancelable context. New starts every partition goroutine
// immediately (there is no separate Start() anymore), so every batcher
// constructed by a test -- including this helper -- must eventually be
// torn down via cancel() + waitDone() to avoid leaking goroutines across
// the test binary.
func newTestBatcher(
	t *testing.T,
	batchSize int,
	linger time.Duration,
	queueCap int,
	partitions int,
	flush func(ctx context.Context, reqs []Req[testItem]),
) (*Batcher[testItem], context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b, err := New(
		ctx,
		Config[testItem]{
			BatchSize:     batchSize,
			Linger:        linger,
			QueueCapacity: queueCap,
			Partitions:    partitions,
			Hash:          hashOf,
			Flush:         flush,
		},
	)
	require.NoError(t, err)
	return b, cancel
}

// -----------
// Logic tests
// -----------

func TestNewBatcherConstructorDefaults(t *testing.T) {
	noopFlush := func(ctx context.Context, reqs []Req[testItem]) {}

	t.Run("nil hash returns ErrHashFuncNil", func(t *testing.T) {
		b, err := New(
			t.Context(),
			Config[testItem]{
				Flush: noopFlush,
			},
		)
		assert.Nil(t, b)
		assert.ErrorIs(t, err, ErrHashFuncNil)
	})

	t.Run("nil flush returns ErrFlushFuncNil", func(t *testing.T) {
		b, err := New(
			t.Context(),
			Config[testItem]{
				Hash: hashOf,
			},
		)
		assert.Nil(t, b)
		assert.ErrorIs(t, err, ErrFlushFuncNil)
	})

	t.Run("batchSize below 1 normalized to 100", func(t *testing.T) {
		for _, bs := range []int{0, -5} {
			ctx, cancel := context.WithCancel(context.Background())
			b, err := New(
				ctx,
				Config[testItem]{
					BatchSize:  bs,
					Partitions: 2,
					Hash:       hashOf,
					Flush:      noopFlush,
				},
			)
			require.NoError(t, err)
			assert.Equalf(t, 100, b.batchSize, "BatchSize(%d)", bs)
			cancel()
			waitDone(t, b, 2*time.Second)
		}
	})

	t.Run("queueCap below 1 defaults to next power of two of 4x batchSize", func(t *testing.T) {
		// batchSize=5 -> base 4*5=20, rounded up to the next power of two: 32.
		ctx, cancel := context.WithCancel(context.Background())
		b, err := New(
			ctx,
			Config[testItem]{
				BatchSize:  5,
				Partitions: 1,
				Hash:       hashOf,
				Flush:      noopFlush,
			},
		)
		require.NoError(t, err)
		assert.Equal(t, 20, cap(b.partitions[0].incoming))
		cancel()
		waitDone(t, b, 2*time.Second)
	})

	t.Run("partitions below 1 defaults to GOMAXPROCS", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		b, err := New(
			ctx,
			Config[testItem]{
				BatchSize: 1,
				Hash:      hashOf,
				Flush:     noopFlush,
			},
		)
		require.NoError(t, err)
		assert.Equal(t, runtime.GOMAXPROCS(0), len(b.partitions))
		cancel()
		waitDone(t, b, 2*time.Second)
	})

	t.Run("explicit queueCap rounds up to next power of two", func(t *testing.T) {
		// QueueCap=10 -> rounded up to 16, applied identically to every
		// partition -- QueueCap is a per-partition size now, so partition
		// count no longer changes it.
		ctx, cancel := context.WithCancel(context.Background())
		b, err := New(
			ctx,
			Config[testItem]{
				BatchSize:     1,
				QueueCapacity: 10,
				Partitions:    3,
				Hash:          hashOf,
				Flush:         noopFlush,
			},
		)
		require.NoError(t, err)
		for i := range b.partitions {
			assert.Equal(t, 10, cap(b.partitions[i].incoming))
		}
		cancel()
		waitDone(t, b, 2*time.Second)
	})
}

func TestFlushTriggeredByBatchSize(t *testing.T) {
	var mu sync.Mutex
	var batches [][]testItem

	b, cancel := newTestBatcher(t, 3, 0, 16, 1, echoFlush(&mu, &batches))
	defer func() {
		cancel()
		waitDone(t, b, 5*time.Second)
	}()

	var wg sync.WaitGroup
	errs := make([]error, 3)
	for i := range 3 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
			defer c()
			errs[i] = submitSync(ctx, b, testItem{key: "k", seq: i})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoErrorf(t, err, "submit %d", i)
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, batches, 1, "expected exactly 1 batch (flushed by size)")
	assert.Len(t, batches[0], 3)
}

func TestFlushTriggeredByLinger(t *testing.T) {
	var mu sync.Mutex
	var batches [][]testItem

	b, cancel := newTestBatcher(t, 100, 30*time.Millisecond, 16, 1, echoFlush(&mu, &batches))
	defer func() {
		cancel()
		waitDone(t, b, 5*time.Second)
	}()

	start := time.Now()
	ctx, submitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer submitCancel()
	require.NoError(t, submitSync(ctx, b, testItem{key: "solo"}))
	elapsed := time.Since(start)

	assert.GreaterOrEqualf(t, elapsed, 25*time.Millisecond, "flush happened suspiciously fast; expected it to wait for linger (~30ms)")
	assert.Lessf(t, elapsed, time.Second, "flush took too long; linger timer may be broken")

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, batches, 1)
	assert.Len(t, batches[0], 1)
}

func TestPerKeyOrderPreservedAcrossBatches(t *testing.T) {
	var mu sync.Mutex
	var batches [][]testItem

	// Single caller, sequential submits: each Submit blocks until its own
	// result arrives, so batchSize can never actually be reached (never more
	// than 1 item in flight at once). linger is the only path to completion
	// here, so every batch ends up size 1 -- what we're checking is that 6
	// flush calls in a row for the same key are observed in submitted order.
	b, cancel := newTestBatcher(t, 2, 15*time.Millisecond, 16, 1, echoFlush(&mu, &batches))
	defer func() {
		cancel()
		waitDone(t, b, 5*time.Second)
	}()

	for i := range 6 {
		ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
		err := submitSync(ctx, b, testItem{key: "ordered", seq: i})
		c()
		require.NoErrorf(t, err, "submit %d", i)
	}

	mu.Lock()
	defer mu.Unlock()
	var seen []int
	for _, batch := range batches {
		for _, it := range batch {
			seen = append(seen, it.seq)
		}
	}
	require.Len(t, seen, 6)
	for i, s := range seen {
		assert.Equalf(t, i, s, "out-of-order delivery, full sequence: %v", seen)
	}
}

// A later batch for the SAME partition must not enter flush until the
// earlier batch's synchronous flush call has returned.
func TestSamePartitionLaterBatchWaitsForEarlierFlush(t *testing.T) {
	entered := make(chan int, 2)
	release := make(chan struct{})

	flush := func(ctx context.Context, reqs []Req[testItem]) {
		entered <- reqs[0].Data.seq
		if reqs[0].Data.seq == 0 {
			<-release // hold batch 0's flush open until the test releases it
		}
		for _, r := range reqs {
			r.OnResult(nil, nil)
		}
	}

	b, cancel := newTestBatcher(t, 1, 0, 16, 1, flush)
	defer func() {
		cancel()
		waitDone(t, b, 5*time.Second)
	}()

	errCh0 := make(chan error, 1)
	go func() {
		sctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		errCh0 <- submitSync(sctx, b, testItem{key: "same", seq: 0})
	}()

	select {
	case got := <-entered:
		require.Equal(t, 0, got, "expected batch 0 to enter flush first")
	case <-time.After(2 * time.Second):
		require.Fail(t, "batch 0 never entered flush")
	}

	errCh1 := make(chan error, 1)
	go func() {
		sctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		errCh1 <- submitSync(sctx, b, testItem{key: "same", seq: 1})
	}()

	// Batch 1 must NOT enter flush while batch 0's flush is still blocked
	// open -- the partition loop can only be inside one synchronous flush
	// call at a time.
	select {
	case got := <-entered:
		require.Failf(t, "", "batch 1 (seq=%d) entered flush before batch 0's flush returned", got)
	case <-time.After(150 * time.Millisecond):
		// expected: nothing happened yet.
	}

	close(release)

	select {
	case got := <-entered:
		require.Equal(t, 1, got, "expected batch 1 to enter flush next")
	case <-time.After(2 * time.Second):
		require.Fail(t, "batch 1 never entered flush after batch 0 completed")
	}

	assert.NoError(t, <-errCh0)
	assert.NoError(t, <-errCh1)
}

// Different partitions must be free to flush concurrently -- no unintended
// global serialization across partitions.
func TestDifferentPartitionsCanFlushConcurrently(t *testing.T) {
	const numPartitions = 4

	var enteredCount int32
	barrierReached := make(chan struct{})
	var once sync.Once

	flush := func(ctx context.Context, reqs []Req[testItem]) {
		if atomic.AddInt32(&enteredCount, 1) == int32(numPartitions) {
			once.Do(func() { close(barrierReached) })
		}
		select {
		case <-barrierReached:
		case <-time.After(3 * time.Second):
		}
		for _, r := range reqs {
			r.OnResult(nil, nil)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	b, err := New(
		ctx,
		Config[testItem]{
			BatchSize:     1,
			QueueCapacity: 16,
			Partitions:    numPartitions,
			Hash:          hashOf,
			Flush:         flush,
		},
	)
	require.NoError(t, err)
	defer func() {
		cancel()
		waitDone(t, b, 5*time.Second)
	}()

	keysByPartition := make(map[int]string)
	for i := 0; len(keysByPartition) < numPartitions && i < 100000; i++ {
		k := fmt.Sprintf("k%d", i)
		idx := b.partitionIndex(testItem{key: k})
		if _, ok := keysByPartition[idx]; !ok {
			keysByPartition[idx] = k
		}
	}
	require.Len(t, keysByPartition, numPartitions, "could not find keys covering all partitions")

	var subWg sync.WaitGroup
	errs := make([]error, numPartitions)
	i := 0
	for _, k := range keysByPartition {
		subWg.Add(1)
		go func(i int, key string) {
			defer subWg.Done()
			sctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			errs[i] = submitSync(sctx, b, testItem{key: key})
		}(i, k)
		i++
	}
	subWg.Wait()

	for j, err := range errs {
		assert.NoErrorf(t, err, "submit %d", j)
	}

	select {
	case <-barrierReached:
	default:
		require.Fail(t, "not all partitions' flushes ran concurrently (barrier never reached); partitions appear to be serialized against each other")
	}
}

func TestSameKeyAlwaysSamePartition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	b, err := New(
		ctx,
		Config[testItem]{
			BatchSize:     1,
			QueueCapacity: 16,
			Partitions:    8,
			Hash:          hashOf,
			Flush:         func(ctx context.Context, reqs []Req[testItem]) {},
		},
	)
	require.NoError(t, err)
	defer func() {
		cancel()
		waitDone(t, b, 5*time.Second)
	}()

	keys := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, k := range keys {
		first := b.partitionIndex(testItem{key: k})
		for i := range 50 {
			got := b.partitionIndex(testItem{key: k, seq: i})
			assert.Equalf(t, first, got, "key %q routed inconsistently on call %d", k, i)
		}
	}
}

func TestSubmitOnClosedBatcherIsRejected(t *testing.T) {
	b, cancel := newTestBatcher(t, 4, 0, 16, 2, func(ctx context.Context, reqs []Req[testItem]) {
		for _, r := range reqs {
			r.OnResult(nil, nil)
		}
	})
	cancel()
	waitDone(t, b, 5*time.Second)

	ctx, c := context.WithTimeout(context.Background(), time.Second)
	defer c()
	err := submitSync(ctx, b, testItem{key: "late"})
	assert.ErrorIs(t, err, ErrClosed)
}

// Cancel BEFORE enqueue: the request must never reach the channel, and
// therefore must never be flushed.
func TestSubmitCanceledBeforeEnqueueNeverFlushed(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)

	var mu sync.Mutex
	var flushedKeys []string

	// batchSize=1, queueCap=1 (queue capacity 1), single partition.
	b, cancel := newTestBatcher(t, 1, 0, 1, 1, func(ctx context.Context, reqs []Req[testItem]) {
		select {
		case started <- struct{}{}:
		default:
		}
		if reqs[0].Data.key == "blocker" {
			<-release
		}
		mu.Lock()
		for _, r := range reqs {
			flushedKeys = append(flushedKeys, r.Data.key)
		}
		mu.Unlock()
		for _, r := range reqs {
			r.OnResult(nil, nil)
		}
	})
	defer func() {
		cancel()
		waitDone(t, b, 5*time.Second)
	}()

	// Step 1: "blocker" is pulled off the (now empty) channel and its flush
	// blocks on `release`, occupying the partition loop.
	blockerErr := make(chan error, 1)
	go func() {
		ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		blockerErr <- submitSync(ctx, b, testItem{key: "blocker"})
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("flush never started; test setup is broken")
	}

	// Step 2: "filler" takes the only queue slot (channel is empty again
	// right after blocker was pulled out of it).
	fillerErr := make(chan error, 1)
	go func() {
		ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
		defer c()
		fillerErr <- submitSync(ctx, b, testItem{key: "filler"})
	}()
	time.Sleep(100 * time.Millisecond) // let it actually land in the channel

	// Step 3: queue is now genuinely full and nothing drains it (the
	// partition goroutine is stuck inside blocker's flush) -- this Submit's
	// enqueue-select must take the ctx.Done() branch; it can never reach
	// the channel.
	enqueueCtx, enqueueCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer enqueueCancel()
	err := submitSync(enqueueCtx, b, testItem{key: "cancel-me"})
	require.ErrorIs(t, err, context.DeadlineExceeded)

	close(release)

	require.NoError(t, <-blockerErr)
	require.NoError(t, <-fillerErr)

	// System must still be healthy: a normal submit afterward must work.
	ctx, c := context.WithTimeout(context.Background(), 2*time.Second)
	defer c()
	require.NoError(t, submitSync(ctx, b, testItem{key: "after"}))

	mu.Lock()
	defer mu.Unlock()
	assert.NotContains(t, flushedKeys, "cancel-me")
	assert.ElementsMatch(t, []string{"blocker", "filler", "after"}, flushedKeys)
}

// Cancel AFTER enqueue: the request stays in the batch and is still
// flushed, even though its caller already gave up waiting.
func TestSubmitCanceledAfterEnqueueStillFlushed(t *testing.T) {
	var mu sync.Mutex
	var flushedKeys []string
	releaseFlush := make(chan struct{})

	b, cancel := newTestBatcher(t, 1, 0, 4, 1, func(ctx context.Context, reqs []Req[testItem]) {
		<-releaseFlush // hold the batch open long enough for the caller's ctx to expire
		mu.Lock()
		for _, r := range reqs {
			flushedKeys = append(flushedKeys, r.Data.key)
		}
		mu.Unlock()
		for _, r := range reqs {
			r.OnResult(nil, nil)
		}
	})
	defer func() {
		cancel()
		waitDone(t, b, 5*time.Second)
	}()

	submitCtx, submitCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer submitCancel()

	submitDone := make(chan error, 1)
	go func() {
		submitDone <- submitSync(submitCtx, b, testItem{key: "late-caller"})
	}()

	time.Sleep(150 * time.Millisecond) // let the caller's ctx expire first
	close(releaseFlush)

	select {
	case err := <-submitDone:
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(2 * time.Second):
		t.Fatal("Submit never returned")
	}

	assert.Eventually(
		t,
		func() bool {
			mu.Lock()
			defer mu.Unlock()
			return slices.Contains(flushedKeys, "late-caller")
		},
		2*time.Second, 5*time.Millisecond,
		"a request canceled after enqueue was never flushed",
	)
}

func TestGracefulShutdownRespondsToPendingRequests(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	b, err := New(
		ctx,
		Config[testItem]{
			BatchSize:     1000,
			QueueCapacity: 64,
			Partitions:    4,
			Hash:          hashOf,
			Flush: func(ctx context.Context, reqs []Req[testItem]) {
				for _, r := range reqs {
					r.OnResult(nil, nil)
				}
			},
		},
	)
	require.NoError(t, err)

	const n = 50
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sctx, c := context.WithTimeout(context.Background(), 3*time.Second)
			defer c()
			errs[i] = submitSync(sctx, b, testItem{key: fmt.Sprintf("k%d", i%4), seq: i})
		}(i)
	}

	time.Sleep(20 * time.Millisecond)
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("submits did not all return after shutdown; possible deadlock")
	}

	for i, err := range errs {
		assert.Errorf(t, err, "submit %d: expected an error after shutdown", i)
	}

	waitDone(t, b, 5*time.Second)
}

func TestShutdownWithEmptyPartitionsDoesNotPanic(t *testing.T) {
	for iter := range 20 {
		ctx, cancel := context.WithCancel(context.Background())
		b, err := New(
			ctx,
			Config[testItem]{
				BatchSize:     4,
				Linger:        5 * time.Millisecond,
				QueueCapacity: 16,
				Partitions:    8,
				Hash:          hashOf,
				Flush: func(ctx context.Context, reqs []Req[testItem]) {
					for _, r := range reqs {
						r.OnResult(nil, nil)
					}
				},
			},
		)
		require.NoErrorf(t, err, "iter %d", iter)
		cancel()
		waitDone(t, b, 2*time.Second)
	}
}

// -----------------------------------------
// Concurrency / race tests (run with -race)
// -----------------------------------------

// Concurrent submit stress with a per-(key,goroutine) ordering assertion: a
// single caller's own submissions for a given key can never be observed out
// of order by flush, no matter how they interleave with other callers/keys.
func TestConcurrent_SubmitAllCompleteAndOrderedPerKey(t *testing.T) {
	type flushRecord struct {
		key string
		gid int
		seq int
	}

	var mu sync.Mutex
	var records []flushRecord
	var processed atomic.Int64

	b, cancel := newTestBatcher(t, 8, 5*time.Millisecond, 256, 4,
		func(ctx context.Context, reqs []Req[testItem]) {
			processed.Add(int64(len(reqs)))
			mu.Lock()
			for _, r := range reqs {
				records = append(records, flushRecord{key: r.Data.key, gid: r.Data.gid, seq: r.Data.seq})
			}
			mu.Unlock()
			for _, r := range reqs {
				r.OnResult(nil, nil)
			}
		})
	defer func() {
		cancel()
		waitDone(t, b, 5*time.Second)
	}()

	const goroutines = 50
	const perGoroutine = 100
	var wg sync.WaitGroup
	var failures atomic.Int64

	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range perGoroutine {
				ctx, c := context.WithTimeout(context.Background(), 5*time.Second)
				key := fmt.Sprintf("k%d", (g*perGoroutine+i)%17)
				err := submitSync(ctx, b, testItem{key: key, gid: g, seq: i})
				c()
				if err != nil {
					failures.Add(1)
				}
			}
		}(g)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("concurrent submits did not complete in time")
	}

	require.Zero(t, failures.Load())
	require.Equal(t, int64(goroutines*perGoroutine), processed.Load())

	mu.Lock()
	defer mu.Unlock()
	type pk struct {
		key string
		gid int
	}
	lastSeq := make(map[pk]int)
	for _, r := range records {
		k := pk{key: r.key, gid: r.gid}
		if prev, ok := lastSeq[k]; ok {
			assert.Greaterf(t, r.seq, prev, "out-of-order flush for key=%s gid=%d", r.key, r.gid)
		}
		lastSeq[k] = r.seq
	}
}

func TestConcurrent_SubmitDuringShutdownNoDeadlockNoPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	b, err := New(
		ctx,
		Config[testItem]{
			BatchSize:     4,
			Linger:        2 * time.Millisecond,
			QueueCapacity: 64,
			Partitions:    4,
			Hash:          hashOf,
			Flush: func(ctx context.Context, reqs []Req[testItem]) {
				for _, r := range reqs {
					r.OnResult(nil, nil)
				}
			},
		},
	)
	require.NoError(t, err)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for g := range 30 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				sctx, c := context.WithTimeout(context.Background(), 500*time.Millisecond)
				_ = submitSync(sctx, b, testItem{key: fmt.Sprintf("k%d-%d", g, i%5), seq: i})
				c()
				i++
			}
		}(g)
	}

	time.Sleep(30 * time.Millisecond)
	cancel()
	time.Sleep(30 * time.Millisecond)
	close(stop)

	waited := make(chan struct{})
	go func() {
		wg.Wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-time.After(10 * time.Second):
		require.Fail(t, "submitting goroutines never returned after shutdown; possible deadlock")
	}

	waitDone(t, b, 5*time.Second)
}

// Every request gets its own, verifiably-unique result -- proves the
// Result channels are never mixed up under heavy concurrent batching.
func TestConcurrent_NoResultCrossWiringUnder(t *testing.T) {
	flush := func(ctx context.Context, reqs []Req[testItem]) {
		for _, r := range reqs {
			r.OnResult(nil, fmt.Errorf("seq:%d:%s", r.Data.seq, r.Data.key))
		}
	}
	b, cancel := newTestBatcher(t, 6, time.Millisecond, 128, 6, flush)
	defer func() {
		cancel()
		waitDone(t, b, 5*time.Second)
	}()

	const n = 500
	var wg sync.WaitGroup
	var mismatches atomic.Int64
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i%23)
			item := testItem{key: key, seq: i}
			sctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			err := submitSync(sctx, b, item)
			want := fmt.Sprintf("seq:%d:%s", item.seq, item.key)
			if err == nil || err.Error() != want {
				mismatches.Add(1)
				assert.Failf(t, "", "cross-wiring detected: submitted seq=%d key=%s, got result %q, want %q", item.seq, item.key, err, want)
			}
		}(i)
	}
	wg.Wait()

	assert.Zero(t, mismatches.Load())
}

// -----------
// Stress test
// -----------

func TestStressManyPartitionsManyGoroutines(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}

	var processed atomic.Int64
	var maxBatch atomic.Int64

	ctx, cancel := context.WithCancel(context.Background())
	b, err := New(
		ctx,
		Config[testItem]{
			BatchSize:     16,
			Linger:        time.Millisecond,
			QueueCapacity: 4096,
			Partitions:    runtime.GOMAXPROCS(0),
			Hash:          hashOf,
			Flush: func(ctx context.Context, reqs []Req[testItem]) {
				if int64(len(reqs)) > maxBatch.Load() {
					maxBatch.Store(int64(len(reqs)))
				}
				time.Sleep(time.Duration(rand.Intn(200)) * time.Microsecond)
				processed.Add(int64(len(reqs)))
				for _, r := range reqs {
					r.OnResult(nil, nil)
				}
			},
		},
	)
	require.NoError(t, err)

	const goroutines = 200
	const perGoroutine = 300
	var wg sync.WaitGroup
	var failures atomic.Int64
	var canceledOK atomic.Int64

	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(g)))
			for i := range perGoroutine {
				timeout := 2 * time.Second
				if rnd.Intn(20) == 0 {
					// Occasionally use a very tight deadline to exercise the
					// cancel-before/after-enqueue paths under real contention.
					timeout = time.Millisecond
				}
				sctx, c := context.WithTimeout(context.Background(), timeout)
				key := fmt.Sprintf("k%d", rnd.Intn(64))
				err := submitSync(sctx, b, testItem{key: key, seq: i})
				c()
				if err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						canceledOK.Add(1)
						continue
					}
					failures.Add(1)
				}
			}
		}(g)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		require.Fail(t, "stress test did not complete in time")
	}

	cancel()
	waitDone(t, b, 10*time.Second)

	assert.Zero(t, failures.Load(), "submits failed with an unexpected (non-deadline) error")
	t.Logf(
		"processed=%d tight-deadline-cancellations=%d max observed batch=%d",
		processed.Load(), canceledOK.Load(), maxBatch.Load(),
	)
}
