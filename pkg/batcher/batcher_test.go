/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
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

// testItem is the shared item type for all tests.
// gid identifies the producer goroutine for tests of per-caller order.
type testItem struct {
	key string
	seq int
	gid int
}

// hashOf returns a deterministic hash of key.
// New has no internal randomization or per-instance random seed. Any stable hash works here.
func hashOf(i testItem) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(i.key))
	return h.Sum64()
}

// waitDone waits for b.WaitDone() or fails the test after timeout.
// Cancel the batcher's context before a call to waitDone.
// WaitDone() returns only after every partition loop exits.
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

// submitSync waits for the result of an accepted request or ctx cancellation.
// Before admission, submitSync returns rejection errors directly.
// After admission, ctx cancellation does not remove the request from the batcher.
// If the result and ctx.Done are ready together, the select can choose either case.
func submitSync(ctx context.Context, b *Batcher[testItem], data testItem) error {
	resultCh := make(chan error, 1)

	err := b.Submit(ctx, data, func(_ any, err error) {
		resultCh <- err
	})
	if err != nil {
		return err
	}

	select {
	case err = <-resultCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// echoFlush replies nil to every request in the batch.
// echoFlush can record each batch under a mutex for later inspection by tests.
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

// newTestBatcher creates a batcher with an independent cancelable context.
// New starts every partition goroutine immediately. The API has no separate Start method.
//
// Call cancel() for each test batcher to prevent goroutine leaks.
// Then call waitDone().
func newTestBatcher(
	t *testing.T,
	batchSize int,
	linger time.Duration,
	queueCap int,
	partitions int,
	flush func(ctx context.Context, reqs []Req[testItem]),
) (*Batcher[testItem], context.CancelFunc) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
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

func TestNewRejectsInvalidConfig(t *testing.T) {
	noopFlush := func(context.Context, []Req[testItem]) {}

	cases := []struct {
		name string
		ctx  context.Context
		cfg  Config[testItem]
		want error
	}{
		{
			name: "nil ctx",
			ctx:  nil,
			cfg: Config[testItem]{
				Hash:  hashOf,
				Flush: noopFlush,
			},
			want: ErrCtxNil,
		},
		{
			name: "nil hash",
			ctx:  t.Context(),
			cfg:  Config[testItem]{Flush: noopFlush},
			want: ErrHashFuncNil,
		},
		{
			name: "nil flush",
			ctx:  t.Context(),
			cfg:  Config[testItem]{Hash: hashOf},
			want: ErrFlushFuncNil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := New(tc.ctx, tc.cfg)
			assert.Nil(t, b)
			assert.ErrorIs(t, err, tc.want)
		})
	}
}

func TestNewAppliesDefaults(t *testing.T) {
	// start creates a batcher and stops it at the end of the subtest.
	start := func(t *testing.T, cfg Config[testItem]) *Batcher[testItem] {
		t.Helper()

		cfg.Hash = hashOf
		cfg.Flush = func(context.Context, []Req[testItem]) {}

		ctx, cancel := context.WithCancel(t.Context())
		b, err := New(ctx, cfg)
		require.NoError(t, err)
		t.Cleanup(func() {
			cancel()
			waitDone(t, b, 2*time.Second)
		})
		return b
	}

	t.Run("batchSize below 1 normalized to 100", func(t *testing.T) {
		for _, bs := range []int{0, -5} {
			b := start(t, Config[testItem]{
				BatchSize:  bs,
				Partitions: 2,
			})
			assert.Equalf(t, 100, b.batchSize, "BatchSize(%d)", bs)
		}
	})

	t.Run("queueCap below 1 defaults to exactly 4x batchSize", func(t *testing.T) {
		// BatchSize=5 gives QueueCapacity=20 without rounding.
		b := start(t, Config[testItem]{
			BatchSize:  5,
			Partitions: 1,
		})
		assert.Equal(t, 20, cap(b.partitions[0].incoming))
	})

	t.Run("explicit queueCap is used exactly for every partition", func(t *testing.T) {
		// QueueCapacity=10 gives every partition a channel of capacity 10,
		// regardless of the number of partitions.
		b := start(t, Config[testItem]{
			BatchSize:     1,
			QueueCapacity: 10,
			Partitions:    3,
		})
		for i := range b.partitions {
			assert.Equal(t, 10, cap(b.partitions[i].incoming))
		}
	})

	t.Run("partitions below 1 defaults to GOMAXPROCS", func(t *testing.T) {
		b := start(t, Config[testItem]{BatchSize: 1})
		assert.Len(t, b.partitions, runtime.GOMAXPROCS(0))
	})
}

func TestFlushTriggeredByLinger(t *testing.T) {
	var mu sync.Mutex
	var batches [][]testItem

	b, cancel := newTestBatcher(
		t,
		100,
		30*time.Millisecond,
		16,
		1,
		echoFlush(&mu, &batches),
	)
	defer func() {
		cancel()
		waitDone(t, b, 5*time.Second)
	}()

	start := time.Now()
	ctx, submitCancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer submitCancel()
	require.NoError(t, submitSync(ctx, b, testItem{key: "solo"}))
	elapsed := time.Since(start)

	assert.GreaterOrEqualf(
		t,
		elapsed,
		25*time.Millisecond,
		"flush happened suspiciously fast; expected it to wait for linger (~30ms)",
	)
	assert.Lessf(
		t,
		elapsed,
		time.Second,
		"flush took too long; linger timer may be broken",
	)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, batches, 1)
	assert.Len(t, batches[0], 1)
}

// A later batch for the same partition cannot enter flush until the earlier batch's synchronous
// flush call returns.
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
		ctx, c := context.WithTimeout(t.Context(), 5*time.Second)
		defer c()

		errCh0 <- submitSync(ctx, b, testItem{
			key: "same",
			seq: 0,
		})
	}()

	select {
	case got := <-entered:
		require.Equal(t, 0, got, "expected batch 0 to enter flush first")

	case <-time.After(2 * time.Second):
		require.Fail(t, "batch 0 never entered flush")
	}

	errCh1 := make(chan error, 1)
	go func() {
		ctx, c := context.WithTimeout(t.Context(), 5*time.Second)
		defer c()

		errCh1 <- submitSync(ctx, b, testItem{
			key: "same",
			seq: 1,
		})
	}()

	// Batch 1 cannot enter flush while batch 0's flush blocks.
	// The partition loop executes at most one synchronous flush call at a time.
	select {
	case got := <-entered:
		require.Failf(t, "", "batch 1 (seq=%d) entered flush before batch 0's flush returned", got)

	case <-time.After(150 * time.Millisecond):
		// Batch 1 does not enter flush yet.
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

// Different partitions can flush concurrently without global serialization.
func TestDifferentPartitionsCanFlushConcurrently(t *testing.T) {
	const numPartitions = 4

	var (
		once           sync.Once
		enteredCount   atomic.Int32
		barrierReached = make(chan struct{})
	)

	flush := func(ctx context.Context, reqs []Req[testItem]) {
		if enteredCount.Add(1) == int32(numPartitions) {
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

	ctx, cancel := context.WithCancel(t.Context())
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
		_, ok := keysByPartition[idx]
		if !ok {
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

			sctx, c := context.WithTimeout(t.Context(), 5*time.Second)
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

// Each case fails before admission.
// Submit must not call Hash or the callback, and must not change the partition state.
func TestSubmitRejectsBeforeAdmission(t *testing.T) {
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	expired, cancelExpired := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancelExpired()

	cases := []struct {
		name        string
		rootCtx     context.Context
		ctx         context.Context
		nilCallback bool
		want        error
		wantClosed  bool
	}{
		{
			name:    "nil ctx",
			rootCtx: t.Context(),
			ctx:     nil,
			want:    ErrCtxNil,
		},
		{
			name:        "nil callback",
			rootCtx:     t.Context(),
			ctx:         t.Context(),
			nilCallback: true,
			want:        ErrResultFuncNil,
		},
		{
			name:    "canceled",
			rootCtx: t.Context(),
			ctx:     canceled,
			want:    context.Canceled,
		},
		{
			name:    "deadline_exceeded",
			rootCtx: t.Context(),
			ctx:     expired,
			want:    context.DeadlineExceeded,
		},
		{
			name:       "closed batcher",
			rootCtx:    canceled,
			ctx:        t.Context(),
			want:       context.Canceled,
			wantClosed: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hashCalls := 0
			callbackCalls := 0
			b := &Batcher[testItem]{
				rootCtx: tc.rootCtx,
				hash: func(testItem) uint64 {
					hashCalls++
					return 0
				},
				partitions: make([]partition[testItem], 1),
			}
			// Leave space throughout the test: a select between enqueue and
			// ctx.Done alone could otherwise accept an already canceled request.
			b.partitions[0].incoming = make(chan Req[testItem], 32)

			onResult := func(any, error) { callbackCalls++ }
			if tc.nilCallback {
				onResult = nil
			}

			for range 32 {
				err := b.Submit(tc.ctx, testItem{key: "rejected"}, onResult)
				require.ErrorIs(t, err, tc.want)
				if tc.wantClosed {
					var ce *CloseError
					require.ErrorAs(t, err, &ce)
				}
			}
			require.Zero(t, hashCalls)
			require.Zero(t, callbackCalls)
			require.Zero(t, b.partitions[0].activeCount())
			require.Empty(t, b.partitions[0].incoming)
		})
	}
}

// A full queue prevents enqueue until the request context expires in this test.
// Submit rejects the request, so Flush never receives it.
func TestSubmitCanceledBeforeEnqueueNeverFlushed(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)

	var mu sync.Mutex
	var flushedKeys []string

	// batchSize=1, queueCap=1 (queue capacity 1), single partition.
	b, cancel := newTestBatcher(
		t,
		1, 0, 1, 1,
		func(ctx context.Context, reqs []Req[testItem]) {
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
		},
	)
	defer func() {
		cancel()
		waitDone(t, b, 5*time.Second)
	}()

	// Step 1: The partition removes "blocker" from the channel, which becomes empty.
	// The flush blocks on release and occupies the partition loop.
	blockerErr := make(chan error, 1)
	go func() {
		ctx, c := context.WithTimeout(t.Context(), 5*time.Second)
		defer c()

		blockerErr <- submitSync(ctx, b, testItem{key: "blocker"})
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("flush never started; test setup is broken")
	}

	// Step 2: "filler" takes the only queue slot after the partition removes "blocker".
	fillerErr := make(chan error, 1)
	go func() {
		ctx, c := context.WithTimeout(t.Context(), 5*time.Second)
		defer c()

		fillerErr <- submitSync(ctx, b, testItem{key: "filler"})
	}()

	require.Eventually(
		t,
		func() bool {
			return len(b.partitions[0].incoming) == 1
		},
		2*time.Second,
		time.Millisecond,
	)

	// Step 3: The queue is full. The partition goroutine blocks inside the flush for "blocker".
	// No goroutine drains the queue. This Submit's enqueue-select must take the ctx.Done() branch.
	// The request cannot reach the channel.
	enqueueCtx, enqueueCancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer enqueueCancel()
	err := submitSync(enqueueCtx, b, testItem{key: "cancel-me"})
	require.ErrorIs(t, err, context.DeadlineExceeded)

	close(release)

	require.NoError(t, <-blockerErr)
	require.NoError(t, <-fillerErr)

	// Verify that a subsequent normal submit succeeds.
	ctx, c := context.WithTimeout(t.Context(), 2*time.Second)
	defer c()
	require.NoError(t, submitSync(ctx, b, testItem{key: "after"}))

	mu.Lock()
	defer mu.Unlock()
	assert.NotContains(t, flushedKeys, "cancel-me")
	assert.ElementsMatch(t, []string{"blocker", "filler", "after"}, flushedKeys)
}

// After queue acceptance, request context cancellation does not stop batch execution.
// This test uses submitSync to wait for the result or cancellation.
func TestSubmitCanceledAfterEnqueueStillFlushed(t *testing.T) {
	var mu sync.Mutex
	var flushedKeys []string
	releaseFlush := make(chan struct{})

	b, cancel := newTestBatcher(
		t,
		1, 0, 4, 1,
		func(ctx context.Context, reqs []Req[testItem]) {
			<-releaseFlush // hold the batch open long enough for the caller's ctx to expire

			mu.Lock()
			for _, r := range reqs {
				flushedKeys = append(flushedKeys, r.Data.key)
			}
			mu.Unlock()

			for _, r := range reqs {
				r.OnResult(nil, nil)
			}
		},
	)
	defer func() {
		cancel()
		waitDone(t, b, 5*time.Second)
	}()

	submitCtx, submitCancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer submitCancel()

	submitDone := make(chan error, 1)
	go func() {
		submitDone <- submitSync(submitCtx, b, testItem{key: "late-caller"})
	}()

	<-submitCtx.Done()
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

func TestFlushPanicIsRecoveredAndPartitionSurvives(t *testing.T) {
	var (
		panics      atomic.Int32
		ctx, cancel = context.WithCancel(t.Context())
	)

	b, err := New(ctx, Config[testItem]{
		BatchSize:  2,
		Partitions: 1,
		Hash:       hashOf,
		OnPanic:    func(*PanicError) { panics.Add(1) },
		Flush: func(_ context.Context, reqs []Req[testItem]) {
			if reqs[0].Data.key == "boom" {
				reqs[0].OnResult(nil, nil) // answered before the panic
				panic("kaboom")
			}
			for _, r := range reqs {
				r.OnResult(nil, nil)
			}
		},
	})
	require.NoError(t, err)
	defer func() {
		cancel()
		waitDone(t, b, 5*time.Second)
	}()

	errs := make(chan error, 2)
	for _, k := range []string{"boom", "boom"} {
		require.NoError(
			t,
			b.Submit(
				t.Context(),
				testItem{key: k},
				func(_ any, err error) { errs <- err },
			),
		)
	}

	var nilCount, panicCount int
	for range 2 {
		select {
		case err = <-errs:
			if err == nil {
				nilCount++
			} else {
				var pe *PanicError
				require.ErrorAs(t, err, &pe)
				require.NotEmpty(t, pe.Stack)
				panicCount++
			}

		case <-time.After(2 * time.Second):
			t.Fatal("callback not delivered after flush panic")
		}
	}
	// The first request already has an answer before the panic. Prevent a duplicate answer.
	require.Equal(t, 1, nilCount)
	require.Equal(t, 1, panicCount)
	require.EqualValues(t, 1, panics.Load())

	// Verify that the partition still works after the panic.
	ok := make(chan error, 2)
	for range 2 {
		require.NoError(
			t,
			b.Submit(
				t.Context(),
				testItem{key: "fine"},
				func(_ any, err error) { ok <- err },
			),
		)
	}
	for range 2 {
		require.NoError(t, <-ok)
	}
}

func TestOnResultIsCalledAtMostOnce(t *testing.T) {
	var (
		calls       atomic.Int32
		ctx, cancel = context.WithCancel(t.Context())
	)

	b, err := New(ctx, Config[testItem]{
		BatchSize:  1,
		Partitions: 1,
		Hash:       hashOf,
		Flush: func(_ context.Context, reqs []Req[testItem]) {
			for _, r := range reqs {
				r.OnResult(nil, nil)
				r.OnResult(nil, nil) // duplicate must be ignored
			}
		},
	})
	require.NoError(t, err)

	done := make(chan struct{})
	require.NoError(
		t,
		b.Submit(
			t.Context(),
			testItem{key: "x"},
			func(any, error) {
				if calls.Add(1) == 1 {
					close(done)
				}
			},
		),
	)
	<-done

	// WaitDone returns only after the active Flush returns.
	// Thus, the duplicate OnResult call is complete before the check.
	cancel()
	waitDone(t, b, 2*time.Second)
	require.EqualValues(t, 1, calls.Load())
}

// Reuse of reqs must not expose data from a previous batch.
// Linger is one hour, so only BatchSize starts a flush.
// The exact batches also show that each partition keeps the queue order.
func TestFlushBySizeKeepsOrderAndDoesNotLeakPreviousBatch(t *testing.T) {
	var mu sync.Mutex
	var seen [][]int
	b, cancel := newTestBatcher(
		t,
		2,
		time.Hour,
		16,
		1,
		func(_ context.Context, reqs []Req[testItem]) {
			ids := make([]int, len(reqs))
			for i, r := range reqs {
				ids[i] = r.Data.seq
				r.OnResult(nil, nil)
			}
			mu.Lock()
			seen = append(seen, ids)
			mu.Unlock()
		},
	)
	defer func() {
		cancel()
		waitDone(t, b, 5*time.Second)
	}()

	done := make(chan struct{}, 6)
	for i := range 6 {
		require.NoError(
			t,
			b.Submit(
				t.Context(),
				testItem{
					key: "k",
					seq: i,
				},
				func(any, error) { done <- struct{}{} },
			),
		)
	}
	for range 6 {
		<-done
	}

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, [][]int{{0, 1}, {2, 3}, {4, 5}}, seen)
}

// With no size or timer flush possible, root context cancellation leaves accepted requests
// for shutdown.
// Shutdown returns the context error through their callbacks without a Flush call.
// The error matches CloseError and context.Canceled.
// This test does not cover cancellation between the context test and the Flush call.
func TestShutdownAnswersPartialBatchAndNeverFlushesCanceledCtx(t *testing.T) {
	var flushedWithCanceled atomic.Int32
	b, cancel := newTestBatcher(
		t,
		100,
		time.Hour,
		16,
		1,
		func(ctx context.Context, reqs []Req[testItem]) {
			if ctx.Err() != nil {
				flushedWithCanceled.Add(1)
			}
			for _, r := range reqs {
				r.OnResult(nil, nil)
			}
		},
	)

	const n = 5
	errs := make(chan error, n)
	for i := range n {
		require.NoError(
			t,
			b.Submit(
				t.Context(),
				testItem{
					key: "k",
					seq: i,
				},
				func(_ any, err error) { errs <- err },
			),
		)
	}

	cancel()
	waitDone(t, b, 5*time.Second)

	for range n {
		err := <-errs
		var ce *CloseError
		require.ErrorAs(t, err, &ce)
		require.ErrorIs(t, err, context.Canceled)
	}
	require.Zero(t, flushedWithCanceled.Load())
}

// -----------------------------------------
// Concurrency / race tests (run with -race)
// -----------------------------------------

// Every request receives its own unique result.
// This verifies that heavy concurrent batching preserves the correct result channel for each
// request. Flush also must receive every request exactly one time.
func TestConcurrent_EveryRequestReceivesItsOwnResult(t *testing.T) {
	resultOf := func(i testItem) error {
		return fmt.Errorf("gid:%d:seq:%d:%s", i.gid, i.seq, i.key)
	}

	var processed atomic.Int64
	b, cancel := newTestBatcher(t, 8, time.Millisecond, 256, 4,
		func(ctx context.Context, reqs []Req[testItem]) {
			processed.Add(int64(len(reqs)))
			for _, r := range reqs {
				r.OnResult(nil, resultOf(r.Data))
			}
		})
	defer func() {
		cancel()
		waitDone(t, b, 5*time.Second)
	}()

	const goroutines = 50
	const perGoroutine = 100
	var wg sync.WaitGroup
	var mismatches atomic.Int64

	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()

			for i := range perGoroutine {
				item := testItem{
					key: fmt.Sprintf("k%d", (g*perGoroutine+i)%17),
					gid: g,
					seq: i,
				}
				ctx, c := context.WithTimeout(t.Context(), 5*time.Second)
				err := submitSync(ctx, b, item)
				c()

				want := resultOf(item).Error()
				if err == nil || err.Error() != want {
					mismatches.Add(1)
					assert.Failf(t, "", "cross-wiring detected: got result %v, want %q", err, want)
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

	require.Zero(t, mismatches.Load())
	require.Equal(t, int64(goroutines*perGoroutine), processed.Load())
}

func TestConcurrent_SubmitDuringShutdownCompletesEveryAcceptedRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
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
	var accepted, completed, duplicateCallbacks, unexpectedErrors atomic.Int64
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

				var calls atomic.Int32
				sctx, c := context.WithTimeout(t.Context(), 500*time.Millisecond)
				err2 := b.Submit(
					sctx,
					testItem{
						key: fmt.Sprintf("k%d-%d", g, i%5),
						seq: i,
					},
					func(any, error) {
						completed.Add(1)
						if calls.Add(1) != 1 {
							duplicateCallbacks.Add(1)
						}
					},
				)

				c()

				var ce *CloseError
				if err2 == nil {
					accepted.Add(1)
				} else if !errors.As(err2, &ce) && !errors.Is(err2, context.DeadlineExceeded) {
					unexpectedErrors.Add(1)
				}
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
	require.Zero(t, unexpectedErrors.Load())
	require.Zero(t, duplicateCallbacks.Load())
	require.Positive(
		t,
		accepted.Load(),
		"the test must admit requests before shutdown",
	)
	require.Equal(
		t,
		accepted.Load(),
		completed.Load(),
		"every accepted request must receive exactly one callback",
	)
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

	ctx, cancel := context.WithCancel(t.Context())
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
					// Occasionally use a very short deadline to test cancellation before and after enqueue under
					// contention.
					timeout = time.Millisecond
				}
				sctx, c := context.WithTimeout(t.Context(), timeout)
				key := fmt.Sprintf("k%d", rnd.Intn(64))
				err := submitSync(sctx, b, testItem{
					key: key,
					seq: i,
				})
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
