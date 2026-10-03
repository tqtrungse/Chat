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
	"bytes"
	"errors"
	"log"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPool_SubmitExecutesTask(t *testing.T) {
	p, closePool := New(
		Config{
			NumWorkers: 2,
		},
		nil,
	)
	defer closePool()

	done := make(chan struct{})
	err := p.Submit(func(ctx *Context) { close(done) })
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.Fail(t, "task never ran")
	}
}

func TestPool_SubmitMultipleTasksAllExecuteExactlyOnce(t *testing.T) {
	p, closePool := New(
		Config{
			NumWorkers: 4,
		},
		nil,
	)
	defer closePool()

	const n = 10_000
	var counts [n]atomic.Int32
	for i := range n {
		j := i
		err := p.Submit(func(ctx *Context) { counts[j].Add(1) })
		require.NoError(t, err)
	}
	p.Wait()

	for i := range counts {
		got := counts[i].Load()
		require.Equal(t, int32(1), got)
	}
}

func TestPool_SpawnLocalChildExecutes(t *testing.T) {
	p, closePool := New(
		Config{
			NumWorkers: 2,
		},
		nil,
	)
	defer closePool()

	childDone := make(chan struct{})
	err := p.Submit(func(ctx *Context) {
		ctx.SpawnLocal(func(ctx *Context) { close(childDone) })
	})
	require.NoError(t, err)

	select {
	case <-childDone:
	case <-time.After(2 * time.Second):
		require.Fail(t, "spawned child never ran")
	}
}

func TestPool_SpawnLocalDeepChain(t *testing.T) {
	p, closePool := New(
		Config{
			NumWorkers: 2,
		},
		nil,
	)
	defer closePool()

	const depth = 50
	done := make(chan struct{})

	var next func(ctx *Context, remaining int)
	next = func(ctx *Context, remaining int) {
		if remaining == 0 {
			close(done)
			return
		}
		ctx.SpawnLocal(func(ctx *Context) { next(ctx, remaining-1) })
	}

	err := p.Submit(func(ctx *Context) { next(ctx, depth) })
	require.NoError(t, err)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.Fail(t, "spawn chain never completed")
	}
}

func TestPool_CloseWaitsForSpawnedDescendants(t *testing.T) {
	p, closePool := New(
		Config{
			NumWorkers: 4,
		},
		nil,
	)

	var leafRan atomic.Bool
	err := p.Submit(func(ctx *Context) {
		ctx.SpawnLocal(func(ctx *Context) {
			ctx.SpawnLocal(func(ctx *Context) {
				time.Sleep(20 * time.Millisecond) // give Close a window to (wrongly) race ahead
				leafRan.Store(true)
			})
		})
	})
	require.NoError(t, err)

	time.Sleep(5 * time.Millisecond) // let the chain start spawning before we close
	closePool()

	require.True(t, leafRan.Load(), "Close() returned before a task spawned by a spawned task finished")
}

func TestPool_PanicInTaskDoesNotStopThePool(t *testing.T) {
	p, closePool := New(
		Config{
			NumWorkers: 2,
		},
		nil,
	)
	defer closePool()

	err := p.Submit(func(ctx *Context) { panic("boom") })
	require.NoError(t, err)
	p.Wait()

	// Pool must still be usable after a task panicked.
	done := make(chan struct{})
	err = p.Submit(func(ctx *Context) { close(done) })
	require.NoError(t, err, "Submit after panic")

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.Fail(t, "pool stopped processing tasks after a panic")
	}
}

func TestPool_PanicHandlerReceivesDetails(t *testing.T) {
	gotWorker := -1
	var gotRecovered any
	var gotStackLen int
	handlerCalled := make(chan struct{})

	p, closePool := New(
		Config{
			NumWorkers: 2,
		},
		func(workerID int, recovered any, stack []byte) {
			gotWorker = workerID
			gotRecovered = recovered
			gotStackLen = len(stack)
			close(handlerCalled)
		},
	)
	defer closePool()

	err := p.Submit(func(ctx *Context) { panic("boom-details") })
	require.NoError(t, err)

	select {
	case <-handlerCalled:
	case <-time.After(2 * time.Second):
		require.Fail(t, "onPanic handler was never invoked")
	}

	assert.GreaterOrEqual(t, gotWorker, 0)
	assert.Less(t, gotWorker, p.NumWorkers())
	assert.Equal(t, "boom-details", gotRecovered)
	assert.NotZero(t, gotStackLen)
}

func TestPool_DefaultPanicHandlerLogsWithoutCrashing(t *testing.T) {
	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(orig)

	p, closePool := New(
		Config{
			NumWorkers: 1,
		},
		nil,
	)
	defer closePool()

	err := p.Submit(func(ctx *Context) { panic("default-handler-boom") })
	require.NoError(t, err)
	// exec()'s deferred tasksWg.Done() runs strictly after the deferred
	// recover+log, so by the time Wait() returns the log line is written.
	p.Wait()

	require.Containsf(
		t,
		buf.String(),
		"default-handler-boom",
		"expected default log output to mention the panic value, got: %q",
		buf.String(),
	)
}

func TestPool_SubmitAfterCloseReturnsErrPoolClosed(t *testing.T) {
	p, closePool := New(
		Config{
			NumWorkers: 2,
		},
		nil,
	)
	closePool()

	err := p.Submit(func(ctx *Context) {})
	require.Equal(t, ErrPoolClosed, err)
}

func TestPool_CloseIsIdempotentAndAllCallersReturn(t *testing.T) {
	_, closePool := New(
		Config{
			NumWorkers: 2,
		},
		nil,
	)
	closePool()
	closePool()
	closePool() // must not hang or panic on repeat calls
}

func TestPool_CloseWithNoWorkReturnsPromptly(t *testing.T) {
	_, closePool := New(
		Config{
			NumWorkers: 4,
		},
		nil,
	)
	done := make(chan struct{})
	go func() {
		closePool()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.Fail(t, "Close() with no submitted work took too long")
	}
}

func TestPool_WaitWithNoWorkReturnsImmediately(t *testing.T) {
	p, closePool := New(
		Config{
			NumWorkers: 4,
		},
		nil,
	)
	defer closePool()

	done := make(chan struct{})
	go func() {
		p.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		require.Fail(t, "Wait() with no submitted work took too long")
	}
}

func TestPool_WaitBlocksUntilTaskActuallyFinishes(t *testing.T) {
	p, closePool := New(
		Config{
			NumWorkers: 2,
		},
		nil,
	)
	defer closePool()

	release := make(chan struct{})
	started := make(chan struct{})
	err := p.Submit(func(ctx *Context) {
		close(started)
		<-release
	})
	require.NoError(t, err)
	<-started

	waitReturned := make(chan struct{})
	go func() {
		p.Wait()
		close(waitReturned)
	}()

	select {
	case <-waitReturned:
		require.Fail(t, "Wait() returned before the in-flight task finished")
	case <-time.After(50 * time.Millisecond):
		// expected: still blocked
	}

	close(release)
	select {
	case <-waitReturned:
	case <-time.After(2 * time.Second):
		require.Fail(t, "Wait() did not return after the task finished")
	}
}

func TestPool_NumWorkersMatchesRequested(t *testing.T) {
	p, closePool := New(
		Config{
			NumWorkers: 7,
		},
		nil,
	)
	defer closePool()

	got := p.NumWorkers()
	assert.Equal(t, 7, got)
}

func TestPool_NewNonPositiveDefaultsToGOMAXPROCS(t *testing.T) {
	for _, n := range []int{0, -1, -100} {
		p, closePool := New(
			Config{
				NumWorkers: n,
			},
			nil,
		)
		got := p.NumWorkers()
		assert.GreaterOrEqual(t, got, 1)
		closePool()
	}
}

func TestPool_ConcurrentSubmitFromManyGoroutines(t *testing.T) {
	p, closePool := New(
		Config{
			NumWorkers: 4,
		},
		nil,
	)
	defer closePool()

	const producers = 32
	const perProducer = 2000
	var executed atomic.Int64

	var wg sync.WaitGroup
	wg.Add(producers)
	for range producers {
		go func() {
			defer wg.Done()
			for range perProducer {
				err := p.Submit(func(ctx *Context) { executed.Add(1) })
				if !assert.NoError(t, err) {
					return
				}
			}
		}()
	}
	wg.Wait()
	p.Wait()

	want := int64(producers * perProducer)
	got := executed.Load()
	require.Equal(t, want, got)
}

func TestPool_Concurrent_SpawnLocalFromManyTasks(t *testing.T) {
	p, closePool := New(
		Config{
			NumWorkers: 4,
		},
		nil,
	)
	defer closePool()

	const n = 5000
	var executed atomic.Int64
	var wg sync.WaitGroup
	wg.Add(n * 2) // each top-level task + the one child it spawns

	for range n {
		err := p.Submit(func(ctx *Context) {
			executed.Add(1)
			wg.Done()
			ctx.SpawnLocal(func(ctx *Context) {
				executed.Add(1)
				wg.Done()
			})
		})
		require.NoError(t, err)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		require.Fail(t, "timed out waiting for all top-level + spawned tasks")
	}

	got := executed.Load()
	require.Equal(t, int64(n*2), got)
}

func TestPool_Concurrent_SubmitAndClose(t *testing.T) {
	const rounds = 300
	for round := range rounds {
		p, closePool := New(
			Config{
				NumWorkers: 4,
			},
			nil,
		)

		var executed atomic.Int64
		var wg sync.WaitGroup
		wg.Go(func() {
			for range 200 {
				err := p.Submit(func(ctx *Context) { executed.Add(1) })
				if err != nil && !errors.Is(err, ErrPoolClosed) {
					t.Errorf("round %d: unexpected Submit error: %v", round, err)
				}
			}
		})

		closePool() // races the goroutine above by construction
		wg.Wait()
		_ = executed.Load() // no assertion on the exact count: outcome depends on timing
	}
}

func TestPool_Concurrent_CloseCallersAllReturn(t *testing.T) {
	p, closePool := New(
		Config{
			NumWorkers: 3,
		},
		nil,
	)

	for range 200 {
		err := p.Submit(func(ctx *Context) {})
		require.NoError(t, err)
	}

	const callers = 20
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() {
			defer wg.Done()
			closePool()
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.Fail(t, "some concurrent Close() caller never returned")
	}
}

func TestPool_Concurrent_WaitCallersAllSeeCompletion(t *testing.T) {
	p, closePool := New(
		Config{
			NumWorkers: 4,
		},
		nil,
	)
	defer closePool()

	const n = 3000
	var executed atomic.Int64
	// Submit everything up front, sequentially, before any Wait() call —
	// Wait()'s documented contract only covers a fixed batch, not a
	// concurrently-still-growing one.
	for range n {
		err := p.Submit(func(ctx *Context) { executed.Add(1) })
		require.NoError(t, err)
	}

	const waiters = 8
	var wg sync.WaitGroup
	wg.Add(waiters)
	for range waiters {
		go func() {
			defer wg.Done()
			p.Wait()
			got := executed.Load()
			assert.Equal(t, int64(n), got)
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.Fail(t, "some concurrent Wait() caller never returned")
	}
}

func TestPool_Concurrent_StealDistributesWorkAcrossWorkers(t *testing.T) {
	p, closePool := New(
		Config{
			NumWorkers: 4,
		},
		nil,
	)
	defer closePool()

	const n = 20000
	var mu sync.Mutex
	seen := make(map[int]int)
	var wg sync.WaitGroup
	wg.Add(n)

	for range n {
		err := p.Submit(func(ctx *Context) {
			defer wg.Done()
			mu.Lock()
			seen[ctx.w.id]++
			mu.Unlock()
		})
		require.NoError(t, err)
	}
	wg.Wait()

	assert.GreaterOrEqual(t, len(seen), 2)
	t.Logf("per-worker task counts: %v", seen)
}

func TestPool_Concurrent_NoGoroutineLeakAfterClose(t *testing.T) {
	baseline := runtime.NumGoroutine()

	p, closePool := New(
		Config{
			NumWorkers: 6,
		},
		nil,
	)

	for range 500 {
		err := p.Submit(func(ctx *Context) {})
		require.NoError(t, err)
	}
	closePool()

	deadline := time.Now().Add(2 * time.Second)
	for {
		n := runtime.NumGoroutine()
		if n <= baseline {
			return
		}
		assert.Falsef(
			t,
			time.Now().After(deadline),
			"goroutine leak after Close: baseline=%d, now=%d", baseline, n,
		)
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPool_Stress_HighVolumeSubmit(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}

	p, closePool := New(
		Config{
			NumWorkers: 8,
		},
		nil,
	)
	defer closePool()

	const n = 300_000
	var executed atomic.Int64
	for range n {
		err := p.Submit(func(ctx *Context) { executed.Add(1) })
		require.NoError(t, err)
	}
	p.Wait()

	got := executed.Load()
	require.Equal(t, int64(n), got)
}

func TestPool_Stress_ManyProducersHighVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}

	p, closePool := New(
		Config{
			NumWorkers: 8,
		},
		nil,
	)
	defer closePool()

	const producers = 16
	const perProducer = 15_000
	var executed atomic.Int64

	done := make(chan struct{}, producers)
	for range producers {
		go func() {
			defer func() { done <- struct{}{} }()
			for range perProducer {
				if err := p.Submit(func(ctx *Context) { executed.Add(1) }); err != nil {
					t.Errorf("Submit: %v", err)
					return
				}
			}
		}()
	}
	for range producers {
		<-done
	}
	p.Wait()

	want := int64(producers * perProducer)
	got := executed.Load()
	require.Equal(t, want, got)
}

// TestStressRandomSpawnDepth builds, per top-level submission, a randomly
// shaped tree of SpawnLocal children (depth <= 3, 0-2 children per node)
// and verifies every node in every tree actually ran — independent
// confirmation of tasksWg's bookkeeping via an external WaitGroup that
// mirrors the same Add-before-enqueue / Done-after-execute discipline.
func TestPool_Stress_RandomSpawnDepth(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}

	p, closePool := New(
		Config{
			NumWorkers: 8,
		},
		nil,
	)
	defer closePool()

	const topLevel = 4000
	var executed atomic.Int64
	done := make(chan struct{}, topLevel*8) // generous upper bound on tree size

	var spawn func(ctx *Context, depthLeft int)
	spawn = func(ctx *Context, depthLeft int) {
		executed.Add(1)
		done <- struct{}{}
		if depthLeft <= 0 {
			return
		}
		children := rand.IntN(3) // 0, 1, or 2
		for range children {
			d := depthLeft - 1
			ctx.SpawnLocal(func(ctx *Context) { spawn(ctx, d) })
		}
	}

	for range topLevel {
		sErr := p.Submit(func(ctx *Context) { spawn(ctx, 3) })
		require.NoError(t, sErr)
	}

	p.Wait()
	got := executed.Load()
	require.GreaterOrEqualf(
		t,
		got,
		int64(topLevel),
		"executed = %d, want at least %d (one per top-level submission)", got, topLevel,
	)
	require.Equalf(
		t,
		int64(len(done)),
		got,
		"done signals = %d, executed counter = %d, should match exactly", len(done), got,
	)
	t.Logf("spawn tree total nodes executed: %d (top-level: %d)", got, topLevel)
}

func TestPool_Stress_SmallPoolForcesOverflowAndSteal(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}

	// deliberately small: local queues will overflow constantly
	p, closePool := New(
		Config{
			NumWorkers: 2,
		},
		nil,
	)
	defer closePool()

	const n = capacity * 20 // capacity is local_queue.go's per-worker size
	var executed atomic.Int64
	for range n {
		err := p.Submit(func(ctx *Context) { executed.Add(1) })
		require.NoError(t, err)
	}
	p.Wait()

	got := executed.Load()
	require.Equal(t, int64(n), got)
}

func TestPool_Stress_PanicsInterleavedWithNormalTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}

	// Swallow the (expected, high-volume) panic logs so test output stays
	// readable; still sanity-check the log saw at least one of them.
	var buf bytes.Buffer
	orig := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(orig)

	p, closePool := New(
		Config{
			NumWorkers: 6,
		},
		nil,
	)
	defer closePool()

	const n = 50_000
	var normalDone atomic.Int64
	var wantNormal int64
	for i := range n {
		panics := i%7 == 0
		if !panics {
			wantNormal++
		}
		sErr := p.Submit(func(ctx *Context) {
			if panics {
				panic("stress-induced panic")
			}
			normalDone.Add(1)
		})
		require.NoError(t, sErr)
	}
	p.Wait()

	got := normalDone.Load()
	require.Equalf(
		t,
		wantNormal,
		got,
		"normalDone = %d, want %d (panicking tasks must not affect the rest)", got, wantNormal,
	)
	require.NotEmpty(t, buf)
}

func TestPool_Stress_RepeatedPoolLifecycle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in -short mode")
	}
	baseline := runtime.NumGoroutine()

	const rounds = 200
	const perRound = 300
	for range rounds {
		p, closePool := New(
			Config{
				NumWorkers: 4,
			},
			nil,
		)

		var executed atomic.Int64
		for range perRound {
			err := p.Submit(func(ctx *Context) { executed.Add(1) })
			require.NoError(t, err)
		}
		closePool()
		got := executed.Load()
		require.Equal(t, int64(perRound), got)
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		n := runtime.NumGoroutine()
		if n <= baseline {
			return
		}
		require.Falsef(
			t,
			time.Now().After(deadline),
			"goroutine count grew across %d pool lifecycles: baseline=%d, now=%d", rounds, baseline, n,
		)
		time.Sleep(10 * time.Millisecond)
	}
}

// ----------
// Benchmarks
// ----------

func BenchmarkSubmitSequential(b *testing.B) {
	p, closePool := New(
		Config{
			NumWorkers: runtime.GOMAXPROCS(0),
		},
		nil,
	)
	defer closePool()

	var executed atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Submit(func(ctx *Context) { executed.Add(1) })
	}
	b.StopTimer()
	p.Wait()
}

func BenchmarkSubmitParallel(b *testing.B) {
	p, closePool := New(
		Config{
			NumWorkers: runtime.GOMAXPROCS(0),
		},
		nil,
	)
	defer closePool()

	var executed atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = p.Submit(func(ctx *Context) { executed.Add(1) })
		}
	})
	b.StopTimer()
	p.Wait()
}
