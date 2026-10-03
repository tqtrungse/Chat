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
	"context"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// waitWithTimeout waits for wg to finish, but gives up after timeout instead
// of blocking forever. The wrapped goroutine leaks if it fires, but that's
// preferable to a silent multi-minute `go test` timeout with zero context.
func waitWithTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// logSeenSummary prints a small sample of missing/duplicated task ids so a
// failure is actionable instead of just "it timed out".
func logSeenSummary(t *testing.T, seen []atomic.Int32, total int) {
	t.Helper()
	missing, dup := 0, 0
	for i := range total {
		switch c := seen[i].Load(); {
		case c == 0:
			if missing < 10 {
				t.Logf("missing task id=%d", i)
			}
			missing++
		case c > 1:
			if dup < 10 {
				t.Logf("duplicated task id=%d count=%d", i, c)
			}
			dup++
		}
	}
	t.Logf("summary: %d missing, %d duplicated (out of %d)", missing, dup, total)
}

// -----------------
// Basic correctness
// -----------------

func TestElastic_PopBatch_InvalidMax(t *testing.T) {
	q := newElastic[int]()
	q.Push(1)

	out, ok := q.PopBatch(0)
	require.False(t, ok)
	require.Nil(t, out)

	out, ok = q.PopBatch(-5)
	require.False(t, ok)
	require.Nil(t, out)
}

func TestElastic_PopBatch_EmptyQueue(t *testing.T) {
	q := newElastic[int]()
	out, ok := q.PopBatch(10)
	require.False(t, ok)
	require.Nil(t, out)
}

func TestElastic_SPSC_PreservesFIFOOrder(t *testing.T) {
	q := newElastic[int]()
	const n = 10_000

	for i := range n {
		q.Push(i)
	}

	got := make([]int, 0, n)
	for len(got) < n {
		// 37 is not a divisor of segmentSize (64) — deliberately
		// misaligns batches against segment boundaries.
		batch, ok := q.PopBatch(37)
		require.True(t, ok)
		got = append(got, batch...)
	}

	for i, v := range got {
		require.Equal(t, v, i)
	}
}

// -----------------------------------------------
// Segment-boundary edge cases (segmentSize == 64)
// -----------------------------------------------

func TestElastic_ExactSegmentBoundary(t *testing.T) {
	q := newElastic[int]()
	for i := range segmentSize {
		q.Push(i)
	}

	batch, ok := q.PopBatch(segmentSize)
	require.True(t, ok)
	require.Len(t, batch, segmentSize)

	for i, v := range batch {
		require.Equal(t, i, v)
	}

	_, ok = q.PopBatch(1)
	require.False(t, ok)
}

func TestElastic_SpansMultipleSegments(t *testing.T) {
	q := newElastic[int]()
	const n = segmentSize*3 + 7 // forces growth across 4 segments, last one partial

	for i := range n {
		q.Push(i)
	}

	total := 0
	for total < n {
		batch, ok := q.PopBatch(segmentSize / 2) // smaller than a segment, crosses boundaries repeatedly
		require.True(t, ok)

		for _, v := range batch {
			require.Equal(t, total, v)
			total++
		}
	}
	_, ok := q.PopBatch(1)
	require.Falsef(t, ok, "expected empty queue after draining all %d items", n)
}

func TestElastic_PopBatch_LargerThanAvailable(t *testing.T) {
	q := newElastic[int]()
	for i := range 5 {
		q.Push(i)
	}
	batch, ok := q.PopBatch(1000)
	require.True(t, ok)
	require.Len(t, batch, 5)
}

// ------------------------------------------------------------------
// White-box: read() must not leak the popped value (GC-friendliness)
// ------------------------------------------------------------------

func TestElastic_ReadZeroesSlotAfterPop(t *testing.T) {
	type big struct{ n int }
	q := newElastic[*big]()
	v := &big{n: 42}
	q.Push(v)

	head := q.head.Load() // white-box: package-internal access
	batch, ok := q.PopBatch(1)
	require.True(t, ok)
	require.Len(t, batch, 1)
	require.Equal(t, v, batch[0])
	require.Nilf(t, head.buf[0].Value, "slot 0 still holds a reference after pop")
}

// ---------------------------------------------------------------------
// Regression test for the head-advance-past-open-segment race fixed in
// PopBatch (stale tryClaimPopN snapshot read before the segment closes,
// followed by CAS'ing head past it). Deliberately small so it's cheap
// to hammer for flakes, e.g.:
//
//	go test -run NoLossAtSegmentTransition -race -count=200
// ---------------------------------------------------------------------

func TestElastic_NoLossAtSegmentTransition_Regression(t *testing.T) {
	const (
		numProducers     = 8
		numConsumers     = 8
		tasksPerProducer = 2000
		totalTasks       = numProducers * tasksPerProducer
	)

	q := newElastic[int]()
	seen := make([]atomic.Int32, totalTasks)
	var uniqueCount atomic.Int64

	var wgConsumers sync.WaitGroup
	for range numConsumers {
		wgConsumers.Go(func() {
			for uniqueCount.Load() < totalTasks {
				// 128 > segmentSize (64): widens the snapshot-to-CAS
				// window that the fix closes.
				batch, ok := q.PopBatch(128)
				if !ok {
					continue
				}
				for _, item := range batch {
					if seen[item].Add(1) == 1 {
						uniqueCount.Add(1)
					}
				}
			}
		})
	}

	var wgProducers sync.WaitGroup
	for i := range numProducers {
		wgProducers.Add(1)
		go func(pID int) {
			defer wgProducers.Done()
			for j := range tasksPerProducer {
				q.Push(pID*tasksPerProducer + j)
			}
		}(i)
	}
	wgProducers.Wait()

	require.Truef(
		t,
		waitWithTimeout(&wgConsumers, 10*time.Second),
		"consumers stuck (unique=%d/%d)", uniqueCount.Load(), totalTasks,
	)

	_, ok := q.PopBatch(1)
	require.False(t, ok, "items left in queue after all consumers reported completion")

	for i := range totalTasks {
		c := seen[i].Load()
		require.NotZerof(t, c, "DATA LOSS: task %d never popped", i)
		require.LessOrEqualf(t, c, int32(1), "DATA DUPLICATION: task %d popped %d times", i, c)
	}
}

func TestElastic_Stress(t *testing.T) {
	q := newElastic[int]()

	const numProducers = 12
	const numConsumers = 12
	const tasksPerProducer = 50_000
	const totalTasks = numProducers * tasksPerProducer

	// seen[id] tracks how many times a given task ID was popped.
	// Every entry must end up at exactly 1: 0 means lost, >1 means duplicated.
	seen := make([]atomic.Int32, totalTasks)

	var totalPopped atomic.Int64 // raw count of items popped — may exceed totalTasks if there's a duplication bug
	var uniqueCount atomic.Int64 // count of DISTINCT ids popped at least once — the real completion signal

	// Watchdog: if the queue loses an item, uniqueCount can never reach
	// totalTasks and a naive "loop until totalPopped==totalTasks" consumer
	// would spin forever, turning a real bug into a 10-minute unexplained
	// `go test` timeout instead of a fast, diagnosable failure.
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	var wgProducers sync.WaitGroup
	var wgConsumers sync.WaitGroup

	// Start consumers first so they're ready and waiting.
	for range numConsumers {
		wgConsumers.Go(func() {
			for {
				// Stop once every distinct id has been observed at least
				// once. Using uniqueCount (not totalPopped) here matters:
				// if some id were ever delivered twice, totalPopped could
				// reach totalTasks while a *different* id was never
				// delivered at all, masking a real data-loss bug.
				if uniqueCount.Load() >= totalTasks {
					return
				}
				select {
				case <-ctx.Done():
					return
				default:
				}

				batch, ok := q.PopBatch(128)
				if !ok {
					runtime.Gosched()
					continue
				}
				for _, item := range batch {
					if seen[item].Add(1) == 1 {
						uniqueCount.Add(1)
					}
				}
				totalPopped.Add(int64(len(batch)))
			}
		})
	}

	for i := range numProducers {
		wgProducers.Add(1)
		go func(pID int) {
			defer wgProducers.Done()
			for j := range tasksPerProducer {
				taskID := pID*tasksPerProducer + j
				q.Push(taskID)
			}
		}(i)
	}

	require.True(
		t, waitWithTimeout(&wgProducers, 30*time.Second),
		"producers did not finish within timeout — Push appears to be stuck",
	)

	// Give consumers a bit longer than the context deadline so they have
	// time to notice ctx.Done() and return cleanly.
	if !waitWithTimeout(&wgConsumers, 60*time.Second) {
		logSeenSummary(t, seen, totalTasks)
		require.Failf(
			t,
			"consumers did not finish within timeout",
			`(popped=%d unique=%d/%d): PopBatch/read appears to be stuck (e.g. spinning on an entry that was claimed but never published)`, totalPopped.Load(), uniqueCount.Load(), totalTasks,
		)
	}

	if ctx.Err() != nil {
		// Consumers exited via the watchdog, not because they finished.
		logSeenSummary(t, seen, totalTasks)
		require.Failf(
			t,
			"watchdog deadline exceeded",
			"unique=%d/%d popped=%d — items were likely lost", uniqueCount.Load(), totalTasks, totalPopped.Load(),
		)
	}

	// Nothing should remain in the queue at this point; a leftover item
	// here would mean consumers stopped early while real items still sat
	// unclaimed (a symptom distinct from plain loss/duplication above).
	extra, ok := q.PopBatch(1)
	require.Falsef(t, ok, "queue still had items after consumers completed: %+v", extra)

	for i := range totalTasks {
		count := seen[i].Load()
		require.NotZerof(t, count, "DATA LOSS: task %d was never popped", i)
		require.LessOrEqualf(t, count, int32(1), "DATA DUPLICATION: task %d was popped %d times", i, count)
	}

	require.Equal(t, totalTasks, int(totalPopped.Load()), "total popped mismatch")
}

// -----------------------------
// PushBatch: basic correctness
// -----------------------------

func TestElastic_PushBatch_Empty(t *testing.T) {
	q := newElastic[int]()

	q.PushBatch(nil)
	q.PushBatch([]int{})

	out, ok := q.PopBatch(10)
	require.False(t, ok)
	require.Nil(t, out)

	// Queue must still work normally afterwards — an empty PushBatch
	// shouldn't have left sendIdx/tail in a weird state.
	q.Push(42)
	batch, ok := q.PopBatch(1)
	require.True(t, ok)
	require.Len(t, batch, 1)
	require.Equal(t, 42, batch[0])
}

func TestElastic_PushBatch_PreservesOrder(t *testing.T) {
	q := newElastic[int]()
	const n = 10_000

	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	q.PushBatch(items)

	got := make([]int, 0, n)
	for len(got) < n {
		// 37 is not a divisor of segmentSize (64) — same deliberate
		// misalignment as the existing single-Push SPSC order test.
		batch, ok := q.PopBatch(37)
		require.Truef(t, ok, "queue drained early at %d/%d items", len(got), n)
		got = append(got, batch...)
	}
	for i, v := range got {
		require.Equalf(t, i, v, "order broken at position %d", i)
	}
}

// ---------------------------------
// PushBatch: segment-boundary cases
// (segmentSize == 64)
// ---------------------------------

func TestElastic_PushBatch_ExactSegmentBoundary(t *testing.T) {
	q := newElastic[int]()

	items := make([]int, segmentSize)
	for i := range items {
		items[i] = i
	}
	q.PushBatch(items)

	batch, ok := q.PopBatch(segmentSize)
	require.True(t, ok)
	require.Len(t, batch, segmentSize)
	for i, v := range batch {
		require.Equalf(t, i, v, "order broken at %d", i)
	}
	_, ok = q.PopBatch(1)
	require.Falsef(t, ok, "expected empty queue after draining exactly one full segment")
}

func TestElastic_PushBatch_SpansMultipleSegments(t *testing.T) {
	q := newElastic[int]()
	const n = segmentSize*3 + 7 // one PushBatch call, forces tail growth across 4 segments, last one partial

	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	q.PushBatch(items)

	total := 0
	for total < n {
		batch, ok := q.PopBatch(segmentSize / 2) // smaller than a segment, crosses boundaries repeatedly
		require.Truef(t, ok, "drained early at %d/%d", total, n)
		for _, v := range batch {
			require.Equalf(t, total, v, "order broken at %d", total)
			total++
		}
	}
	_, ok := q.PopBatch(1)
	require.Falsef(t, ok, "expected empty queue after draining all %d items", n)
}

// TestElastic_PushBatch_SingleCallSpansManySegments pushes a much larger
// batch in one call (~11 segments' worth), stressing PushBatch's own
// internal loop making many consecutive growthTail calls within a
// single invocation, not just across several Push calls.
func TestElastic_PushBatch_SingleCallSpansManySegments(t *testing.T) {
	q := newElastic[int]()
	const n = segmentSize*10 + 13

	items := make([]int, n)
	for i := range items {
		items[i] = i
	}
	q.PushBatch(items)

	got := make([]int, 0, n)
	for len(got) < n {
		batch, ok := q.PopBatch(segmentSize + 1) // misaligned, bigger than one segment
		require.Truef(t, ok, "drained early at %d/%d", len(got), n)
		got = append(got, batch...)
	}
	for i, v := range got {
		require.Equalf(t, i, v, "order broken at %d", i)
	}
}

// TestElastic_PushBatch_MixedWithPush interleaves single Push calls with
// PushBatch calls of varying sizes — including ones landing exactly at
// or straddling a segment boundary — to check both paths share
// sendIdx/segment growth correctly with each other, not just internally
// consistent on their own.
func TestElastic_PushBatch_MixedWithPush(t *testing.T) {
	q := newElastic[int]()

	next := 0
	want := make([]int, 0, 200)

	pushOne := func() {
		q.Push(next)
		want = append(want, next)
		next++
	}
	pushBatch := func(n int) {
		batch := make([]int, n)
		for i := range batch {
			batch[i] = next
			next++
		}
		q.PushBatch(batch)
		want = append(want, batch...)
	}

	for range 5 {
		pushOne()
	}
	pushBatch(segmentSize - 5)  // fills out the rest of the first segment exactly
	pushOne()                   // first item of the second segment
	pushBatch(segmentSize + 10) // spans from mid-second-segment into a third
	for range 3 {
		pushOne()
	}

	got := make([]int, 0, len(want))
	for len(got) < len(want) {
		batch, ok := q.PopBatch(23) // misaligned against segmentSize on purpose
		require.Truef(t, ok, "drained early at %d/%d", len(got), len(want))
		got = append(got, batch...)
	}

	require.Equal(t, len(want), len(got), "length mismatch")
	for i := range want {
		require.Equalf(t, want[i], got[i], "order broken at %d", i)
	}
}

// ---------------------------------
// PushBatch: concurrent correctness
// ---------------------------------

// TestElastic_PushBatch_ConcurrentProducers_NoLossOrDup runs many
// producers each pushing a sequence of randomly-sized batches (larger
// than segmentSize, so most batches straddle a boundary) against many
// consumers, checking exactly-once delivery. Batches — and the ID
// assignment — are precomputed up front so the expected total is known
// without any shared counter racing with the goroutines under test.
func TestElastic_PushBatch_ConcurrentProducers_NoLossOrDup(t *testing.T) {
	const (
		numProducers       = 12
		numConsumers       = 12
		batchesPerProducer = 500
		maxBatchSize       = 200 // > segmentSize on purpose
	)

	q := newElastic[int]()

	rng := rand.New(rand.NewSource(1))
	producerBatches := make([][][]int, numProducers)
	nextID := 0
	for p := range numProducers {
		for range batchesPerProducer {
			size := rng.Intn(maxBatchSize) + 1
			batch := make([]int, size)
			for i := range batch {
				batch[i] = nextID
				nextID++
			}
			producerBatches[p] = append(producerBatches[p], batch)
		}
	}
	totalTasks := nextID

	seen := make([]atomic.Int32, totalTasks)
	var uniqueCount atomic.Int64

	var wgConsumers sync.WaitGroup
	for range numConsumers {
		wgConsumers.Go(func() {
			for uniqueCount.Load() < int64(totalTasks) {
				batch, ok := q.PopBatch(128)
				if !ok {
					continue
				}
				for _, item := range batch {
					if seen[item].Add(1) == 1 {
						uniqueCount.Add(1)
					}
				}
			}
		})
	}

	var wgProducers sync.WaitGroup
	for p := range numProducers {
		wgProducers.Add(1)
		go func(batches [][]int) {
			defer wgProducers.Done()
			for _, batch := range batches {
				q.PushBatch(batch)
			}
		}(producerBatches[p])
	}
	require.True(
		t, waitWithTimeout(&wgProducers, 30*time.Second),
		"producers did not finish within timeout — PushBatch appears to be stuck",
	)

	if !waitWithTimeout(&wgConsumers, 20*time.Second) {
		logSeenSummary(t, seen, totalTasks)
		require.Failf(t, "consumers stuck", "unique=%d/%d", uniqueCount.Load(), totalTasks)
	}

	_, ok := q.PopBatch(1)
	require.False(t, ok, "items left in queue after all consumers reported completion")

	for i := range totalTasks {
		c := seen[i].Load()
		require.NotZerof(t, c, "DATA LOSS: task %d never popped", i)
		require.LessOrEqualf(t, c, int32(1), "DATA DUPLICATION: task %d popped %d times", i, c)
	}
}

// TestElastic_PushBatch_SimultaneousGrowthTail_Regression deliberately
// fills a segment to exactly one slot short, then has many goroutines
// race to PushBatch a single item each at the exact same instant. All
// but one of them observe the same full tail segment and call
// growthTail(tail) concurrently — this is the narrow window around the
// double-checked lock in growthTail, isolated from the noise of a big
// diffuse stress test so it's cheap to hammer for flakes:
//
//	go test -run SimultaneousGrowthTail -race -count=200
func TestElastic_PushBatch_SimultaneousGrowthTail_Regression(t *testing.T) {
	const racers = 64

	q := newElastic[int]()
	for i := range segmentSize - 1 {
		q.Push(i)
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for id := range racers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			q.PushBatch([]int{segmentSize - 1 + id})
		}(id)
	}
	close(start)

	require.True(
		t,
		waitWithTimeout(&wg, 10*time.Second),
		"producers stuck — suspect a growthTail deadlock/livelock",
	)

	wantTotal := (segmentSize - 1) + racers // values [0, wantTotal) each exactly once
	seen := make([]bool, wantTotal)
	got := 0
	for got < wantTotal {
		batch, ok := q.PopBatch(37) // misaligned on purpose
		require.Truef(t, ok, "drained early at %d/%d", got, wantTotal)
		for _, v := range batch {
			require.Truef(t, v >= 0 && v < wantTotal, "value %d out of expected range [0,%d)", v, wantTotal)
			require.Falsef(t, seen[v], "DATA DUPLICATION: value %d seen twice", v)
			seen[v] = true
			got++
		}
	}
	_, ok := q.PopBatch(1)
	require.False(t, ok, "items left in queue after draining expected total")
}

// ----------
// Benchmarks
// ----------

func BenchmarkElastic_SPSC(b *testing.B) {
	q := newElastic[int]()
	done := make(chan struct{})
	start := make(chan struct{})

	go func() {
		<-start
		got := 0
		for got < b.N {
			batch, ok := q.PopBatch(128)
			if !ok {
				continue
			}
			got += len(batch)
		}
		close(done)
	}()

	b.ResetTimer()
	close(start)
	for i := 0; i < b.N; i++ {
		q.Push(i)
	}
	<-done
}

func BenchmarkElastic_MPMC(b *testing.B) {
	const producers = 4
	q := newElastic[int]()
	var produced atomic.Int64
	var consumed atomic.Int64
	start := make(chan struct{})
	done := make(chan struct{})

	for range producers {
		go func() {
			<-start
			for {
				n := produced.Add(1) - 1
				if n >= int64(b.N) {
					return
				}
				q.Push(int(n))
			}
		}()
	}
	go func() {
		<-start
		for consumed.Load() < int64(b.N) {
			batch, ok := q.PopBatch(128)
			if !ok {
				continue
			}
			consumed.Add(int64(len(batch)))
		}
		close(done)
	}()

	b.ResetTimer()
	close(start)
	<-done
}
