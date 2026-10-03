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
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"xxx/pkg"
)

func newTestLocal() *local[int] {
	return newLocal[int]()
}

func drainLocal(l *local[int]) []int {
	out := make([]int, 0, l.Len())

	for {
		v, ok := l.Pop()
		if !ok {
			return out
		}
		out = append(out, v)
	}
}

func assertIntsEqual(t *testing.T, got, want []int) {
	t.Helper()
	require.Equal(t, want, got)
}

func popAllElastic[V any](q *elastic[V]) []V {
	var out []V

	for {
		items, ok := q.PopBatch(256)
		if !ok {
			return out
		}

		out = append(out, items...)
	}
}

// -----------
// Basic logic
// -----------

func TestLocal_Empty(t *testing.T) {
	l := newTestLocal()

	require.Zero(t, l.Len())
	require.Equal(t, capacity, l.RemainingSlots())

	_, ok := l.Pop()
	require.False(t, ok, "Pop() on empty queue returned ok=true")
}

func TestLocal_PushPopFIFO(t *testing.T) {
	l := newTestLocal()
	e := newElastic[int]()

	const n = 100

	for i := range n {
		l.Push(i, e)
	}

	require.Equal(t, n, l.Len())
	require.Equal(t, capacity-n, l.RemainingSlots())

	for i := range n {
		got, ok := l.Pop()
		require.Truef(t, ok, "Pop() failed at i=%d", i)
		require.Equal(t, i, got)
	}

	require.Zero(t, l.Len(), "Len() after drain")
}

func TestLocal_PushBatchNoOverflow(t *testing.T) {
	l := newTestLocal()

	tasks := make([]int, 100)
	for i := range tasks {
		tasks[i] = i
	}

	l.PushBatchNoOverflow(tasks)

	require.Equal(t, len(tasks), l.Len())

	got := drainLocal(l)
	assertIntsEqual(t, got, tasks)
}

func TestLocal_PushBatchNoOverflow_Empty(t *testing.T) {
	l := newTestLocal()

	l.PushBatchNoOverflow(nil)
	l.PushBatchNoOverflow([]int{})

	require.Zero(t, l.Len())
}

func TestLocal_PushBatchNoOverflow_ExactCapacity(t *testing.T) {
	l := newTestLocal()

	tasks := make([]int, capacity)
	for i := range tasks {
		tasks[i] = i
	}

	l.PushBatchNoOverflow(tasks)

	require.Equal(t, capacity, l.Len())

	got := drainLocal(l)
	assertIntsEqual(t, got, tasks)
}

// ---------------
// Panic contracts
// ---------------

func TestLocal_PushBatchNoOverflow_TooLargePanics(t *testing.T) {
	l := newTestLocal()

	tasks := make([]int, capacity+1)

	require.Panics(t, func() {
		l.PushBatchNoOverflow(tasks)
	})
}

func TestLocal_PushBatchNoOverflow_NotEnoughRoomPanics(t *testing.T) {
	l := newTestLocal()

	tasks := make([]int, capacity)
	l.PushBatchNoOverflow(tasks)

	require.Panics(t, func() {
		l.PushBatchNoOverflow([]int{1})
	})
}

// -----------
// Wrap-around
// -----------

func TestLocal_WrapAround(t *testing.T) {
	l := newTestLocal()
	e := newElastic[int]()

	// Fill the queue completely so every physical slot [0, capacity) is
	// written at least once.
	for i := range capacity {
		l.Push(i, e)
	}

	// Free up a quarter of the queue so the following pushes must land
	// at logical index >= capacity, i.e. they alias — and must
	// correctly reuse — physical slots [0, drain), the exact slots just
	// freed. This is the actual wraparound this test is meant to catch;
	// the previous version never pushed past logical index `capacity`,
	// so no physical slot was ever written twice.
	const drain = capacity / 4

	for i := range drain {
		got, ok := l.Pop()
		require.Truef(t, ok, "Pop() failed at i=%d", i)
		require.Equal(t, i, got)
	}

	for i := range drain {
		l.Push(capacity+i, e)
	}

	require.Equal(t, capacity, l.Len())

	got := drainLocal(l)

	// The live range is now a contiguous run starting at `drain`: the
	// untouched middle (drain..capacity-1) followed by the values that
	// just wrapped into the freed slots (capacity..capacity+drain-1).
	want := make([]int, capacity)
	for i := range want {
		want[i] = drain + i
	}

	assertIntsEqual(t, got, want)
}

func TestLocal_MultipleWrapArounds(t *testing.T) {
	l := newTestLocal()
	e := newElastic[int]()

	// Enough rounds that total pushes clear several multiples of
	// capacity, so the ring buffer's physical slots each get reused
	// many times over. The previous version ran only 20 rounds and
	// topped out at 210 total pushes — under one lap of a 256-slot
	// buffer — so it never actually exercised slot reuse despite its
	// name.
	const rounds = 2000

	var expected []int
	next := 0

	for round := range rounds {
		n := 1 + round%64 // stays well under capacity

		for range n {
			l.Push(next, e)
			expected = append(expected, next)
			next++
		}

		// Keep the live backlog small (bounded well under capacity) so
		// this test only ever exercises ring-buffer wraparound, never
		// the overflow path — that has its own tests. Leaving a small
		// residual instead of always draining to zero keeps push/pop
		// interleaved across the wrap boundary rather than always
		// landing on it exactly.
		popN := len(expected)
		if popN > 8 {
			popN -= 8
		}

		for range popN {
			got, ok := l.Pop()
			require.Truef(t, ok, "Pop() failed in round %d", round)
			require.Equalf(t, expected[0], got, "round %d", round)

			expected = expected[1:]
		}
	}

	// Self-check: guard against this test silently regressing back to
	// not actually wrapping, the way the version it replaced did.
	require.GreaterOrEqualf(
		t,
		next,
		capacity*20,
		"test invariant broken: only pushed %d items total, want at least %d to exercise several ring wraps",
		next, capacity*20,
	)

	got := drainLocal(l)
	assertIntsEqual(t, got, expected)
}

// -----------------
// MoveTo / stealing
// -----------------
//
// Contract (see moveInto2/MoveTo in local_queue.go): MoveTo reserves the
// oldest ceil(avail/2) tasks from src. Of that reserved batch, the
// *newest* task (the one bordering what src keeps) is handed back
// directly to the caller to run immediately; the remaining older tasks
// in the batch are published into dst, oldest-first. This is the
// opposite of "return the oldest stolen task" — it lets the thief run
// the freshest work now while banking the colder backlog for later.

func TestLocal_MoveTo_Empty(t *testing.T) {
	src := newTestLocal()
	dst := newTestLocal()

	_, ok := src.MoveTo(dst)
	require.False(t, ok, "MoveTo() from empty queue returned ok=true")

	require.Zero(t, src.Len(), "src Len()")
	require.Zero(t, dst.Len(), "dst Len()")
}

func TestLocal_MoveTo_One(t *testing.T) {
	src := newTestLocal()
	dst := newTestLocal()
	e := newElastic[int]()

	src.Push(42, e)

	got, ok := src.MoveTo(dst)
	require.True(t, ok, "MoveTo() returned ok=false")
	require.Equal(t, 42, got)

	require.Zero(t, src.Len(), "src Len()")
	require.Zero(t, dst.Len(), "dst Len()")
}

func TestLocal_MoveTo_Even(t *testing.T) {
	src := newTestLocal()
	dst := newTestLocal()
	e := newElastic[int]()

	const n = 100

	for i := range n {
		src.Push(i, e)
	}

	// take = ceil(n/2): the oldest `take` tasks are reserved and copied
	// out of src. Of those, the *newest* one (the one adjacent to what
	// src keeps) is handed back directly to the caller; the remaining
	// take-1 (the older ones) are published into dst, oldest-first.
	const take = n - n/2 // 50

	stolen, ok := src.MoveTo(dst)
	require.True(t, ok, "MoveTo() returned ok=false")
	require.Equal(t, take-1, stolen, "returned task")

	require.Equal(t, n-take, src.Len(), "src Len()")
	require.Equal(t, take-1, dst.Len(), "dst Len()")

	// Source keeps the second (newer) half.
	srcGot := drainLocal(src)

	wantSrc := make([]int, n-take)
	for i := range wantSrc {
		wantSrc[i] = take + i
	}

	assertIntsEqual(t, srcGot, wantSrc)

	// Destination gets the older take-1 stolen tasks, oldest-first; the
	// newest of the stolen batch was returned directly instead.
	dstGot := drainLocal(dst)

	wantDst := make([]int, take-1)
	for i := range wantDst {
		wantDst[i] = i
	}

	assertIntsEqual(t, dstGot, wantDst)
}

func TestLocal_MoveTo_Odd(t *testing.T) {
	src := newTestLocal()
	dst := newTestLocal()
	e := newElastic[int]()

	const n = 101

	for i := range n {
		src.Push(i, e)
	}

	// take = ceil(101/2) = 51. One (the newest of the stolen batch) is
	// returned directly, the other 50 (older) are published into dst.
	const take = n - n/2 // 51

	stolen, ok := src.MoveTo(dst)
	require.True(t, ok, "MoveTo() returned ok=false")
	require.Equal(t, take-1, stolen, "returned task")

	require.Equal(t, n-take, src.Len(), "src Len()")
	require.Equal(t, take-1, dst.Len(), "dst Len()")

	srcGot := drainLocal(src)
	dstGot := drainLocal(dst)

	wantSrc := make([]int, n-take)
	for i := range wantSrc {
		wantSrc[i] = take + i
	}

	wantDst := make([]int, take-1)
	for i := range wantDst {
		wantDst[i] = i
	}

	assertIntsEqual(t, srcGot, wantSrc)
	assertIntsEqual(t, dstGot, wantDst)
}

func TestLocal_MoveTo_Repeated(t *testing.T) {
	src := newTestLocal()
	dst := newTestLocal()
	e := newElastic[int]()

	for i := range 128 {
		src.Push(i, e)
	}

	var stolen []int

	for {
		item, ok := src.MoveTo(dst)
		if !ok {
			break
		}

		stolen = append(stolen, item)

		// The destination can fill up. Drain it as its owner would —
		// and record what comes out, so this test can still account
		// for every task if a future change makes this loop actually
		// run.
		for dst.Len() > capacity/2 {
			v, ok := dst.Pop()
			require.True(t, ok, "dst unexpectedly empty")
			stolen = append(stolen, v)
		}
	}

	// Everything must be accounted for exactly once — a bare length
	// check would miss a duplicate silently masking a lost item.
	all := append(stolen, drainLocal(src)...)
	all = append(all, drainLocal(dst)...)

	assertPermutation(t, all, 128)
}

// --------------------------
// Randomized sequential test
// --------------------------

func TestLocal_Randomized(t *testing.T) {
	l := newTestLocal()
	e := newElastic[int]()

	rng := rand.New(rand.NewSource(1))

	var expected []int
	nextID := 0

	for step := range 100_000 {
		switch rng.Intn(3) {
		case 0:
			// Push only when there is room. This deliberately avoids
			// exercising elastic/overflow here.
			if l.RemainingSlots() > 0 {
				l.Push(nextID, e)
				expected = append(expected, nextID)
				nextID++
			}

		case 1:
			got, ok := l.Pop()

			if len(expected) == 0 {
				require.Falsef(t, ok, "step %d: Pop() succeeded on empty queue", step)
				continue
			}

			require.Truef(t, ok, "step %d: Pop() failed, expected %d", step, expected[0])
			require.Equalf(t, expected[0], got, "step %d", step)

			expected = expected[1:]

		case 2:
			// Check the observable invariants.
			require.Equalf(t, len(expected), l.Len(), "step %d: Len()", step)
			require.Equalf(t, capacity-len(expected), l.RemainingSlots(), "step %d: RemainingSlots()", step)
		}
	}

	got := drainLocal(l)
	assertIntsEqual(t, got, expected)
}

// -----------------------------------------------------------------------
// Concurrent stealing
//
// Important: each thief owns its own destination queue. This matches the
// local-queue ownership model of MoveTo.
// -----------------------------------------------------------------------

func TestLocal_ConcurrentSteal(t *testing.T) {
	const (
		itemCount = capacity
		thieves   = 8
	)

	src := newTestLocal()
	e := newElastic[int]()

	for i := range itemCount {
		src.Push(i, e)
	}

	dsts := make([]*local[int], thieves)
	for i := range dsts {
		dsts[i] = newTestLocal()
	}

	// Each goroutine only appends to its own slice, so no locking is
	// needed here — the slices are merged after wg.Wait().
	consumed := make([][]int, thieves)

	var wg sync.WaitGroup
	wg.Add(thieves)

	for i := range thieves {
		j := i
		go func() {
			defer wg.Done()

			dst := dsts[j]
			var mine []int

			for {
				item, ok := src.MoveTo(dst)
				if !ok {
					// Another thief may currently have a share in flight.
					// Give it a chance to complete before deciding that
					// the source is truly empty.
					runtime.Gosched()

					if src.Len() == 0 {
						break
					}

					continue
				}

				mine = append(mine, item)

				// Drain destination periodically, as the owner would —
				// and, unlike a real worker executing the task, record
				// what comes out so this test can account for it.
				for dst.Len() > capacity/4 {
					v, ok := dst.Pop()
					if !assert.True(t, ok, "destination unexpectedly empty") {
						break
					}
					mine = append(mine, v)
				}
			}

			consumed[j] = mine
		}()
	}

	wg.Wait()

	// Drain everything that remains.
	all := drainLocal(src)

	for _, mine := range consumed {
		all = append(all, mine...)
	}

	for _, dst := range dsts {
		all = append(all, drainLocal(dst)...)
	}

	assertPermutation(t, all, itemCount)
}

func assertPermutation(t *testing.T, got []int, n int) {
	t.Helper()

	require.Len(t, got, n)

	seen := make([]bool, n)

	for _, v := range got {
		require.Truef(t, v >= 0 && v < n, "invalid item %d", v)
		require.Falsef(t, seen[v], "duplicate item %d", v)

		seen[v] = true
	}

	for i, ok := range seen {
		require.Truef(t, ok, "missing item %d", i)
	}
}

// -----------------------------------------------------------------------------
// Concurrent steal stress
// -----------------------------------------------------------------------------

func TestLocal_ConcurrentStealStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped with -short")
	}

	const (
		iterations = 1_000
		thieves    = 16
	)

	for iteration := range iterations {
		src := newTestLocal()
		e := newElastic[int]()

		for i := range capacity {
			src.Push(i, e)
		}

		dsts := make([]*local[int], thieves)
		for i := range dsts {
			dsts[i] = newTestLocal()
		}

		var (
			wg       sync.WaitGroup
			returned atomic.Int64
			seen     = make([]atomic.Uint32, capacity)
		)

		wg.Add(thieves)

		for i := range thieves {
			j := i

			go func() {
				defer wg.Done()

				dst := dsts[j]
				for range 10_000 {
					item, ok := src.MoveTo(dst)

					if ok {
						if !assert.Truef(t, item >= 0 && item < capacity, "invalid stolen item %d", item) {
							return
						}

						seen[item].Add(1)
						returned.Add(1)
						continue
					}

					// Drain destination.
					for {
						item, ok := dst.Pop()
						if !ok {
							break
						}

						if !assert.Truef(t, item >= 0 && item < capacity, "invalid destination item %d", item) {
							return
						}
						seen[item].Add(1)
					}

					if src.Len() == 0 {
						break
					}
					runtime.Gosched()
				}
			}()
		}

		wg.Wait()

		// Drain source and destinations after all stealers stop.
		for {
			item, ok := src.Pop()
			if !ok {
				break
			}
			require.Truef(t, item >= 0 && item < capacity, "invalid source item %d", item)
			seen[item].Add(1)
		}

		for _, dst := range dsts {
			for {
				item, ok := dst.Pop()
				if !ok {
					break
				}
				require.Truef(t, item >= 0 && item < capacity, "invalid destination item %d", item)
				seen[item].Add(1)
			}
		}

		for item := range capacity {
			count := seen[item].Load()
			require.Equalf(
				t,
				uint32(1),
				count,
				"iteration=%d item=%d seen=%d times; want exactly once",
				iteration, item, count,
			)
		}
	}
}

// -----------------------------------------------------------------------------
// Race-oriented test: concurrent thieves + owner Pop.
//
// This is a particularly important state transition because MoveTo reserves
// [share, cur) while Pop advances cur.
// -----------------------------------------------------------------------------

func TestLocal_ConcurrentStealAndPop(t *testing.T) {
	const (
		itemCount = capacity
		thieves   = 8
	)

	src := newTestLocal()
	e := newElastic[int]()

	for i := range itemCount {
		src.Push(i, e)
	}

	dsts := make([]*local[int], thieves)
	for i := range dsts {
		dsts[i] = newTestLocal()
	}

	var (
		wg   sync.WaitGroup
		seen = make([]atomic.Uint32, itemCount)
	)

	consume := func(item int) {
		if !assert.Truef(t, item >= 0 && item < itemCount, "invalid item %d", item) {
			return
		}

		seen[item].Add(1)
	}

	// Owner pops concurrently with thieves.
	wg.Go(func() {
		for {
			item, ok := src.Pop()
			if !ok {
				if src.Len() == 0 {
					return
				}
				runtime.Gosched()
				continue
			}
			consume(item)
		}
	})

	for i := range thieves {
		j := i

		wg.Go(func() {
			dst := dsts[j]
			for {
				item, ok := src.MoveTo(dst)
				if ok {
					consume(item)
					continue
				}

				// Execute tasks already transferred to our local queue.
				for {
					item, ok := dst.Pop()
					if !ok {
						break
					}
					consume(item)
				}

				if src.Len() == 0 {
					return
				}
				runtime.Gosched()
			}
		})
	}

	wg.Wait()

	for _, dst := range dsts {
		for {
			item, ok := dst.Pop()
			if !ok {
				break
			}
			consume(item)
		}
	}

	for i := range itemCount {
		got := seen[i].Load()
		require.Equalf(t, uint32(1), got, "item %d seen %d times, want exactly once", i, got)
	}
}

// -----------------------------------------------------------------------------
// Long-running mixed stress.
//
// This deliberately creates lots of wrap-around and share/Pop races.
// -----------------------------------------------------------------------------

func TestLocal_MixedStress(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped with -short")
	}

	const (
		rounds  = 10_000
		thieves = 4
	)

	for round := range rounds {
		src := newTestLocal()
		e := newElastic[int]()

		// Keep the queue below capacity so Push doesn't enter the elastic
		// overflow path. Overflow gets its own test because elastic is an
		// external dependency of local.
		n := 32 + round%128

		for i := range n {
			src.Push(i, e)
		}

		dsts := make([]*local[int], thieves)
		for i := range dsts {
			dsts[i] = newTestLocal()
		}

		var wg sync.WaitGroup

		var seen [capacity]atomic.Uint32

		consume := func(v int) {
			if !assert.Truef(t, v >= 0 && v < n, "round=%d invalid value=%d n=%d", round, v, n) {
				return
			}
			seen[v].Add(1)
		}

		for i := range thieves {
			j := i

			wg.Go(func() {
				dst := dsts[j]
				for {
					item, ok := src.MoveTo(dst)
					if ok {
						consume(item)
						continue
					}

					// Consume tasks transferred to this worker.
					for {
						item, ok := dst.Pop()
						if !ok {
							break
						}
						consume(item)
					}

					if src.Len() == 0 {
						return
					}
					runtime.Gosched()
				}
			})
		}

		wg.Wait()

		// Final drains.
		for {
			item, ok := src.Pop()
			if !ok {
				break
			}
			consume(item)
		}

		for _, dst := range dsts {
			for {
				item, ok := dst.Pop()
				if !ok {
					break
				}
				consume(item)
			}
		}

		for i := range n {
			got := seen[i].Load()
			require.Equalf(t, uint32(1), got, "round=%d item=%d seen=%d times; want 1", round, i, got)
		}
	}
}

func TestLocal_PushOverflow(t *testing.T) {
	l := newTestLocal()
	e := newElastic[int]()

	// Fill the local queue completely.
	for i := range capacity {
		l.Push(i, e)
	}
	require.Equal(t, capacity, l.Len())

	// This push must trigger pushOverflow.
	l.Push(capacity, e)

	// pushOverflow keeps the first half locally.
	wantLocal := capacity / 2
	require.Equal(t, wantLocal, l.Len(), "local Len()")

	// Second half + newly pushed task go to overflow.
	gotOverflow := popAllElastic(e)
	wantOverflow := capacity/2 + 1
	require.Len(t, gotOverflow, wantOverflow, "overflow contains")

	// Verify exact ordering.
	for i, v := range gotOverflow {
		want := capacity/2 + i
		require.Equalf(t, want, v, "overflow[%d]", i)
	}

	// Verify local half.
	gotLocal := drainLocal(l)
	require.Len(t, gotLocal, capacity/2, "local drained")

	for i, v := range gotLocal {
		require.Equalf(t, i, v, "local[%d]", i)
	}
}

func TestLocal_PushOverflow_AfterWrapAround(t *testing.T) {
	l := newTestLocal()
	e := newElastic[int]()

	// Move head/tail forward.
	for i := range capacity / 2 {
		l.Push(i, e)
	}

	for i := range capacity / 2 {
		got, ok := l.Pop()
		require.Truef(t, ok, "Pop() failed at %d", i)
		require.Equal(t, i, got)
	}

	// Fill remaining physical capacity.
	for i := capacity / 2; i < capacity+capacity/2; i++ {
		l.Push(i, e)
	}
	require.Equal(t, capacity, l.Len())

	// Trigger overflow.
	l.Push(capacity+capacity/2, e)
	require.Equal(t, capacity/2, l.Len(), "local Len()")

	overflowItems := popAllElastic(e)
	require.Len(t, overflowItems, capacity/2+1, "overflow len")

	// Before this Push, the queue held the logical range [capacity/2,
	// capacity/2+capacity) (values 128..383). pushOverflow keeps the
	// OLDER half of that range locally — i.e. capacity/2..capacity-1 —
	// and sends the newer half (capacity..capacity+capacity/2-1) plus
	// the new task to overflow. This is a fixed offset from the queue's
	// current logical window, not from the total historical push count.
	for i, v := range overflowItems {
		want := capacity + i
		require.Equalf(t, want, v, "overflow[%d]", i)
	}

	localItems := drainLocal(l)
	for i, v := range localItems {
		want := capacity/2 + i
		require.Equalf(t, want, v, "local[%d]", i)
	}
}

func TestLocal_RefillFromOverflow(t *testing.T) {
	l := newTestLocal()
	e := newElastic[int]()

	const n = capacity / 2

	items := make([]int, n)

	for i := range items {
		items[i] = i
	}

	e.PushBatch(items)
	require.Equal(t, capacity, l.RemainingSlots(), "initial RemainingSlots()")

	batch, ok := e.PopBatch(n)
	require.True(t, ok, "PopBatch() returned false")

	l.PushBatchNoOverflow(batch)
	require.Equal(t, n, l.Len())

	got := drainLocal(l)
	assertIntsEqual(t, got, items)

	remaining, ok := e.PopBatch(1)
	require.Falsef(t, ok, "overflow should be empty: got=%v", remaining)
	require.Nil(t, remaining)
}

func TestLocal_OverflowRoundTrip(t *testing.T) {
	l := newTestLocal()
	e := newElastic[int]()

	const n = capacity * 4

	for i := range n {
		l.Push(i, e)
	}

	var all []int

	// Drain local.
	all = append(all, drainLocal(l)...)

	// Drain overflow.
	all = append(all, popAllElastic(e)...)

	// local + overflow together must contain every pushed task exactly
	// once. They do NOT form one global FIFO stream: pushOverflow keeps
	// its retained half by aliasing tail forward (no data movement), so
	// across repeated overflow rounds an item's position in the buffer
	// no longer lines up with its original push order. Global ordering
	// across the local/overflow boundary was never a guarantee this
	// data structure makes — it's a work-stealing run queue, not a
	// strict FIFO.
	assertPermutation(t, all, n)
}

func TestLocal_ConcurrentStealOverflow(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test skipped with -short")
	}

	const (
		thieves = 8
		items   = capacity * 16
	)

	overflow := newElastic[int]()
	src := newTestLocal()

	for i := range items {
		src.Push(i, overflow)
	}

	dsts := make([]*local[int], thieves)
	for i := range dsts {
		dsts[i] = newTestLocal()
	}

	seen := make([]atomic.Uint32, items)
	var wg sync.WaitGroup

	consume := func(v int) {
		if !assert.Truef(t, v >= 0 && v < items, "invalid item %d", v) {
			return
		}
		seen[v].Add(1)
	}

	wg.Add(thieves)

	for i := range thieves {
		j := i

		go func() {
			defer wg.Done()

			dst := dsts[j]
			for {
				item, ok := src.MoveTo(dst)
				if ok {
					consume(item)
					continue
				}

				for {
					item, ok := dst.Pop()
					if !ok {
						break
					}
					consume(item)
				}

				if src.Len() == 0 {
					return
				}
				runtime.Gosched()
			}
		}()
	}

	wg.Wait()

	// Drain everything remaining.
	for {
		item, ok := src.Pop()
		if !ok {
			break
		}
		consume(item)
	}

	for _, dst := range dsts {
		for {
			item, ok := dst.Pop()
			if !ok {
				break
			}
			consume(item)
		}
	}

	// Overflow is part of the system too.
	for _, item := range popAllElastic(overflow) {
		consume(item)
	}

	for i := range items {
		got := seen[i].Load()
		require.Equalf(t, uint32(1), got, "item %d seen %d times, want exactly once", i, got)
	}
}

func TestLocal_CacheRemapIsPermutation(t *testing.T) {
	l := newTestLocal()

	seen := make([]bool, capacity)

	for i := range uint32(capacity) {
		idx := pkg.CacheRemap(
			i,
			capacity,
			l.entrySize,
		)

		require.Lessf(t, idx, uint32(capacity), "CacheRemap(%d) outside [0,%d)", i, capacity)
		require.Falsef(t, seen[idx], "CacheRemap is not injective: physical index %d mapped twice", idx)

		seen[idx] = true
	}

	for i, ok := range seen {
		require.Truef(t, ok, "physical index %d is never mapped", i)
	}
}
