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

package mpsc

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------
// Basic single-goroutine correctness
// ----------------------------------

func TestElastic_New_EmptyQueueHasNoData(t *testing.T) {
	e, err := NewElastic[int](8)
	require.NoError(t, err, "NewElastic returned unexpected error")

	_, ok := e.TryPop()
	require.False(t, ok, "TryPop on empty queue returned ok=true")

	_, ok = e.Pop()
	require.False(t, ok, "Pop on empty queue returned ok=true")

	require.False(t, e.IsClosed(), "freshly created queue reports closed")
}

func TestElastic_PushTryPop_SingleNode_FIFOOrder(t *testing.T) {
	e, err := NewElastic[int](8)
	require.NoError(t, err)

	const n = 8 // exactly fills one node, no rotation
	for i := range n {
		closed := e.Push(i)
		require.Falsef(t, closed, "Push(%d) reported closed unexpectedly", i)
	}
	for i := range n {
		v, ok := e.TryPop()
		require.Truef(t, ok, "TryPop: expected value %d, got no data", i)
		require.Equalf(t, i, v, "FIFO order violated")
	}
	_, ok := e.TryPop()
	require.False(t, ok, "TryPop after full drain returned ok=true")
}

func TestElastic_PushPop_AcrossNodeRotations_FIFOOrder(t *testing.T) {
	e, err := NewElastic[int](4) // small capacity forces several node rotations
	require.NoError(t, err)

	const n = 100 // ~25 rotations at capacity 4
	for i := range n {
		closed := e.Push(i)
		require.Falsef(t, closed, "Push(%d) reported closed unexpectedly", i)
	}
	for i := range n {
		v, ok := e.Pop()
		require.Truef(t, ok, "Pop: expected value %d at position %d, got no data", i, i)
		require.Equalf(t, i, v, "FIFO order violated across node rotation")
	}
}

func TestElastic_PushPop_NonPowerOfTwoCapacityRoundsUp(t *testing.T) {
	// nodeCap=5 should round up to 8 internally (NextPowerOfTwo); the
	// queue must still behave correctly across the resulting rotations.
	e, err := NewElastic[int](5)
	require.NoError(t, err)

	const n = 50
	for i := range n {
		e.Push(i)
	}
	for i := range n {
		v, ok := e.Pop()
		require.Truef(t, ok, "at %d: got no data", i)
		require.Equalf(t, i, v, "at %d", i)
	}
}

func TestElastic_PushPop_StructValueType(t *testing.T) {
	type payload struct {
		ID   int
		Name string
	}
	e, err := NewElastic[payload](4)
	require.NoError(t, err)

	want := []payload{{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}, {5, "e"}}
	for _, p := range want {
		e.Push(p)
	}
	for _, want := range want {
		got, ok := e.Pop()
		require.Truef(t, ok, "Pop: expected %+v, got no data", want)
		require.Equal(t, want, got)
	}
	// Popped slots must not retain stale references (checked indirectly:
	// a subsequent TryPop on the same, now-empty node must report no data
	// rather than replaying a stale value).
	_, ok := e.TryPop()
	require.False(t, ok, "TryPop after full drain returned ok=true")
}

// -----------------
// Close() semantics
// -----------------

func TestElastic_Close_RejectsFurtherPushes(t *testing.T) {
	e, err := NewElastic[int](8)
	require.NoError(t, err)

	e.Push(1)
	e.Close()
	require.True(t, e.IsClosed(), "IsClosed() false after Close()")

	closed := e.Push(2)
	require.True(t, closed, "Push after Close() reported closed=false")

	// The pre-close value must still be there; the post-close value must not.
	v, ok := e.Pop()
	require.True(t, ok, "expected the pre-close value to still be there")
	require.Equal(t, 1, v)

	_, ok = e.Pop()
	require.False(t, ok, "Pop returned a value that was pushed after Close()")
}

func TestElastic_Close_Idempotent(t *testing.T) {
	e, err := NewElastic[int](8)
	require.NoError(t, err)

	e.Close()
	e.Close() // must not panic or deadlock
	require.True(t, e.IsClosed(), "IsClosed() false after double Close()")
}

// TryPop must fully drain every buffered item after Close(), including
// items in nodes beyond the one e.head currently points at.
func TestElastic_TryPop_DrainsFullyAfterClose(t *testing.T) {
	e, err := NewElastic[int](4)
	require.NoError(t, err)

	const n = 20 // spans 5 nodes at capacity 4
	for i := range n {
		e.Push(i)
	}
	e.Close() // nothing popped yet: e.head is still on the first, closed node

	got := make([]int, 0, n)
	for len(got) < n {
		v, ok := e.TryPop()
		if !ok {
			continue // TryPop is nonblocking; a miss doesn't mean "done"
		}
		got = append(got, v)
	}
	for i, v := range got {
		require.Equalf(t, i, v, "order violated at position %d", i)
	}
}

// Pop must behave the same as TryPop here: fully drain every buffered item
// after Close(), even across node boundaries the consumer hasn't reached
// yet.
//
// KNOWN BUG in the reviewed elastic.go: Pop() has an extra
//
//	if e.IsClosed() { return value, false }
//
// right after the "current node is closed and drained" check. That check
// only tells you the CURRENT node is empty, not that the whole queue is
// empty, so Pop() bails out as soon as the global close flag is set
// instead of checking e.head.next like TryPop does. This test fails
// against the unmodified file (it recovers 4/20 items instead of 20/20).
// Fix: delete that `if e.IsClosed() { ... }` block from Pop so its control
// flow matches TryPop (Pop should only differ from TryPop in that it
// spin-waits out the initNext transition instead of returning early).
func TestElastic_Pop_DrainsFullyAfterClose(t *testing.T) {
	e, err := NewElastic[int](4)
	require.NoError(t, err)

	const n = 20
	for i := range n {
		e.Push(i)
	}
	e.Close()

	got := make([]int, 0, n)
	for {
		v, ok := e.Pop()
		if !ok {
			break
		}
		got = append(got, v)
	}
	require.Lenf(t, got, n, "Pop() after Close(): recovered %d/%d items: %v", len(got), n, got)
	for i, v := range got {
		require.Equalf(t, i, v, "order violated at position %d", i)
	}
}

// Sanity check isolating the bug above: Pop() must traverse node
// boundaries just fine as long as Close() has NOT been called, since the
// offending check is gated on the *global* closed flag, not per-node
// closure (every interior node closes itself on rotation regardless).
func TestElastic_Pop_TraversesNodesBeforeClose(t *testing.T) {
	e, err := NewElastic[int](4)
	require.NoError(t, err)

	const n = 20
	for i := range n {
		e.Push(i)
	}
	for i := range n {
		v, ok := e.Pop()
		require.Truef(t, ok, "at %d: got no data", i)
		require.Equalf(t, i, v, "at %d", i)
	}
}

// -----------
// Concurrency
// -----------

// Many producers, one consumer (the documented contract: e.head is a
// plain, un-synchronized pointer, so only a single goroutine may call
// TryPop/Pop on a given Elastic). Verifies no lost values and no
// duplicate values under -race.
func TestElastic_ConcurrentProducers_NoLossNoDuplication(t *testing.T) {
	const (
		numProducers = 16
		perProducer  = 5000
		nodeCap      = 8 // small on purpose: forces constant node rotation
	)
	total := numProducers * perProducer

	e, err := NewElastic[int64](nodeCap)
	require.NoError(t, err)

	const seqBits = 32
	encode := func(producer, seq int) int64 {
		return int64(producer)<<seqBits | int64(seq)
	}

	var wg sync.WaitGroup
	wg.Add(numProducers)
	for p := range numProducers {
		go func(p int) {
			defer wg.Done()
			for s := range perProducer {
				// assert, not require: FailNow()/require.* must only be
				// called from the goroutine running the test function
				// itself, never from a spawned goroutine like this one.
				if closed := e.Push(encode(p, s)); closed {
					assert.Falsef(t, closed, "producer %d: Push reported closed unexpectedly", p)
					return
				}
			}
		}(p)
	}

	var producersDone atomic.Bool
	go func() {
		wg.Wait()
		e.Close()
		producersDone.Store(true)
	}()

	seen := make([][]bool, numProducers)
	for i := range seen {
		seen[i] = make([]bool, perProducer)
	}

	got, dupes := 0, 0
	for got < total {
		v, ok := e.TryPop()
		if !ok {
			if producersDone.Load() {
				// Give the consumer a bounded number of extra attempts in
				// case of a benign race between the closer flag and the
				// final drain; TryPop is nonblocking.
				misses := 0
				for !ok && misses < 10000 {
					v, ok = e.TryPop()
					misses++
				}
				if !ok {
					break
				}
			} else {
				continue
			}
		}
		producer := int(v >> seqBits)
		seq := int(v & ((1 << seqBits) - 1))
		require.Truef(
			t,
			producer >= 0 && producer < numProducers && seq >= 0 && seq < perProducer,
			"corrupt value decoded: producer=%d seq=%d raw=%d", producer, seq, v,
		)
		if seen[producer][seq] {
			dupes++
		}
		seen[producer][seq] = true
		got++
	}

	missing := 0
	for p := range numProducers {
		for s := range perProducer {
			if !seen[p][s] {
				missing++
			}
		}
	}
	assert.Zerof(t, missing, "lost %d/%d values", missing, total)
	assert.Zerof(t, dupes, "saw %d duplicate values", dupes)
}

// Documents (rather than asserts pass/fail) that Elastic is MPSC-only:
// e.head is a plain, un-synchronized pointer, so concurrent consumers race
// on it. Skipped by default so it doesn't fail a routine `go test -race`
// run; run explicitly with -run to confirm the invariant, e.g.:
//
//	go test -race -run TestSingleConsumerInvariant -v -tags manual
func TestElastic_SingleConsumerInvariant_ConcurrentConsumersRace(t *testing.T) {
	t.Skip("documents an intentional constraint (MPSC only); run explicitly with -race to observe the race on e.head")

	e, err := NewElastic[int](4)
	require.NoError(t, err)

	for i := range 2000 {
		e.Push(i)
	}
	e.Close()

	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			for {
				if _, ok := e.TryPop(); !ok {
					return
				}
			}
		}()
	}
	wg.Wait()
}

// ----------
// Benchmarks
// ----------

// Sequential push/pop round trip in a single goroutine: cheapest possible
// baseline for the per-item cost of the fast path (no node rotation, no
// contention).
//
// NOTE: b.Fatal is left as-is here, not converted to require/assert. Both
// the failure check and the operation it's checking sit inside the b.N
// loop, i.e. on the exact path being timed; require.True/assert.True add
// an extra function call plus a not-quite-free interface dispatch on top
// of the raw `if`, which is enough to nudge ns/op on a benchmark this
// tight. Given the queue's ns/op numbers elsewhere have already turned out
// to be sensitive to measurement artifacts, better to keep this loop as
// plain stdlib.
func BenchmarkElastic_PushPop_SPSC(b *testing.B) {
	e, err := NewElastic[int](1024)
	if err != nil {
		b.Fatalf("NewElastic: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Push(i)
		if _, ok := e.TryPop(); !ok {
			b.Fatal("TryPop found no data immediately after Push")
		}
	}
}

// Concurrent producer throughput: GOMAXPROCS producers hammering Push in
// parallel while a background goroutine drains via TryPop to keep the
// queue from growing unbounded during the benchmark.
func BenchmarkElastic_Push_Parallel(b *testing.B) {
	e, err := NewElastic[int](1024)
	if err != nil {
		b.Fatalf("NewElastic: %v", err)
	}

	stop := make(chan struct{})
	var drainerWG sync.WaitGroup
	drainerWG.Go(func() {
		for {
			select {
			case <-stop:
				for {
					if _, ok := e.TryPop(); !ok {
						return
					}
				}
			default:
				e.TryPop()
			}
		}
	})

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			e.Push(i)
			i++
		}
	})
	b.StopTimer()
	close(stop)
	drainerWG.Wait()
}
