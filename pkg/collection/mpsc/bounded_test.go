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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"xxx/pkg/backoff"

	"github.com/stretchr/testify/require"
)

const benchCapacity = 4096

func TestBounded_NewZeroCapacity(t *testing.T) {
	_, err := NewBounded[int](0)
	require.Error(t, err)
}

func TestBounded_NewNonZero(t *testing.T) {
	q, err := NewBounded[int](7) // non-power-of-two; constructor should round up
	require.NoError(t, err)
	require.NotNil(t, q)
	// 7 should be rounded up to the next power of two, which is 8
	require.Equal(t, uint32(8), q.capacity)
}

func TestBounded_PushPop_FIFO(t *testing.T) {
	q, _ := NewBounded[int](8)
	const n = 8
	for i := range n {
		closed := q.Push(i)
		require.False(t, closed)
	}
	for i := range n {
		v, closed := q.Pop()
		require.False(t, closed)
		require.Equal(t, i, v)
	}
}

func TestBounded_PushPop_Wraparound(t *testing.T) {
	const capacity = 4
	q, _ := NewBounded[int](capacity)
	for round := range 20 {
		for i := range capacity {
			q.Push(round*capacity + i)
		}
		for i := range capacity {
			v, _ := q.Pop()
			want := round*capacity + i
			require.Equal(t, want, v)
		}
	}
}

func TestBounded_TryPush_ClosedQueue(t *testing.T) {
	q, _ := NewBounded[int](8)
	q.Close()
	added := q.TryPush(1)
	require.False(t, added)
}

func TestBounded_TryPush_Success(t *testing.T) {
	q, _ := NewBounded[int](8)
	added := q.TryPush(42)
	require.True(t, added)

	v, closed := q.Pop()
	require.False(t, closed)
	require.Equal(t, 42, v)
}

func TestBounded_TryPush_Full(t *testing.T) {
	const capacity = 4
	q, _ := NewBounded[int](capacity)
	for i := range capacity {
		added := q.TryPush(i)
		require.True(t, added)
	}
	added := q.TryPush(99)
	require.False(t, added)
}

func TestBounded_TryPush_FIFOOrder(t *testing.T) {
	const n = 8
	q, _ := NewBounded[int](n)
	for i := range n {
		added := q.TryPush(i)
		require.True(t, added)
	}
	for i := range n {
		v, closed := q.Pop()
		require.False(t, closed)
		require.Equal(t, i, v)
	}
}

func TestBounded_TryPush_FullThenDrainThenSucceeds(t *testing.T) {
	const capacity = 4
	q, _ := NewBounded[int](capacity)
	for i := range capacity {
		q.TryPush(i)
	}

	require.False(t, q.TryPush(99), "TryPush to full ring must return false")

	q.Pop()

	require.True(t, q.TryPush(99), "TryPush after draining a slot must return true")
}

func TestBounded_IsClosed(t *testing.T) {
	q, _ := NewBounded[int](8)
	require.False(t, q.IsClosed(), "newly created queue must not be closed")
	q.Close()
	require.True(t, q.IsClosed(), "queue must be closed after Close()")
}

func TestBounded_Close_PushReturnsClosed(t *testing.T) {
	q, _ := NewBounded[int](8)
	q.Close()
	require.True(t, q.Push(1), "Push to closed queue must return closed=true")
}

func TestBounded_Close_PopReturnsClosed(t *testing.T) {
	q, _ := NewBounded[int](8)
	q.Close()
	_, closed := q.Pop()
	require.True(t, closed, "Pop from closed queue must return closed=true")
}

// sequenceItem helps verify that each producer's items stay in FIFO order.
type sequenceItem struct {
	producerID int
	sequence   int
}

// TestBounded_MultiPushOnePop_Order extensively verifies that multiple producers can
// push concurrently without losing items, and that the single consumer receives
// all items strictly in FIFO order on a per-producer basis.
func TestBounded_MultiPushOnePop_Order(t *testing.T) {
	const (
		producers   = 16
		perProducer = 100_000
		total       = producers * perProducer
	)

	q, _ := NewBounded[sequenceItem](1024)

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		lastSeen := make([]int, producers)
		for i := range lastSeen {
			lastSeen[i] = -1 // Start at -1 so the first sequence expected is 0
		}

		received := 0
		for received < total {
			v, closed := q.Pop()
			if closed {
				require.FailNowf(t, "", "Consumer saw unexpected close after %d items", received)
				return
			}

			// Verify per-producer FIFO order
			if v.sequence != lastSeen[v.producerID]+1 {
				require.Equalf(t,
					lastSeen[v.producerID]+1,
					v.sequence,
					"Producer %d order violated",
					v.producerID,
				)
			}
			lastSeen[v.producerID] = v.sequence
			received++
		}
	}()

	var wg sync.WaitGroup
	wg.Add(producers)

	// Launch M producers
	for p := range producers {
		go func(pID int) {
			defer wg.Done()
			for i := range perProducer {
				q.Push(sequenceItem{producerID: pID, sequence: i})
			}
		}(p)
	}

	wg.Wait()      // Wait for all producers to finish pushing
	<-consumerDone // Wait for consumer to process everything
}

// TestBounded_MultiTryPushOnePop_Order does the same FIFO validation as above, but forces
// producers to spin using TryPush instead of relying on the blocking Push.
func TestBounded_MultiTryPushOnePop_Order(t *testing.T) {
	const (
		producers   = 8
		perProducer = 50_000
		total       = producers * perProducer
	)

	q, _ := NewBounded[sequenceItem](512)

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		lastSeen := make([]int, producers)
		for i := range lastSeen {
			lastSeen[i] = -1
		}

		received := 0
		for received < total {
			v, closed := q.Pop()
			if closed {
				require.FailNowf(t, "", "Consumer saw unexpected close after %d items", received)
				return
			}
			if v.sequence != lastSeen[v.producerID]+1 {
				require.Equalf(t,
					lastSeen[v.producerID]+1,
					v.sequence,
					"Producer %d order violated",
					v.producerID,
				)
			}
			lastSeen[v.producerID] = v.sequence
			received++
		}
	}()

	var wg sync.WaitGroup
	wg.Add(producers)

	for p := range producers {
		go func(pID int) {
			defer wg.Done()
			var bkc backoff.Cpu
			for i := 0; i < perProducer; {
				if q.TryPush(sequenceItem{producerID: pID, sequence: i}) {
					i++
					bkc.Reset()
				} else {
					bkc.Snooze()
				}
			}
		}(p)
	}

	wg.Wait()
	<-consumerDone
}

func TestBounded_MultiProducer_Count(t *testing.T) {
	const (
		producers   = 8
		perProducer = 1000
		total       = producers * perProducer
	)

	q, _ := NewBounded[int](512)
	var received atomic.Int64

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			_, closed := q.Pop()
			if closed {
				return
			}
			received.Add(1)
		}
	}()

	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range perProducer {
				q.Push(id*perProducer + i)
			}
		}(p)
	}
	wg.Wait()
	q.Close()
	<-consumerDone

	for {
		_, ok := q.TryPop()
		if !ok {
			break
		}
		received.Add(1)
	}
	//q.Drain(func(int) { received.Add(1) })

	require.Equal(t, int64(total), received.Load(), "received items")
}

func TestBounded_TryPush_MultiProducer_Count(t *testing.T) {
	const (
		producers   = 8
		perProducer = 1000
		total       = producers * perProducer
	)

	q, _ := NewBounded[int](512)
	var received atomic.Int64

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			_, closed := q.Pop()
			if closed {
				return
			}
			received.Add(1)
		}
	}()

	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var bkc backoff.Cpu
			for i := 0; i < perProducer; {
				if q.TryPush(id*perProducer + i) {
					i++
					bkc.Reset()
				} else {
					bkc.Snooze()
				}
			}
		}(p)
	}
	wg.Wait()
	q.Close()
	<-consumerDone

	for {
		_, ok := q.TryPop()
		if !ok {
			break
		}
		received.Add(1)
	}
	//q.Drain(func(int) { received.Add(1) })

	require.Equal(t, int64(total), received.Load(), "received items")
}

func TestBounded_MultiProducer_CloseWhilePushing(t *testing.T) {
	const producers = 8
	q, _ := NewBounded[int](64)

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			_, closed := q.Pop()
			if closed {
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for range producers {
		wg.Go(func() {
			for i := range 100_000 {
				if q.Push(i) {
					return
				}
			}
		})
	}

	q.Close()
	wg.Wait()
	<-consumerDone
}

// Large-capacity (>65536, so the mapIdx<<16 packing in metaReaderWait can
// truncate/collide) concurrent stress test: many producers, one consumer
// repeatedly parking (forced by pacing), must complete within a bound with
// no lost or corrupted items.
func TestBounded_LargeCapacityNoDeadlockNoLoss(t *testing.T) {
	const capacity = 1<<16 - 1 // 131072, above the 16-bit mapIdx budget
	const totalItems = 20000
	const producers = 8

	m, err := NewBounded[int](capacity)
	require.NoError(t, err)

	results := make(chan int, totalItems)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range totalItems {
			v, closed := m.Pop()
			if closed {
				return
			}
			results <- v
			if i%500 == 0 {
				time.Sleep(time.Millisecond) // give the reader a chance to actually park
			}
		}
	}()

	perProducer := totalItems / producers
	for p := range producers {
		go func(base int) {
			for i := range perProducer {
				m.Push(base*perProducer + i)
			}
		}(p)
	}

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		require.FailNow(t, "BUG: consumer never finished -- possible deadlock under large capacity")
	}
	close(results)

	seen := make(map[int]bool, totalItems)
	count := 0
	for v := range results {
		if seen[v] {
			require.FailNowf(t, "", "duplicate value received: %d", v)
		}
		seen[v] = true
		count++
	}
	require.Equal(t, totalItems, count, "received items (data loss)")
	t.Logf("all %d items received exactly once, capacity=%d (>65536)", count, capacity)
}

func TestBounded_Drain_Empty(t *testing.T) {
	q, _ := NewBounded[int](8)
	q.Close()
	called := false
	q.Drain(func(int) { called = true })
	require.False(t, called, "drain callback must not be called on empty closed queue")
}

func TestBounded_Drain_AllItemsDelivered(t *testing.T) {
	const n = 200
	q, _ := NewBounded[int](256)
	for i := range n {
		q.Push(i)
	}
	q.Close()

	var got []int
	q.Drain(func(v int) { got = append(got, v) })

	require.Len(t, got, n)
	for i, v := range got {
		require.Equalf(t, i, v, "got[%d]", i)
	}
}

func TestBounded_Drain_Concurrency(t *testing.T) {
	capacity := int(1024)
	q, err := NewBounded[int64](capacity)
	require.NoError(t, err, "can not init Mpsc")

	var (
		numProducers = 10
		itemsPerProd = 1000

		totalPushedCount atomic.Int64
		totalPushedSum   atomic.Int64

		totalPoppedCount atomic.Int64
		totalPoppedSum   atomic.Int64

		wgProducers sync.WaitGroup
		wgConsumer  sync.WaitGroup

		// The flag forces Consumer data to stop in order to intentionally leave data for Drain.
		stopConsumer atomic.Bool
	)

	// 1. Consumer: Deliberately driving slowly will be forced to stop early.
	wgConsumer.Go(func() {
		// Run until forced to stop.
		for !stopConsumer.Load() {
			time.Sleep(10 * time.Microsecond) // Take a short nap to allow the Producer to load the data

			val, closed := q.Pop()
			if closed {
				break
			}
			totalPoppedCount.Add(1)
			totalPoppedSum.Add(val)
		}
	})

	// 2. Producers: Continuously pushing data
	for i := range numProducers {
		wgProducers.Add(1)
		go func(prodID int) {
			defer wgProducers.Done()
			for j := range itemsPerProd {
				val := int64(prodID*itemsPerProd + j + 1)

				if j%2 == 0 {
					closed := q.Push(val)
					if !closed {
						totalPushedCount.Add(1)
						totalPushedSum.Add(val)
					} else {
						break // Queue is closed.
					}
				} else {
					for {
						if q.TryPush(val) {
							totalPushedCount.Add(1)
							totalPushedSum.Add(val)
							break
						}
						if q.IsClosed() {
							break
						}
						runtime.Gosched()
					}
				}
			}
		}(i)
	}

	// 3. Allow the system to run chaotically for 5ms to accumulate data.
	time.Sleep(5 * time.Millisecond)

	// 4. FORCE STOP: Stop the Consumer first, then close the Queue.
	stopConsumer.Store(true)
	q.Close()

	// 5. Chờ mọi thứ thoái lui hoàn toàn.
	wgProducers.Wait()
	wgConsumer.Wait()

	// 6. At this point, 100% of the queue has excess elements. Start Draining.
	var totalDrainedCount int64
	var totalDrainedSum int64

	q.Drain(func(val int64) {
		totalDrainedCount++
		totalDrainedSum += val
	})

	// 7. Verify the results.
	pushedCount := totalPushedCount.Load()
	pushedSum := totalPushedSum.Load()

	poppedCount := totalPoppedCount.Load()
	poppedSum := totalPoppedSum.Load()

	finalCount := poppedCount + totalDrainedCount
	finalSum := poppedSum + totalDrainedSum

	t.Logf("Pushed : count=%d, sum=%d", pushedCount, pushedSum)
	t.Logf("Popped : count=%d, sum=%d", poppedCount, poppedSum)
	t.Logf("Drained: count=%d, sum=%d", totalDrainedCount, totalDrainedSum)

	// Check if the test was performed correctly.
	require.NotZero(t, totalDrainedCount, "TEST ERROR: No stuck elements were forced into Drain. The CPU may be running too fast; increase the time.Sleep in step 3")

	// Ensure that no data bits are lost or overwritten.
	require.Equal(t, pushedCount, finalCount, "QUANTITY ERROR: Pushed and Pop + Drain counts differ")
	require.Equal(t, pushedSum, finalSum, "VALUE ERROR: pushed and Pop + Drain sums differ")
}
