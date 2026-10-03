//go:build !(386 || amd64 || amd64p32)

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

// Why channel instead of Bounded on Apple Silicon (darwin/arm64)
//
// Benchmarked on Apple M4 Pro vs Intel i5-14600KF, BenchmarkBounded_Push,
// p=8 and p=12 (max cores), no profiling flags (profiling instrumentation
// itself was found to distort relative timings -- see block-profile note
// below):
//
//	            p=8            p=12
//	Intel  mpsc  45.1ns  ->     ~26ns  (chan: 73ns -> 123ns, mpsc wins by 4.7-5.5x)
//	M4 Pro mpsc  72.8ns  ->   75.6ns  (chan: 41ns  -> 45ns,  chan wins by ~1.7x)
//
// On Intel, Bounded's Push gets FASTER as contention increases -- FAA on
// sendIdx pipelines well across cores, and per-slot writes are independent,
// so throughput scales up with more producers. On M4 Pro, Bounded's Push is
// essentially flat regardless of p (stdev ~0.1ns -- looks like a hardware
// floor, not scheduler noise), while channel still beats it at every p
// tested.
//
// Ruled out before concluding this is a genuine hardware/fabric difference,
// not a bug in our code:
//   - backoff.Cpu's runtime.Gosched() escalation: removed entirely: no
//     change in ns/op (only cut ~19% of aggregate CPU time spent on
//     scheduler wake/park churn, none of which was on the critical path).
//   - cache-line false sharing: not applicable -- pkg.CacheLineSize aliases
//     x/sys/cpu.CacheLinePad, which is already 128B on arm64 vs 64B on
//     amd64, so padding was correct on both platforms from the start.
//   - block-profiler comparison: invalid signal -- Go's block profiler
//     instruments chansend/chanrecv/sema but is blind to Push's spin+Snooze
//     loop entirely (zero recorded contentions for Push), and
//     -blockprofilerate=1 adds real per-event overhead that hits channel's
//     millions of blocking events far harder than Bounded's near-zero count.
//
// Best remaining explanation: FAA on a single shared cache line (sendIdx)
// does not pipeline across cores on Apple's coherence fabric the way it
// does on Intel's ring/mesh interconnect -- each producer pays close to the
// full cross-core round-trip serially, so adding producers just queues them
// behind a fixed-cost bottleneck instead of amortizing it. Not yet confirmed
// by an isolated atomic-only microbenchmark (planned); treat as the leading
// hypothesis, not a proven root cause.
//
// Scope: this override is limited to darwin/arm64, NOT arm64 in general.
// Apple Silicon's fabric is a specific implementation; server-grade arm64
// (Graviton, Ampere Altra) has a different coherence design and has not
// been benchmarked -- do not assume the same conclusion holds there without
// separate data.
//
// Re-verify if: (a) Go's runtime/compiler atomic codegen for arm64 changes,
// (b) benchmarked on PushPop (consumer-contended) workload, not just
// Push-only -- this decision was made from producer-heavy numbers and may
// not hold if the real workload keeps Pop() busy.

import (
	"fmt"
	"math"
	"sync/atomic"

	"xxx/pkg"
)

type Bounded[V any] struct {
	capacity uint32
	isClosed atomic.Bool
	done     chan struct{}
	ch       chan V
}

func NewBounded[V any](capacity int) (*Bounded[V], error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("capacity is zero")
	}
	if capacity > math.MaxUint16 {
		return nil, fmt.Errorf("capacity can not over %d", math.MaxUint16)
	}

	rawCapacity := uint32(pkg.NextPowerOfTwo(capacity))
	return &Bounded[V]{
		ch:       make(chan V, rawCapacity),
		done:     make(chan struct{}),
		capacity: rawCapacity,
	}, nil
}

func (b *Bounded[V]) TryPush(value V) (added bool) {
	if b.IsClosed() {
		return false
	}

	select {
	case b.ch <- value:
		return true
	case <-b.done:
		return false
	default:
		return false
	}
}

func (b *Bounded[V]) Push(value V) (closed bool) {
	if b.IsClosed() {
		return true
	}

	select {
	case b.ch <- value:
		return false
	case <-b.done:
		return true
	}
}

func (b *Bounded[V]) TryPop() (value V, ok bool) {
	select {
	case v, chOk := <-b.ch:
		return v, chOk
	default:
		return pkg.ZeroValue[V](), false
	}
}

// Pop retrieves the next value. After Close() has been called, Pop may
// return closed=true even while buffered values remain -- it does not
// guarantee full drain. Callers that need every remaining value after
// Close must call Drain, not rely on looping Pop until closed=true.
func (b *Bounded[V]) Pop() (value V, closed bool) {
	select {
	case v := <-b.ch:
		return v, false
	case <-b.done:
		return pkg.ZeroValue[V](), true
	}
}

func (b *Bounded[V]) IsClosed() bool {
	return b.isClosed.Load()
}

func (b *Bounded[V]) Close() {
	if !b.isClosed.Swap(true) {
		close(b.done)
	}
}

func (b *Bounded[V]) Drain(fn func(V)) {
	if !b.IsClosed() {
		return
	}
	for {
		select {
		case v := <-b.ch:
			fn(v)
		default:
			return
		}
	}
}

func (b *Bounded[V]) Capacity() int {
	return int(b.capacity)
}
