/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
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

package buffer

import (
	"math/bits"
	"sort"
	"sync"
	"sync/atomic"

	"xxx/pkg"
)

var (
	minBitSize = bits.Len(uint(pkg.CacheLineSize)) - 1
	minSize    = 1 << minBitSize
)

const (
	steps                   = 20
	calibrateCallsThreshold = 42000
	maxPercentile           = 0.95
)

// ringBuf is the alias of ring.Buffer.
type ringBuf = coreRing

// pool represents ring-buffer pool.
//
// Distinct pools may be used for distinct types of byte buffers.
// Properly determined byte buffer types with their own pools may help to reduce
// memory waste.
type pool struct {
	calls       [steps]uint64
	calibrating atomic.Uint64

	defaultSize atomic.Uint64
	maxSize     atomic.Uint64

	pool sync.Pool
}

var builtinPool pool

// Get returns new byte buffer with zero length.
//
// The byte buffer may be returned to the pool via Put after the use
// in order to minimize GC overhead.
func (p *pool) Get() *ringBuf {
	v := p.pool.Get()
	if v != nil {
		return v.(*ringBuf)
	}
	return newCoreRing(int(p.defaultSize.Load()))
}

// Put releases byte buffer obtained via Get to the pool.
//
// The buffer mustn't be accessed after returning to the pool.
func (p *pool) Put(b *ringBuf) {
	idx := index(b.Len())

	if atomic.AddUint64(&p.calls[idx], 1) > calibrateCallsThreshold {
		p.calibrate()
	}

	maxSize := int(p.maxSize.Load())
	if maxSize == 0 || b.Cap() <= maxSize {
		b.Reset()
		p.pool.Put(b)
	}
}

func (p *pool) calibrate() {
	if !p.calibrating.CompareAndSwap(0, 1) {
		return
	}

	a := make(callSizes, 0, steps)
	var callsSum uint64
	for i := range uint64(steps) {
		calls := atomic.SwapUint64(&p.calls[i], 0)
		callsSum += calls
		a = append(a, callSize{
			calls: calls,
			size:  uint64(minSize << i),
		})
	}
	sort.Sort(a)

	defaultSize := a[0].size
	maxSize := defaultSize

	maxSum := uint64(float64(callsSum) * maxPercentile)
	callsSum = 0
	for i := range steps {
		if callsSum > maxSum {
			break
		}
		callsSum += a[i].calls
		size := a[i].size
		if size > maxSize {
			maxSize = size
		}
	}

	p.defaultSize.Store(defaultSize)
	p.maxSize.Store(maxSize)
	p.calibrating.Store(0)
}

type callSize struct {
	calls uint64
	size  uint64
}

type callSizes []callSize

func (ci callSizes) Len() int {
	return len(ci)
}

func (ci callSizes) Less(i, j int) bool {
	return ci[i].calls > ci[j].calls
}

func (ci callSizes) Swap(i, j int) {
	ci[i], ci[j] = ci[j], ci[i]
}

func index(n int) int {
	n--
	n >>= minBitSize
	idx := 0
	if n > 0 {
		idx = bits.Len(uint(n))
	}
	if idx >= steps {
		idx = steps - 1
	}
	return idx
}
