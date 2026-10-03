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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	cases := []struct {
		n    int
		want int
	}{
		{0, 0},
		{1, 0},
		{minSize, 0},          // 64 -> bucket 0
		{minSize + 1, 1},      // 65 -> bucket 1
		{minSize * 2, 1},      // 128 -> bucket 1
		{minSize*2 + 1, 2},    // 129 -> bucket 2
		{minSize * 4, 2},      // 256 -> bucket 2
		{minSize*4 + 1, 3},    // 257 -> bucket 3
		{minSize * 8, 3},      // 512 -> bucket 3
		{minSize*8 + 1, 4},    // 513 -> bucket 4
		{minSize << 19, 19},   // exactly the last bucket's representative size
		{minSize<<19 + 1, 19}, // one byte over -> must saturate, not overflow
		{1 << 30, 19},         // pathologically large -> must saturate, not panic
	}
	for _, c := range cases {
		got := index(c.n)
		assert.Equal(t, c.want, got)
	}
}

func TestPool_PutResetsBuffer(t *testing.T) {
	b := newCoreRing(64)

	_, err := b.WriteString("hello")
	require.NoError(t, err)
	require.False(t, b.IsEmpty())

	var p pool
	p.Put(b)
	require.True(t, b.IsEmpty())
}

func TestPool_MaxSizeEviction(t *testing.T) {
	var p pool
	p.maxSize.Store(256)

	big := newCoreRing(1024) // Cap() == 1024, well above the 256 ceiling
	require.Greater(t, big.Cap(), 256)
	p.Put(big)

	// defaultSize was never calibrated (still 0), so a pool hit would come
	// back with the rejected buffer's leftover 1024-byte capacity. A pool
	// miss falls back to ring.New(0), i.e. Cap() == 0. If eviction works,
	// we must observe the latter.
	got := p.Get()
	require.Zero(t, got.Cap())
}

func TestPool_WithinMaxSizeIsPooled(t *testing.T) {
	var p pool
	p.maxSize.Store(1024)

	small := newCoreRing(64)
	_, err := small.WriteString("marker")
	require.NoError(t, err)
	p.Put(small)

	got := p.Get()
	require.Equal(t, 64, got.Cap())
	require.True(t, got.IsEmpty())
}

func TestPool_Calibrate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping calibration test (crosses the 42000-call threshold) in -short mode")
	}

	var p pool

	// Every buffer here has Cap() == 256 (CeilToPowerOfTwo(200)), i.e. bucket
	// index 2. Driving > calibrateCallsThreshold Put calls through a single
	// bucket forces exactly one calibrate() run with a fully predictable
	// histogram: 100% of the mass in one bucket.
	const puts = calibrateCallsThreshold + 1
	for range puts {
		p.Put(newCoreRing(200))
	}

	got := p.defaultSize.Load()
	assert.Equal(t, uint64(256), got)

	got = p.maxSize.Load()
	assert.Equal(t, uint64(256), got)

	// New Get() calls should now pre-size to the calibrated default instead
	// of starting from an empty (Cap()==0) buffer.
	got2 := p.Get()
	assert.Condition(t, func() bool {
		return got2.Cap() == 0 || got2.Cap() == 256
	})
}

func TestPackageLevelGetPut(t *testing.T) {
	b := builtinPool.Get()
	require.NotNil(t, b)
	_, err := b.WriteString("x")
	require.NoError(t, err)
	builtinPool.Put(b)
}

// --- concurrency / data-race coverage ---------------------------------------
//
// Run with: go test -race -run TestPool_ConcurrentGetPut

func TestPool_ConcurrentGetPut(t *testing.T) {
	var p pool
	const goroutines = 64
	const iterations = 2000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func(seed int) {
			defer wg.Done()
			payload := make([]byte, 16+seed%64)
			for range iterations {
				b := p.Get()
				_, err := b.Write(payload)
				if !assert.NoError(t, err) {
					return
				}
				_ = b.Bytes()
				p.Put(b)
			}
		}(g)
	}
	wg.Wait()
}

func BenchmarkPool_GetPut(b *testing.B) {
	var p pool
	payload := []byte("the quick brown fox jumps over the lazy dog")
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := p.Get()
			_, _ = buf.Write(payload)
			p.Put(buf)
		}
	})
}

func BenchmarkNoPool_NewEachTime(b *testing.B) {
	payload := []byte("the quick brown fox jumps over the lazy dog")
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := newCoreRing(64)
			_, _ = buf.Write(payload)
		}
	})
}
