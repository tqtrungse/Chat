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

package swiss

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"xxx/pkg/hash"
)

func BenchmarkSet_Sequential(b *testing.B) {
	tbl := newIntTableC2()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tbl.Set(i, i)
	}
}

func BenchmarkSet_Update_Sequential(b *testing.B) {
	// 100 % hit — all Sets overwrite an existing key.
	tbl := newIntTableC2()
	const n = 1_000
	for i := range n {
		tbl.Set(i, i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tbl.Set(i%n, i)
	}
}

func BenchmarkGet_Hit_Sequential(b *testing.B) {
	tbl := newIntTableC2()
	const n = 1_000
	for i := range n {
		tbl.Set(i, i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tbl.Get(i % n)
	}
}

func BenchmarkGet_Miss_Sequential(b *testing.B) {
	tbl := newIntTableC2()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tbl.Get(i) // table is always empty → always miss
	}
}

func BenchmarkDelete_Sequential(b *testing.B) {
	tbl := newIntTableC2()
	for i := 0; i < b.N; i++ {
		tbl.Set(i, i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tbl.Delete(i)
	}
}

func BenchmarkGet_Hit_1K(b *testing.B) { benchmarkGetHit(b, 1_000) }

func BenchmarkGet_Hit_100K(b *testing.B) { benchmarkGetHit(b, 100_000) }

func BenchmarkGet_Hit_1M(b *testing.B) { benchmarkGetHit(b, 1_000_000) }

func benchmarkGetHit(b *testing.B, size int) {
	b.Helper()
	tbl := newIntTableC2()
	for i := range size {
		tbl.Set(i, i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = tbl.Get(i % size)
	}
}

func BenchmarkSet_WithResize(b *testing.B) {
	// Measures the full cost of building a table from scratch including
	// every resize. Reports allocations to catch unexpected heap pressure.
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tbl := newIntTableC2()
		b.StartTimer()
		for j := range 10_000 {
			tbl.Set(j, j)
		}
	}
}

func BenchmarkSet_Parallel(b *testing.B) {
	tbl := newIntTableC2()
	var counter atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k := int(counter.Add(1))
			tbl.Set(k, k)
		}
	})
}

func BenchmarkGet_Parallel_HitRate100(b *testing.B) {
	tbl := newIntTableC2()
	const n = 10_000
	for i := range n {
		tbl.Set(i, i)
	}
	var counter atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k := int(counter.Add(1)) % n
			_ = tbl.Get(k)
		}
	})
}

func BenchmarkMixed_Parallel_80Read20Write(b *testing.B) {
	tbl := newIntTableC2()
	const n = 10_000
	for i := range n {
		tbl.Set(i, i)
	}
	var counter atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k := int(counter.Add(1))
			if k%5 == 0 {
				tbl.Set(k%n, k)
			} else {
				_ = tbl.Get(k % n)
			}
		}
	})
}

func BenchmarkTableC2_Concurrent_90Read_10Write(b *testing.B) {
	m := NewTableC2WithHintCap[int, int](1024, hash.Int)

	// Pre-fill some data
	for i := range 10000 {
		m.Set(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			k := rng.Intn(20000)
			if rng.Intn(100) < 90 { // 90% Get
				_ = m.Get(k)
			} else { // 10% Set
				m.Set(k, k)
			}
		}
	})
}

func BenchmarkTableC2_Concurrent_WriteHeavy(b *testing.B) {
	m := NewTableC2WithHintCap[int, int](1024, hash.Int)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			m.Set(i, i)
			i++
		}
	})
}

func BenchmarkSyncMap_Concurrent_90Read_10Write(b *testing.B) {
	var m sync.Map

	for i := range 10000 {
		m.Store(i, i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			k := rng.Intn(20000)
			if rng.Intn(100) < 90 {
				_, _ = m.Load(k)
			} else {
				m.Store(k, k)
			}
		}
	})
}

func BenchmarkSyncMap_Concurrent_WriteHeavy(b *testing.B) {
	var m sync.Map

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			m.Store(i, i)
			i++
		}
	})
}
