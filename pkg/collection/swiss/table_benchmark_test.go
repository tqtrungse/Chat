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
	"fmt"
	"strings"
	"testing"

	"xxx/pkg/hash"
)

const size = 1024

func BenchmarkMemory(b *testing.B) {
	strs := make([]string, size)
	for i := range size {
		strs[i] = strings.Repeat("x", 20) + fmt.Sprintf("string#%d", i)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		table := NewTable[string, int](hash.String)

		for j := range size {
			table.Set(strs[j], j)
		}
	}
}

func BenchmarkMemory_Std(b *testing.B) {
	strs := make([]string, size)
	for i := range size {
		strs[i] = strings.Repeat("x", 20) + fmt.Sprintf("string#%d", i)
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		table := make(map[string]int)

		for j := range size {
			table[strs[j]] = j
		}
	}
}

func BenchmarkHashStringSpeed(b *testing.B) {
	strs := make([]string, size)
	for i := range size {
		strs[i] = strings.Repeat("x", 8) + fmt.Sprintf("string#%d", i)
	}

	sum := 0
	m := NewTableWithHintCap[string, int](size, hash.String)
	for i := range size {
		m.Set(strs[i], 0)
	}
	idx := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum += m.Get(strs[idx])
		idx++
		if idx == size {
			idx = 0
		}
	}
}

func BenchmarkHashStringSpeed_Std(b *testing.B) {
	strs := make([]string, size)
	for i := range size {
		strs[i] = strings.Repeat("x", 8) + fmt.Sprintf("string#%d", i)
	}

	sum := 0
	m := make(map[string]int, size)
	for i := range size {
		m[strs[i]] = 0
	}
	idx := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum += m[strs[idx]]
		idx++
		if idx == size {
			idx = 0
		}
	}
}

func BenchmarkHashInt32Speed(b *testing.B) {
	ints := make([]int32, size)
	for i := range size {
		ints[i] = int32(i)
	}
	sum := 0
	m := NewTableWithHintCap[int32, int](size, hash.Int32)
	for i := range size {
		m.Set(ints[i], 0)
	}
	idx := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum += m.Get(ints[idx])
		idx++
		if idx == size {
			idx = 0
		}
	}
}

func BenchmarkHashInt32Speed_Std(b *testing.B) {
	ints := make([]int32, size)
	for i := range size {
		ints[i] = int32(i)
	}
	sum := 0
	m := make(map[int32]int, size)
	for i := range size {
		m[ints[i]] = 0
	}
	idx := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum += m[ints[idx]]
		idx++
		if idx == size {
			idx = 0
		}
	}
}

func BenchmarkHashInt64Speed(b *testing.B) {
	ints := make([]int64, size)
	for i := range size {
		ints[i] = int64(i)
	}
	sum := 0
	m := NewTableWithHintCap[int64, int](size, hash.Int64)
	for i := range size {
		m.Set(ints[i], 0)
	}
	idx := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum += m.Get(ints[idx])
		idx++
		if idx == size {
			idx = 0
		}
	}
}

func BenchmarkHashInt64Speed_Std(b *testing.B) {
	ints := make([]int64, size)
	for i := range size {
		ints[i] = int64(i)
	}
	sum := 0
	m := make(map[int64]int, size)
	for i := range size {
		m[ints[i]] = 0
	}
	idx := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum += m[ints[idx]]
		idx++
		if idx == size {
			idx = 0
		}
	}
}

func BenchmarkHashStringArraySpeed(b *testing.B) {
	stringPairs := make([][2]string, size)
	for i := range size {
		for j := range 2 {
			stringPairs[i][j] = fmt.Sprintf("string#%d/%d", i, j)
		}
	}
	sum := 0
	m := NewTableWithHintCap[[2]string, int](
		size,
		func(key [2]string, seed uint64) uint64 {
			state := hash.String(key[0], seed)
			return hash.String(key[1], state)
		},
	)
	for i := range size {
		m.Set(stringPairs[i], 0)
	}
	idx := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum += m.Get(stringPairs[idx])
		idx++
		if idx == size {
			idx = 0
		}
	}
}

func BenchmarkHashStringArraySpeed_Std(b *testing.B) {
	stringPairs := make([][2]string, size)
	for i := range size {
		for j := range 2 {
			stringPairs[i][j] = fmt.Sprintf("string#%d/%d", i, j)
		}
	}
	sum := 0
	m := make(map[[2]string]int, size)
	for i := range size {
		m[stringPairs[i]] = 0
	}
	idx := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum += m[stringPairs[idx]]
		idx++
		if idx == size {
			idx = 0
		}
	}
}

func BenchmarkMegMap(b *testing.B) {
	m := NewTable[string, bool](hash.String)
	for suffix := 'A'; suffix <= 'G'; suffix++ {
		m.Set(strings.Repeat("X", 1<<20-1)+fmt.Sprint(suffix), true)
	}
	key := strings.Repeat("X", 1<<20-1) + "k"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m.Lookup(key)
	}
}

func BenchmarkMegMap_Std(b *testing.B) {
	m := make(map[string]bool)
	for suffix := 'A'; suffix <= 'G'; suffix++ {
		m[strings.Repeat("X", 1<<20-1)+fmt.Sprint(suffix)] = true
	}
	key := strings.Repeat("X", 1<<20-1) + "k"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m[key]
	}
}

func BenchmarkMegOneMap(b *testing.B) {
	m := NewTable[string, bool](hash.String)
	m.Set(strings.Repeat("X", 1<<20), true)
	key := strings.Repeat("Y", 1<<20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m.Lookup(key)
	}
}

func BenchmarkMegOneMap_Std(b *testing.B) {
	m := make(map[string]bool)
	m[strings.Repeat("X", 1<<20)] = true
	key := strings.Repeat("Y", 1<<20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m[key]
	}
}

func BenchmarkMegEqMap(b *testing.B) {
	m := NewTable[string, bool](hash.String)
	key1 := strings.Repeat("X", 1<<20)
	key2 := strings.Repeat("X", 1<<20) // equal but different instance
	m.Set(key1, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m.Lookup(key2)
	}
}

func BenchmarkMegEqMap_Std(b *testing.B) {
	m := make(map[string]bool)
	key1 := strings.Repeat("X", 1<<20)
	key2 := strings.Repeat("X", 1<<20) // equal but different instance
	m[key1] = true
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m[key2]
	}
}

func BenchmarkMegEmptyMap(b *testing.B) {
	m := NewTable[string, bool](hash.String)
	key := strings.Repeat("X", 1<<20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m.Lookup(key)
	}
}

func BenchmarkMegEmptyMap_Std(b *testing.B) {
	m := make(map[string]bool)
	key := strings.Repeat("X", 1<<20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m[key]
	}
}

func BenchmarkSmallStrMap(b *testing.B) {
	m := NewTable[string, bool](hash.String)
	for suffix := 'A'; suffix <= 'G'; suffix++ {
		m.Set(fmt.Sprint(suffix), true)
	}
	key := "k"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m.Lookup(key)
	}
}

func BenchmarkSmallStrMap_Std(b *testing.B) {
	m := make(map[string]bool)
	for suffix := 'A'; suffix <= 'G'; suffix++ {
		m[fmt.Sprint(suffix)] = true
	}
	key := "k"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = m[key]
	}
}

func BenchmarkMapStringKeysEight_16(b *testing.B)      { benchmarkMapStringKeysEightMy(b, 16) }
func BenchmarkMapStringKeysEight_16_Std(b *testing.B)  { benchmarkMapStringKeysEightStd(b, 16) }
func BenchmarkMapStringKeysEight_32(b *testing.B)      { benchmarkMapStringKeysEightMy(b, 32) }
func BenchmarkMapStringKeysEight_32_Std(b *testing.B)  { benchmarkMapStringKeysEightStd(b, 32) }
func BenchmarkMapStringKeysEight_64(b *testing.B)      { benchmarkMapStringKeysEightMy(b, 64) }
func BenchmarkMapStringKeysEight_64_Std(b *testing.B)  { benchmarkMapStringKeysEightStd(b, 64) }
func BenchmarkMapStringKeysEight_128(b *testing.B)     { benchmarkMapStringKeysEightMy(b, 128) }
func BenchmarkMapStringKeysEight_128_Std(b *testing.B) { benchmarkMapStringKeysEightStd(b, 128) }
func BenchmarkMapStringKeysEight_256(b *testing.B)     { benchmarkMapStringKeysEightMy(b, 256) }
func BenchmarkMapStringKeysEight_256_Std(b *testing.B) { benchmarkMapStringKeysEightStd(b, 256) }
func BenchmarkMapStringKeysEight_1M(b *testing.B)      { benchmarkMapStringKeysEightMy(b, 1<<20) }
func BenchmarkMapStringKeysEight_1M_Std(b *testing.B)  { benchmarkMapStringKeysEightStd(b, 1<<20) }

func benchmarkMapStringKeysEightMy(b *testing.B, keySize int) {
	m := NewTable[string, bool](hash.String)
	for i := range 8 {
		m.Set(strings.Repeat("K", i+1), true)
	}
	key := strings.Repeat("K", keySize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Get(key)
	}
}

func benchmarkMapStringKeysEightStd(b *testing.B, keySize int) {
	m := make(map[string]bool)
	for i := range 8 {
		m[strings.Repeat("K", i+1)] = true
	}
	key := strings.Repeat("K", keySize)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m[key]
	}
}

func BenchmarkMapFirst(b *testing.B) {
	for n := 1; n <= 16; n++ {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			m := NewTable[int, bool](hash.Int)
			for i := range 1000 {
				m.Set(i, true)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = m.Get(0)
			}
		})
	}
}

func BenchmarkMapFirst_Std(b *testing.B) {
	for n := 1; n <= 16; n++ {
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			m := make(map[int]bool)
			for i := range 1000 {
				m[i] = true
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = m[0]
			}
		})
	}
}
