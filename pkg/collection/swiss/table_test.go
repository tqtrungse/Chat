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
	"math"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"xxx/pkg/hash"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// negative zero is a good test because:
//  1. 0 and -0 are equal, yet have distinct representations.
//  2. 0 is represented as all zeros, -0 isn't.
//
// I'm not sure the language spec actually requires this behavior,
// but it's what the current map implementation does.
func TestSwissTable_NegativeZero(t *testing.T) {
	m := NewTable[float64, bool](hash.Float64)
	m.Set(+0.0, true)
	m.Set(math.Copysign(0.0, -1.0), false) // should overwrite +0 entry

	assert.Equal(t, 1, m.Len(), "length wrong")

	m = NewTable[float64, bool](hash.Float64)
	m.Set(math.Copysign(0.0, -1.0), true)
	m.Set(+0.0, true) // should overwrite +0 entry

	assert.Equal(t, 1, m.Len(), "length wrong")
}

// nan is a good test because nan != nan, and nan has
// a randomized hash value.
func TestSwissTable_MapAssignmentNan(t *testing.T) {
	m := NewTable[float64, int](hash.Float64)
	nan := math.NaN()

	// Test assignment.
	m.Set(nan, 1)
	m.Set(nan, 2)
	m.Set(nan, 4)
	testSwissTableMapNan(t, m)
}

func TestSwissTable_MapOperatorAssignmentNan(t *testing.T) {
	m := NewTable[float64, int](hash.Float64)
	nan := math.NaN()

	// Test assignment operations.
	m.Set(nan, m.Get(nan)+1)
	m.Set(nan, m.Get(nan)+2)
	m.Set(nan, m.Get(nan)+4)
	testSwissTableMapNan(t, m)
}

func testSwissTableMapNan(t *testing.T, m *Table[float64, int]) {
	assert.Equal(t, 3, m.Len(), "length wrong")
	s := 0
	m.Range(func(key float64, value int) bool {
		assert.False(t, key == key, "nan disappeared")
		assert.Zero(t, value&(value-1), "value wrong")
		s |= value
		return true
	})
	assert.Equal(t, 7, s, "values wrong")
}

func TestSwissTable_MapOperatorAssignment(t *testing.T) {
	m := NewTable[int, int](hash.Int)

	// "m[k] op= x" is rewritten into "m[k] = m[k] op x"
	// differently when op is / or % than when it isn't.
	// Simple test to make sure they all work as expected.
	m.Set(0, 12345)
	m.Set(0, m.Get(0)+67890)
	m.Set(0, m.Get(0)/123)
	m.Set(0, m.Get(0)%456)

	const want = (12345 + 67890) / 123 % 456
	got := m.Get(0)
	assert.Equal(t, want, got)
}

var sinkAppend bool

func TestSwissTable_MapAppendAssignment(t *testing.T) {
	m := NewTable[int, []int](hash.Int)

	m.Set(0, nil)
	m.Set(0, append(m.Get(0), 12345))
	m.Set(0, append(m.Get(0), 67890))
	sinkAppend = !sinkAppend
	m.Set(0, append(m.Get(0), 123, 456))
	a := []int{7, 8, 9, 0}
	m.Set(0, append(m.Get(0), a...))

	want := []int{12345, 67890, 123, 456, 7, 8, 9, 0}
	got := m.Get(0)
	assert.Truef(t, slices.Equal(got, want), "got %v, want %v", got, want)
}

// Maps aren't actually copied on assignment.
func TestSwissTable_Alias(t *testing.T) {
	m := NewTable[int, int](hash.Int)
	m.Set(0, 5)
	n := m
	n.Set(0, 6)
	assert.Equal(t, 6, m.Get(0), "alias didn't work")
}

func TestSwissTable_GrowWithNaN(t *testing.T) {
	m := NewTableWithHintCap[float64, int](4, hash.Float64)
	nan := math.NaN()

	// Use both assignment and assignment operations as they may
	// behave differently.
	m.Set(nan, 1)
	m.Set(nan, 2)
	m.Set(nan, 4)

	cnt := 0
	s := 0
	// force a hashtable resize
	for i := range 50 {
		m.Set(float64(i), i)
	}
	for i := 50; i < 100; i++ {
		m.Set(float64(i), m.Get(float64(i))+i)
	}
	m.Range(func(key float64, value int) bool {
		if key != key {
			cnt++
			s |= value
		}
		return true
	})
	assert.Equal(t, 3, cnt, "NaN keys lost during grow")
	assert.Equal(t, 7, s, "NaN values lost during grow")
}

func TestSwissTable_IterGrowAndDelete(t *testing.T) {
	m := NewTableWithHintCap[int, int](4, hash.Int)
	for i := range 100 {
		m.Set(i, i)
	}

	// grow the table
	for i := 100; i < 1000; i++ {
		m.Set(i, i)
	}
	// delete all odd keys
	for i := 1; i < 1000; i += 2 {
		m.Delete(i)
	}

	m.Range(func(key int, value int) bool {
		assert.NotEqual(t, 1, key&1, "odd value returned")
		return true
	})
}

// make sure old bucket arrays don't get GCd while
// an iterator is still using them.
func TestSwissTable_IterGrowWithGC(t *testing.T) {
	m := NewTableWithHintCap[int, int](4, hash.Int)
	for i := range 8 {
		m.Set(i, i)
	}
	for i := 8; i < 16; i++ {
		m.Set(i, i)
	}

	m.Range(func(key int, value int) bool {
		for i := 100; i < 1000; i++ {
			m.Set(i, i)
		}
		// trigger a gc
		runtime.GC()
		return true
	})

	bitmask := 0
	m.Range(func(key int, value int) bool {
		if key < 16 {
			bitmask |= 1 << uint(key)
		}
		return true
	})
	assert.Equal(t, 1<<16-1, bitmask, "missing key")
}

func TestSwissTable_ConcurrentReadsAfterGrowth(t *testing.T) {
	if runtime.GOMAXPROCS(-1) == 1 {
		defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(16))
	}
	numLoop := 10
	numGrowStep := 250
	numReader := 16
	if testing.Short() {
		numLoop, numGrowStep = 2, 100
	}
	for i := 0; i < numLoop; i++ {
		m := NewTable[int, int](hash.Int)
		for gs := 0; gs < numGrowStep; gs++ {
			m.Set(gs, gs)
			var wg sync.WaitGroup
			wg.Add(numReader * 2)
			for range numReader {
				go func() {
					defer wg.Done()
					m.Range(func(_ int, _ int) bool { return true })
				}()
				go func() {
					defer wg.Done()
					for key := 0; key < gs; key++ {
						m.Get(key)
					}
				}()
			}
			wg.Wait()
		}
	}
}

func TestSwissTable_MapHugeZero(t *testing.T) {
	type T [4000]byte
	m := NewTable[int, T](hash.Int)

	x := m.Get(0)
	assert.Equal(t, T{}, x, "map value not zero")
	_, existence := m.Lookup(0)
	assert.False(t, existence, "map value should be missing")
}

func TestSwissTable_BigItems(t *testing.T) {
	var key [256]string
	for i := range 256 {
		key[i] = "foo"
	}

	m := NewTableWithHintCap[[256]string, [256]string](
		4,
		func(key [256]string, seed uint64) uint64 {
			state := seed
			for _, k := range key {
				state = hash.String(k, state)
			}
			return state
		},
	)

	for i := range 100 {
		key[37] = fmt.Sprintf("string%02d", i)
		m.Set(key, key)
	}
	var keys [100]string
	var values [100]string
	i := 0
	m.Range(func(key [256]string, value [256]string) bool {
		keys[i] = key[37]
		values[i] = value[37]
		i++
		return true
	})

	slices.Sort(keys[:])
	slices.Sort(values[:])
	for j := range 100 {
		assert.Equalf(t, fmt.Sprintf("string%02d", j), keys[j], "#%d: missing key", j)
		assert.Equalf(t, fmt.Sprintf("string%02d", j), values[j], "#%d: missing value", j)
	}
}

func TestSwissTable_EmptyKeyAndValue(t *testing.T) {
	type empty struct{}

	a := NewTableWithHintCap[int, empty](4, hash.Int)
	b := NewTableWithHintCap[empty, int](4, hash.Empty)
	c := NewTableWithHintCap[empty, empty](4, hash.Empty)

	a.Set(0, empty{})
	b.Set(empty{}, 0)
	b.Set(empty{}, 1)
	c.Set(empty{}, empty{})

	assert.Equal(t, 1, a.Len(), "empty value insert problem")
	assert.Equal(t, 1, b.Len(), "empty key insert problem")
	assert.Equal(t, 1, c.Len(), "empty key+value insert problem")
	assert.Equal(t, 1, b.Get(empty{}), "empty key returned wrong value")
}

// Tests a map with a single bucket, with same-lengthen short keys
// ("quick keys") as well as long keys.
func TestSwissTable_SingleBucketMapStringKeys_DupLen(t *testing.T) {
	m := NewTableWithHintCap[string, string](7, hash.String)
	m.Set("x", "x1val")
	m.Set("xx", "x2val")
	m.Set("foo", "fooval")
	m.Set("bar", "barval")
	m.Set("xxxx", "x4val")
	m.Set(strings.Repeat("x", 128), "longval1")
	m.Set(strings.Repeat("y", 128), "longval2")
	testSwissTableMapLookups(t, m)
}

// Tests a map with a single bucket, with all keys having different lengths.
func TestSwissTable_SingleBucketMapStringKeys_NoDupLen(t *testing.T) {
	m := NewTableWithHintCap[string, string](7, hash.String)
	m.Set("x", "x1val")
	m.Set("xx", "x2val")
	m.Set("foo", "fooval")
	m.Set("xxxx", "x4val")
	m.Set("xxxxx", "x5val")
	m.Set("xxxxxx", "x6val")
	m.Set(strings.Repeat("x", 128), "longval")
	testSwissTableMapLookups(t, m)
}

func testSwissTableMapLookups(t *testing.T, m *Table[string, string]) {
	m.Range(func(key string, value string) bool {
		require.Equalf(t, value, m.Get(key), "m[%q]", key)
		return true
	})
}

// Tests whether the iterator returns the right elements when
// started in the middle of a growth, when the keys are NaNs.
func TestSwissTable_MapNanGrowIterator(t *testing.T) {
	m := NewTable[float64, int](hash.Float64)
	nan := math.NaN()
	const nBuckets = 16
	// To fill nBuckets buckets takes LOAD * nBuckets keys.
	nKeys := nBuckets * 7 / 8

	// Get map to full point with nan keys.
	for i := range nKeys {
		m.Set(nan, i)
	}
	// Trigger grow
	m.Set(1.0, 1)
	m.Delete(1.0)

	// Run iterator
	found := make(map[int]struct{})
	m.Range(func(key float64, value int) bool {
		if value != -1 {
			_, repeat := found[value]
			require.Falsef(t, repeat, "repeat of value %d", value)
			found[value] = struct{}{}
		}
		if len(found) == nKeys/2 {
			// Halfway through iteration, finish grow.
			for range nBuckets {
				m.Delete(1.0)
			}
		}
		return true
	})
	require.Len(t, found, nKeys, "missing value")
}

func TestSwissTable_MapSparseIterOrder(t *testing.T) {
	// Run several rounds to increase the probability
	// of failure. One is not enough.
NextRound:
	for round := range 10 {
		m := NewTable[int, bool](hash.Int)
		// Add 1000 items, remove 980.
		for i := range 1000 {
			m.Set(i, true)
		}
		for i := 20; i < 1000; i++ {
			m.Delete(i)
		}

		var first []int
		m.Range(func(key int, value bool) bool {
			first = append(first, key)
			return true
		})

		// 800 chances to get a different iteration order.
		// See bug 8736 for why we need so many tries.
		for range 800 {
			idx := 0
			isNextRound := false
			m.Range(func(key int, value bool) bool {
				if key != first[idx] {
					// iteration order changed.
					isNextRound = true
					return false
				}
				idx++
				return true
			})
			if isNextRound {
				continue NextRound
			}
		}
		require.Failf(t, "constant iteration order", "round %d: %v", round, first)
	}
}

// Map iteration must not return duplicate entries.
func TestSwissTable_MapIterDuplicate(t *testing.T) {
	// Run several rounds to increase the probability
	// of failure. One is not enough.
	for range 1000 {
		m := NewTable[int, bool](hash.Int)
		// Add 1000 items, remove 980.
		for i := range 1000 {
			m.Set(i, true)
		}
		for i := 20; i < 1000; i++ {
			m.Delete(i)
		}

		var want []int
		for i := range 20 {
			want = append(want, i)
		}

		var got []int
		m.Range(func(key int, value bool) bool {
			got = append(got, key)
			return true
		})

		slices.Sort(got)
		assert.Equal(t, want, got, "iteration")
	}
}

func TestSwissTable_MapStringBytesLookup(t *testing.T) {
	// Use large string keys to avoid small-allocation coalescing,
	// which can cause AllocsPerRun to report lower counts than it should.
	m := NewTableWithHintCap[string, int](2, hash.String)
	m.Set("1000000000000000000000000000000000000000000000000", 1)
	m.Set("2000000000000000000000000000000000000000000000000", 2)
	buf := []byte("1000000000000000000000000000000000000000000000000")
	assert.Equal(t, 1, m.Get(string(buf)), `m[string([]byte("1"))]`)
	buf[0] = '2'
	assert.Equal(t, 2, m.Get(string(buf)), `m[string([]byte("2"))]`)

	var x int
	n := testing.AllocsPerRun(100, func() {
		x += m.Get(unsafe.String(&buf[0], len(buf)))
	})
	assert.Zero(t, n, "AllocsPerRun for m[string(buf)]")

	x = 0
	n = testing.AllocsPerRun(100, func() {
		y, existence := m.Lookup(unsafe.String(&buf[0], len(buf)))
		if !existence {
			panic("!ok")
		}
		x += y
	})
	assert.Zero(t, n, "AllocsPerRun for x,ok = m[string(buf)]")
}

func TestSwissTable_MapLargeKeyNoPointer(t *testing.T) {
	const (
		I = 1000
		N = 64
	)
	type T [N]int
	m := NewTable[T, int](
		func(key T, seed uint64) uint64 {
			state := seed
			for _, k := range key {
				state = hash.Int(k, state)
			}

			l := len(key)
			state = hash.Int(l, state)
			return state
		},
	)
	for i := range I {
		var v T
		for j := range N {
			v[j] = i + j
		}
		m.Set(v, i)
	}
	runtime.GC()
	for i := range I {
		var v T
		for j := range N {
			v[j] = i + j
		}
		require.Equal(t, i, m.Get(v), "corrupted map")
	}
}

func TestSwissTable_MapLargeValNoPointer(t *testing.T) {
	const (
		I = 1000
		N = 64
	)
	type T [N]int
	m := NewTable[int, T](hash.Int)
	for i := range I {
		var v T
		for j := range N {
			v[j] = i + j
		}
		m.Set(i, v)
	}
	runtime.GC()
	for i := range I {
		var v T
		for j := range N {
			v[j] = i + j
		}
		v1 := m.Get(i)
		for j := range N {
			require.Equal(t, v[j], v1[j], "corrupted map")
		}
	}
}

func TestSwissTable_DeferDeleteSlow(t *testing.T) {
	ks := []complex128{0, 1, 2, 3}

	m := NewTable[complex128, int](hash.Complex128)
	for i, k := range ks {
		m.Set(k, i)
	}
	assert.Equal(t, len(ks), m.Len(), "elements")

	func() {
		for _, k := range ks {
			defer func() {
				m.Delete(k)
			}()
		}
	}()
	assert.Zero(t, m.Len(), "elements")
}

// TestIncrementAfterDeleteValueInt and other test Issue 25936.
// Value types int, int32, int64 are affected. Value type string
// works as expected.
func TestSwissTable_IncrementAfterDeleteValueInt(t *testing.T) {
	const key1 = 12
	const key2 = 13

	m := NewTable[int, int](hash.Int)
	m.Set(key1, 99)
	m.Delete(key1)
	m.Set(key2, 1)
	n2 := m.Get(key2)
	assert.Equalf(t, 1, n2, "incremented 0 to %d", n2)
}

func TestSwissTable_IncrementAfterDeleteValueInt32(t *testing.T) {
	const key1 = 12
	const key2 = 13

	m := NewTable[int, int32](hash.Int)
	m.Set(key1, 99)
	m.Delete(key1)
	m.Set(key2, 1)
	n2 := m.Get(key2)
	assert.Equalf(t, int32(1), n2, "incremented 0 to %d", n2)
}

func TestSwissTable_IncrementAfterDeleteValueInt64(t *testing.T) {
	const key1 = 12
	const key2 = 13

	m := NewTable[int, int64](hash.Int)
	m.Set(key1, 99)
	m.Delete(key1)
	m.Set(key2, 1)
	n2 := m.Get(key2)
	assert.Equalf(t, int64(1), n2, "incremented 0 to %d", n2)
}

func TestSwissTable_IncrementAfterDeleteKeyStringValueInt(t *testing.T) {
	const key1 = ""
	const key2 = "x"

	m := NewTable[string, int](hash.String)
	m.Set(key1, 99)
	m.Delete(key1)
	m.Set(key2, 1)
	n2 := m.Get(key2)
	assert.Equalf(t, 1, n2, "incremented 0 to %d", n2)
}

func TestSwissTable_IncrementAfterDeleteKeyValueString(t *testing.T) {
	const key1 = ""
	const key2 = "x"

	m := NewTable[string, string](hash.String)
	m.Set(key1, "99")
	m.Delete(key1)
	m.Set(key2, "1")
	n2 := m.Get(key2)
	assert.Equalf(t, "1", n2, "appended '1' to empty (nil) string, got %s", n2)
}

// TestIncrementAfterBulkClearKeyStringValueInt tests that map bulk
// deletion (mapClear) still works as expected. Note that it was not
// affected by Issue 25936.
func TestSwissTable_IncrementAfterBulkClearKeyStringValueInt(t *testing.T) {
	const key1 = ""
	const key2 = "x"

	m := NewTable[string, int](hash.String)
	m.Set(key1, 99)
	m.Range(func(key string, _ int) bool {
		m.Delete(key)
		return true
	})
	m.Set(key2, 1)
	n2 := m.Get(key2)
	assert.Equalf(t, 1, n2, "incremented 0 to %d", n2)
}

func TestSwissTable_Update(t *testing.T) {
	tb := NewTable[string, int](hash.String)

	tb.Set("existing_key_1", 100)
	tb.Set("existing_key_2", 200)

	tests := []struct {
		name         string
		key          string
		newValue     int
		wantPreValue int
		wantExist    bool
	}{
		{
			name:         "Update the existing key",
			key:          "existing_key_1",
			newValue:     999,
			wantPreValue: 100,  // Old value
			wantExist:    true, // Must be existence
		},
		{
			name:         "The updated key does not exist",
			key:          "missing_key",
			newValue:     500,
			wantPreValue: 0,     // Zero value of int
			wantExist:    false, // Must be not existence
		},
		{
			name:         "Update an existing key (making sure it doesn't affect other keys)",
			key:          "existing_key_2",
			newValue:     888,
			wantPreValue: 200,
			wantExist:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			preValue, existence := tb.Update(tt.key, tt.newValue)

			assert.Equal(t, tt.wantExist, existence, "Update() existence")
			assert.Equal(t, tt.wantPreValue, preValue, "Update() preValue")

			// If the key exists, check to see if the value in the map has ACTUALLY been overwritten.
			if existence {
				currentValue, ok := tb.Lookup(tt.key)
				if assert.Truef(t, ok, "After the update, the %v key was removed from the table", tt.key) {
					assert.Equal(
						t,
						tt.newValue,
						currentValue,
						"After the update, the value stored in the table is wrong",
					)
				}
			}
		})
	}
}
