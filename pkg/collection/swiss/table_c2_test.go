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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"xxx/pkg/hash"
)

// clusterHash intentionally maps every key to the same h1 prefix so they
// all start probing from the same group. Used to stress long probe chains
// and the Robin Hood re-acquire path.
func clusterHash(key int, seed uint64) uint64 {
	// Keep upper bits identical (same group) but vary the tag bits (lower 7).
	return seed | uint64(key&0x7f)
}

func newIntTableC2() *TableC2[int, int] {
	return NewTableC2[int, int](hash.Int)
}

func newStrTableC2() *TableC2[string, int] {
	return NewTableC2[string, int](hash.String)
}

func newClusterTableC2() *TableC2[int, int] {
	return NewTableC2[int, int](clusterHash)
}

func TestTableC2_Set_NewKey_ReturnsExistenceFalse(t *testing.T) {
	t.Parallel()
	tbl := newIntTableC2()
	_, existed := tbl.Set(1, 42)
	require.False(t, existed, "new key: expected existence=false")
}

func TestTableC2_Set_UpdateKey_ReturnsOldValue(t *testing.T) {
	t.Parallel()
	tbl := newIntTableC2()
	tbl.Set(1, 42)
	preVal, existed := tbl.Set(1, 100)
	require.True(t, existed, "update: expected existence=true")
	require.Equal(t, 42, preVal, "update: expected preVal=42")
}

func TestTableC2_Set_UpdateKey_ValueIsReplaced(t *testing.T) {
	t.Parallel()
	tbl := newIntTableC2()
	tbl.Set(1, 42)
	tbl.Set(1, 100)
	got := tbl.Get(1)
	require.Equal(t, 100, got, "expected 100 after update")
}

func TestTableC2_Set_MultipleDistinctKeys(t *testing.T) {
	t.Parallel()
	tbl := newIntTableC2()
	const n = 2_000
	for i := range n {
		_, existed := tbl.Set(i, i*2)
		require.Falsef(t, existed, "key %d: should not exist yet", i)
	}
	for i := range n {
		got := tbl.Get(i)
		require.Equalf(t, i*2, got, "key %d", i)
	}
}

func TestTableC2_Set_InterleavedDeleteResize(t *testing.T) {
	t.Parallel()
	tbl := newIntTableC2()
	const n = 5_000
	// Insert half, delete, re-insert — triggers resize from deleted-heavy state.
	for i := range n {
		tbl.Set(i, i)
	}
	for i := range n / 2 {
		tbl.Delete(i)
	}
	for i := range n * 2 {
		tbl.Set(i, i*10)
	}
	for i := range n * 2 {
		got := tbl.Get(i)
		require.Equalf(t, i*10, got, "key %d", i)
	}
}

func TestTableC2_Get_ExistingKey(t *testing.T) {
	t.Parallel()
	tbl := newIntTableC2()
	tbl.Set(7, 99)
	got := tbl.Get(7)
	require.Equal(t, 99, got)
}

func TestTableC2_Get_MissingKey_EmptyTable(t *testing.T) {
	t.Parallel()
	tbl := newIntTableC2()
	got := tbl.Get(42)
	require.Zero(t, got, "expected zero value")
}

func TestTableC2_Get_MissingKey_PopulatedTable(t *testing.T) {
	t.Parallel()
	tbl := newIntTableC2()
	for i := range 100 {
		tbl.Set(i, i)
	}
	got := tbl.Get(9999)
	require.Zero(t, got, "expected zero value")
}

func TestTableC2_Get_StringKey(t *testing.T) {
	t.Parallel()
	tbl := newStrTableC2()
	tbl.Set("hello", 1)
	tbl.Set("world", 2)
	require.Equal(t, 1, tbl.Get("hello"))
	require.Equal(t, 2, tbl.Get("world"))
	require.Zero(t, tbl.Get("missing"))
}

func TestTableC2_Delete_ExistingKey(t *testing.T) {
	t.Parallel()
	tbl := newIntTableC2()
	tbl.Set(5, 50)
	tbl.Delete(5)
	got := tbl.Get(5)
	require.Zero(t, got, "expected 0 after delete")
}

func TestTableC2_Delete_NonExistingKey_NoPanic(t *testing.T) {
	t.Parallel()
	tbl := newIntTableC2()
	tbl.Delete(999) // must not panic
}

func TestTableC2_Delete_EmptyTable_NoPanic(t *testing.T) {
	t.Parallel()
	tbl := newIntTableC2()
	tbl.Delete(0) // must not panic
}

func TestTableC2_Delete_ThenReinsert(t *testing.T) {
	t.Parallel()
	tbl := newIntTableC2()
	tbl.Set(3, 30)
	tbl.Delete(3)
	_, existed := tbl.Set(3, 300)
	require.False(t, existed, "key should not exist after delete")
	got := tbl.Get(3)
	require.Equal(t, 300, got)
}

func TestTableC2_Delete_ManyThenReuse(t *testing.T) {
	t.Parallel()
	tbl := newIntTableC2()
	const n = 1_000
	for i := range n {
		tbl.Set(i, i)
	}
	for i := range n {
		tbl.Delete(i)
	}
	// Re-inserting into a table full of deleted slots must work correctly.
	for i := range n {
		_, existed := tbl.Set(i, i*3)
		require.Falsef(t, existed, "key %d should not exist after delete", i)
	}
	for i := range n {
		got := tbl.Get(i)
		require.Equalf(t, i*3, got, "key %d", i)
	}
}

func TestTableC2_Update_ExistingKey_ReturnsOldValue(t *testing.T) {
	tbl := newStrTableC2()
	tbl.Set("foo", 42)

	preValue, existence := tbl.Update("foo", 99)

	require.True(t, existence, "Update: existence must be TRUE for existing key")
	require.Equal(t, 42, preValue, "Update: expected preValue is 42")
	got := tbl.Get("foo")
	require.Equal(t, 99, got, "Update: expected value after updating is 99")
}

func TestTableC2_Update_NonExistentKey_ExistenceFalse(t *testing.T) {
	tbl := newStrTableC2()
	tbl.Set("bar", 1)

	preValue, existence := tbl.Update("baz", 100)

	require.False(t, existence, "Update: existence must be FALSE for non-existing key")
	require.Zero(t, preValue, "Update: preValue must be zero value")
	// "baz" must not be inserted.
	got := tbl.Get("baz")
	require.Zero(t, got, "Update: the key 'baz' must not be inserted")
	// Items did not increase.
	n := tbl.items.Load()
	require.EqualValues(t, 1, n, "Update: items must be 1")
}

func TestTableC2_Update_EmptyTable_NoPanic(t *testing.T) {
	tbl := newStrTableC2()

	defer func() {
		if r := recover(); r != nil {
			require.Failf(t, "Update on non-empty table is panic", "%v", r)
		}
	}()

	_, existence := tbl.Update("ghost", 7)
	require.False(t, existence, "Update on non-empty table must return existence=false")
}

func TestTableC2_Update_MultipleUpdates_SameKey(t *testing.T) {
	tbl := newStrTableC2()
	tbl.Set("x", 0)

	for i := 1; i <= 10; i++ {
		pre, ok := tbl.Update("x", i)
		require.Truef(t, ok, "update time %d: existence must be TRUE", i)
		require.Equalf(t, i-1, pre, "update time %d: expected preValue", i)
	}
	got := tbl.Get("x")
	require.Equal(t, 10, got, "the expected last value is 10")
}

func TestTableC2_Update_DoesNotIncreaseItemCount(t *testing.T) {
	tbl := newStrTableC2()
	tbl.Set("a", 1)
	tbl.Set("b", 2)
	before := tbl.items.Load()

	tbl.Update("a", 99)
	tbl.Update("b", 100)
	tbl.Update("c", 0) // non-existence key

	after := tbl.items.Load()
	require.Equal(t, before, after, "Update is not been changed items")
}

func TestTableC2_Update_Concurrent_NoDataRace(t *testing.T) {
	const numKeys = 50
	const goroutines = 20
	const iterations = 200

	tbl := newStrTableC2()
	keys := make([]string, numKeys)
	for i := range keys {
		key := string(rune('A' + i%26))
		keys[i] = key + string(rune('0'+i/26))
		tbl.Set(keys[i], i)
	}

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for iter := range iterations {
				k := keys[(id*iterations+iter)%numKeys]
				tbl.Update(k, id*1000+iter)
			}
		}(g)
	}
	wg.Wait()

	// After the go-routines finished, the existed key still find out.
	for _, k := range keys {
		_ = tbl.Get(k)
	}
}

func TestTableC2_Update_AfterDelete_ExistenceFalse(t *testing.T) {
	tbl := newStrTableC2()
	tbl.Set("del", 55)
	tbl.Delete("del")

	_, existence := tbl.Update("del", 99)
	require.False(t, existence, "Update after Delete must return existence=false")
	got := tbl.Get("del")
	require.Zero(t, got, "Update after Delete is not re-inserted")
}

func TestTableC2_Update_ConcurrentWithResize_NoPanic(t *testing.T) {
	tbl := newStrTableC2() // small capacity to trigger early resize

	const writers = 5
	const readers = 5
	const ops = 100

	// Insert all items to trigger resize.
	for i := range 20 {
		tbl.Set(string(rune(i+65)), i)
	}

	var (
		wg     sync.WaitGroup
		panics atomic.Int32
	)

	for g := range writers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					panics.Add(1)
					t.Logf("writer %d panic: %v", id, r)
				}
			}()
			for i := range ops {
				key := string(rune((id*ops+i)%26 + 65))
				tbl.Update(key, id*1000+i)
				// Insert new keys to pressure resize
				if i%10 == 0 {
					tbl.Set(key+"_"+string(rune(i+'0')), i)
				}
			}
		}(g)
	}

	for g := range readers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for range ops {
				key := string(rune(id%26 + 65))
				_ = tbl.Get(key)
			}
		}(g)
	}

	wg.Wait()

	require.Zerof(t, panics.Load(), "there is %d panic in test concurrent resize process", panics.Load())
}

func TestTableC2_Update_NoInsertWhenResizing(t *testing.T) {
	tbl := newStrTableC2()

	// Put pressure on resize to answer true.
	for i := range 6 {
		tbl.Set(string(rune(i+65)), i)
	}
	itemsBefore := tbl.items.Load()

	// Key "Z" is not exist yet.
	_, existence := tbl.Update("Z", 999)
	require.False(t, existence, "Update the new key is not existence=true")
	require.Equal(t, itemsBefore, tbl.items.Load(), "Update is not inserted")
	got := tbl.Get("Z")
	require.Zero(t, got, "Update is not inserted 'Z'")
}

// ---------------------------------------------------------------------------
// Robin Hood re-acquire path
//
// clusterHash maps all keys to the same starting group, creating long probe
// chains. Alternating deletes leave deleted slots before empty slots, which
// forces Set to find insertI in one group and the chain terminator in a
// different (later) group — the exact condition that triggers robinHoodInsert.
// ---------------------------------------------------------------------------

func TestTableC2_RobinHood_DeletedSlotsBeforeEmpty(t *testing.T) {
	t.Parallel()
	tbl := newClusterTableC2()
	const n = 64 // enough to span multiple groups (8 slots each)
	for i := range n {
		tbl.Set(i, i)
	}
	// Delete even keys → every other slot becomes deleted.
	for i := 0; i < n; i += 2 {
		tbl.Delete(i)
	}
	// Re-inserting even keys: the probe chain now starts with deleted slots
	// (in an earlier group) and an empty slot is only found in a later group,
	// so insertI != i and robinHoodInsert is called.
	for i := 0; i < n; i += 2 {
		_, existed := tbl.Set(i, i*100)
		require.Falsef(t, existed, "key %d should not exist after delete", i)
	}
	// Odd keys must be untouched.
	for i := 1; i < n; i += 2 {
		got := tbl.Get(i)
		require.Equalf(t, i, got, "odd key %d", i)
	}
	// Even keys must have the new values.
	for i := 0; i < n; i += 2 {
		got := tbl.Get(i)
		require.Equalf(t, i*100, got, "even key %d", i)
	}
}

func TestTableC2_RobinHood_FullGroupFallback(t *testing.T) {
	t.Parallel()
	// Fill the cluster table until an entire group is full, then insert a new
	// key that must probe past it. Validates the "insertI group became full,
	// restart" branch inside robinHoodInsert.
	tbl := newClusterTableC2()
	const n = 128
	for i := range n {
		tbl.Set(i, i)
	}
	for i := range n {
		got := tbl.Get(i)
		require.Equalf(t, i, got, "key %d", i)
	}
}

func TestTableC2_Concurrent_DisjointWriters(t *testing.T) {
	t.Parallel()

	tbl := newIntTableC2()
	const (
		goroutines = 8
		keysEach   = 500
	)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		g := g
		go func() {
			defer wg.Done()
			for k := g * keysEach; k < (g+1)*keysEach; k++ {
				tbl.Set(k, k)
			}
		}()
	}
	wg.Wait()
	for k := range goroutines * keysEach {
		got := tbl.Get(k)
		assert.Equalf(t, k, got, "key %d", k)
	}
}

func TestTableC2_Concurrent_ConcurrentReaders(t *testing.T) {
	t.Parallel()

	tbl := newIntTableC2()
	const n = 2_000
	for i := range n {
		tbl.Set(i, i)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for i := range n {
				got := tbl.Get(i)
				assert.Equalf(t, i, got, "key %d", i)
			}
		})
	}
	wg.Wait()
}

func TestTableC2_Concurrent_WritersAndReaders(t *testing.T) {
	t.Parallel()

	tbl := newIntTableC2()
	const n = 3_000
	var wg sync.WaitGroup
	// Writers: each goroutine owns a disjoint slice of keys.
	for g := range 4 {
		wg.Add(1)
		g := g
		go func() {
			defer wg.Done()
			for i := g; i < n; i += 4 {
				tbl.Set(i, i)
			}
		}()
	}
	// Readers: race against the writers — zero value is acceptable until
	// the writer for that key completes.
	for range 4 {
		wg.Go(func() {
			for i := range n {
				v := tbl.Get(i)
				assert.Truef(t, v == 0 || v == i, "key %d: unexpected value %d", i, v)
			}
		})
	}
	wg.Wait()
	// All writers done — every key must now be present.
	for i := range n {
		got := tbl.Get(i)
		assert.Equalf(t, i, got, "key %d after all writers done", i)
	}
}

func TestTableC2_Concurrent_WritersAndDeleters(t *testing.T) {
	t.Parallel()

	tbl := newIntTableC2()
	const n = 2_000
	for i := range n {
		tbl.Set(i, i)
	}
	var wg sync.WaitGroup
	// Writers update even keys.
	wg.Go(func() {
		for i := 0; i < n; i += 2 {
			tbl.Set(i, i*100)
		}
	})
	// Deleters remove odd keys.
	wg.Go(func() {
		for i := 1; i < n; i += 2 {
			tbl.Delete(i)
		}
	})
	wg.Wait()
	for i := 0; i < n; i += 2 {
		got := tbl.Get(i)
		assert.Equalf(t, i*100, got, "even key %d", i)
	}
	for i := 1; i < n; i += 2 {
		got := tbl.Get(i)
		assert.Zerof(t, got, "odd key %d: expected 0 after delete", i)
	}
}

func TestTableC2_Concurrent_ResizeUnderLoad(t *testing.T) {
	t.Parallel()

	tbl := newIntTableC2()
	const n = 10_000
	var wg sync.WaitGroup
	// Multiple goroutines inserting causes repeated resizes.
	for g := range 8 {
		wg.Add(1)
		g := g
		go func() {
			defer wg.Done()
			for i := g; i < n; i += 8 {
				tbl.Set(i, i)
			}
		}()
	}
	wg.Wait()
	for i := range n {
		got := tbl.Get(i)
		assert.Equalf(t, i, got, "key %d", i)
	}
}

func TestTableC2_Concurrent_SameKey_NoCorruption(t *testing.T) {
	t.Parallel()
	// Many goroutines writing the same key: the final value must be one of
	// the values that was actually written (no silent corruption or panic).
	tbl := newIntTableC2()
	const goroutines = 32
	valid := make(map[int]bool, goroutines)
	for g := range goroutines {
		valid[g] = true
	}
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		g := g
		go func() {
			defer wg.Done()
			tbl.Set(0, g)
		}()
	}
	wg.Wait()
	got := tbl.Get(0)
	require.Truef(t, valid[got], "corrupted value %d: not written by any goroutine", got)
}

func TestTableC2_Concurrent_Stress(t *testing.T) {
	t.Parallel()

	m := NewTableC2[int, int](hash.Int)
	var wg sync.WaitGroup
	numWorkers := 50
	opsPerWorker := 1000

	// Mix Set, Get, and Delete commands from multiple threads to enable activation.
	for i := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := range opsPerWorker {
				key := (workerID * opsPerWorker) + j

				// Set
				m.Set(key, key)

				// Read what was just set
				val := m.Get(key)
				assert.Equal(t, key, val, "Concurrent Read Failed")

				// Delete some keys to trigger tombstone logic
				if j%2 == 0 {
					m.Delete(key)
				}
			}
		}(i)
	}

	wg.Wait()

	// Check the integrity of the data again (half of the keys must remain).
	expectedItems := int32((numWorkers * opsPerWorker) / 2)
	assert.Equal(t, expectedItems, m.items.Load(), "items")
}

// countTags scans every group's control word directly (white-box) and
// returns the live counts of empty / deleted / full slots across the
// whole table.
func countTags[K comparable, V any](tbl *TableC2[K, V]) (empty, deleted, full int) {
	storage := tbl.storage.Load()
	for gi := range storage.groups {
		ctrl := storage.groups[gi].control
		for slot := range uint(8) {
			switch ctrl.GetTag(slot) {
			case tagEmpty:
				empty++
			case tagDeleted:
				deleted++
			default:
				full++
			}
		}
	}
	return
}

// TestGrowthLeftVsEmptyDivergence uses a SLIDING WINDOW of live keys
// (insert key i, delete key i-window) instead of cycling the exact same
// keys in place — a fixed round-robin on identical keys always maps back
// to the same slot and never stresses the erase() boundary heuristic.
// A sliding window keeps the item count bounded (steady-state, well under
// growthLeft's budget) while still spreading wear across many different
// slots/groups, which is what real connect/disconnect churn looks like.
func TestTableC2_GrowthLeftVsEmptyDivergence(t *testing.T) {
	const capacity = 512 // -> 64 groups
	tbl := NewTableC2WithCap[int, int](capacity, hash.Int)

	// Fill to 95% of Cap() — as close to the load-factor ceiling as
	// possible, since erase() ">=8 contiguous" heuristic needs busy
	// neighborhoods to ever fire.
	window := int(float64(tbl.Cap()) * 0.95)
	for i := range window {
		tbl.Insert(i, i)
	}
	t.Logf("Cap()=%d window=%d after warm-up growthLeft=%d", tbl.Cap(), window, tbl.growthLeft.Load())

	prevStorage := tbl.storage.Load()
	resizeEvents := 0

	// Reuse the SAME `window` keys over and over (not a sliding window of
	// ever-new keys) — this is the pattern that should NOT need any virgin
	// -empty slot after warm-up, so growthLeft should barely move if its
	// accounting really is tracking "current empty slots".
	const cycles = 3_000_000
	minEverEmpty := 1 << 30
	for c := range cycles {
		k := c % window
		_, ok := tbl.Delete(k)
		require.Truef(t, ok, "cycle %d: key %d unexpectedly missing", c, k)
		existed := tbl.Insert(k, k)
		require.Falsef(t, existed, "cycle %d: key %d unexpectedly already present", c, k)

		cur := tbl.storage.Load()
		if cur != prevStorage {
			resizeEvents++
			prevStorage = cur
		}

		if c%50_000 == 0 || c == cycles-1 {
			empty, deleted, full := countTags(tbl)
			gl := tbl.growthLeft.Load()
			t.Logf(
				"cycle=%d empty=%d deleted=%d full=%d growthLeft=%d resizeEventsSoFar=%d",
				c, empty, deleted, full, gl, resizeEvents,
			)
			if empty < minEverEmpty {
				minEverEmpty = empty
			}
			if empty == 0 {
				t.Logf("*** empty=0 reached; growthLeft=%d ***", gl)
				break
			}
		}
	}
	t.Logf(
		"min empty observed=%d, total resize events over %d cycles=%d, final growthLeft=%d",
		minEverEmpty, cycles, resizeEvents, tbl.growthLeft.Load(),
	)
}

// TestInsertHangUnderTombstoneStarvation drives the table into the
// empty=0/growthLeft>0 state, then attempts Insert() of a brand-new key
// (never part of the churned set) under a timeout, to see whether the
// probe loop actually spins forever rather than just being slow.
func TestTableC2_InsertHangUnderTombstoneStarvation(t *testing.T) {
	const capacity = 16 // 2 groups, saturates fast
	tbl := NewTableC2WithCap[int, int](capacity, hash.Int)

	keySetSize := int(float64(tbl.Cap()) * 0.95)
	for i := range keySetSize {
		tbl.Insert(i, i)
	}

	const maxCycles = 5_000_000
	found := false
	for c := range maxCycles {
		k := c % keySetSize
		tbl.Delete(k)
		tbl.Insert(k, k)
		if c%2000 == 0 {
			empty, _, _ := countTags(tbl)
			if empty == 0 {
				found = true
				t.Logf("empty=0 reached after %d cycles, growthLeft=%d", c, tbl.growthLeft.Load())
				break
			}
		}
	}
	if !found {
		t.Skip("did not reach empty=0 within maxCycles; tune capacity/keySetSize")
	}

	done := make(chan struct{})
	go func() {
		tbl.Insert(-1, -1) // brand-new key, not in the churned set
		close(done)
	}()

	select {
	case <-done:
		t.Log("Insert() of a new key returned normally")
	case <-time.After(5 * time.Second):
		require.Fail(t, "Insert() of a new key did not return within 5s — probe loop appears to spin forever")
	}
}

// TestConcurrentChurnNoHang runs many goroutines concurrently deleting and
// re-inserting keys from a shared, bounded key range at high load factor,
// for a fixed wall-clock budget, and fails if the whole run doesn't finish
// in time (a crude but effective livelock detector for a table that's
// supposed to be lock-free at the table level / lock-per-group).
func TestTableC2_ConcurrentChurnNoHang(t *testing.T) {
	const capacity = 4096 // -> 512 groups
	tbl := NewTableC2WithCap[int, int](capacity, hash.Int)

	window := int(float64(tbl.Cap()) * 0.9)
	for i := range window {
		tbl.Insert(i, i)
	}
	t.Logf("Cap()=%d window=%d growthLeft=%d", tbl.Cap(), window, tbl.growthLeft.Load())

	const goroutines = 32
	const perGoroutine = 50_000
	var wg sync.WaitGroup
	var completed atomic.Int64

	start := time.Now()
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range perGoroutine {
				k := (g*perGoroutine + i) % window
				tbl.Delete(k)
				tbl.Insert(k, k)
				completed.Add(1)
			}
		}(g)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Logf(
			"all %d ops completed in %v, final growthLeft=%d, Len()=%d",
			goroutines*perGoroutine, time.Since(start), tbl.growthLeft.Load(), tbl.Len(),
		)
	case <-time.After(60 * time.Second):
		require.Failf(
			t, "concurrent churn did not finish within 60s",
			"completed %d/%d ops, likely livelock in Set/Insert probing",
			completed.Load(), goroutines*perGoroutine,
		)
	}
}
