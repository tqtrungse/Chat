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
 *
 * Parts of the erase/resize logic are adapted from hashbrown
 * (https://github.com/rust-lang/hashbrown), Copyright (c) 2016 Amanieu
 * d'Antras, used under the MIT license. See THIRD_PARTY_NOTICES.
 */

package swiss

import (
	"math"
	"math/bits"
	"sync"
	"sync/atomic"

	"xxx/pkg"
	"xxx/pkg/backoff"
	"xxx/pkg/hash"
)

type groupC2[K comparable, V any] struct {
	mtx     sync.RWMutex
	control control
	isMoved bool
	buckets [8]bucket[K, V]
}

func newGroupC2[K comparable, V any]() groupC2[K, V] {
	return groupC2[K, V]{
		mtx:     sync.RWMutex{},
		control: repeatTag(tagEmpty),
		buckets: [8]bucket[K, V]{},
		isMoved: false,
	}
}

type TableC2[K comparable, V any] struct {
	seed uint64
	h    HashFunc[K]
	_    [pkg.CacheLineSize - pkg.PtrSize - 8]byte

	storage    atomic.Pointer[storageC2[K, V]]
	resizeWait atomic.Pointer[chan struct{}]
	_          [pkg.CacheLineSize - 2*pkg.PtrSize]byte

	growthLeft atomic.Int32
	_          [pkg.CacheLineSize - 4]byte

	items atomic.Int32
	_     [pkg.CacheLineSize - 4]byte
}

// NewTableC2
//
// hintCapacity is a capacity hint helps the map select the appropriate initial bucket size to
// reduce the number of grow/rehash cycles.
func NewTableC2[K comparable, V any](h HashFunc[K]) *TableC2[K, V] {
	return NewTableC2WithHintCap[K, V](0, h)
}

// NewTableC2WithHintCap
//
// hintCapacity is a capacity hint helps the map select the appropriate initial bucket size to
// reduce the number of grow/rehash cycles.
func NewTableC2WithHintCap[K comparable, V any](hintCap int, h HashFunc[K]) *TableC2[K, V] {
	numAllocBuckets := getAllocBuckets(int(math.Ceil(float64(hintCap) / 7)))
	t := &TableC2[K, V]{
		seed: hash.MakeSeed(),
		h:    h,
	}
	t.storage.Store(newStorageC2[K, V](numAllocBuckets))
	t.growthLeft.Store(int32(getActualUsedBuckets(numAllocBuckets - 1)))
	return t
}

func NewTableC2WithCap[K comparable, V any](cap int, h HashFunc[K]) *TableC2[K, V] {
	numAllocBuckets := getAllocBuckets(cap)
	t := &TableC2[K, V]{
		seed: hash.MakeSeed(),
		h:    h,
	}
	t.storage.Store(newStorageC2[K, V](numAllocBuckets))
	t.growthLeft.Store(int32(getActualUsedBuckets(numAllocBuckets - 1)))
	return t
}

func (t *TableC2[K, V]) Set(key K, val V) (preVal V, existence bool) {
	var (
		hashed  = t.h(key, t.seed)
		taG     = newFullTag(hashed)
		storage = t.storage.Load()
		grMask  = len(storage.groups) - 1
		i       = int(h1(hashed)&uint64(storage.bucketMask)) >> 3
	)

	for {
		gr := &storage.groups[i]
		gr.mtx.Lock()

		if gr.isMoved {
			gr.mtx.Unlock()
			t.wait()
			storage, i, grMask = t.reload(hashed)
			continue
		}

		ctrl := gr.control
		iter := ctrl.MatchTag(taG).NewIter()
		for j := iter.Next(); j != -1; j = iter.Next() {
			if gr.buckets[j].key == key {
				existence = true
				preVal = gr.buckets[j].val
				gr.buckets[j].val = val
				gr.mtx.Unlock()
				return
			}
		}

		// j == -1 -> the group is full.
		// Otherwise, there is the least one slot is empty or deleted.
		// If there is no empty slot in the group, continue probe chain.
		// Otherwise, we have the least one deleted slot and the least one empty slot.
		// An empty slot in the current group means the probe chain ends here —
		// the key is definitely not present anywhere ahead.
		j := ctrl.MatchEmptyOrDeleted().LowestSetBit()
		if j == -1 || !ctrl.MatchEmpty().AnyBitSet() {
			gr.mtx.Unlock()
			i = (i + 1) & grMask
			continue
		}

		// If j is deleted slot.
		if !ctrl.GetTag(uint(j)).SpecialIsEmpty() {
			storage.insertAt(key, val, taG, i, j)
			t.items.Add(1)
			gr.mtx.Unlock()
			return
		}

		// If j is empty slot.
		growthLeft := t.growthLeft.Add(-1)
		if growthLeft > 0 {
			storage.insertAt(key, val, taG, i, j)
			t.items.Add(1)
			gr.mtx.Unlock()
			return
		}

		// Need to resize table.
		if growthLeft == 0 {
			gr.mtx.Unlock()
			t.resize()
			// If we lost the resize() CAS (another goroutine's resize is
			// already running), resize() returns immediately without
			// waiting. Without this wait, we'd reload and continue against
			// storage that hasn't been updated yet, potentially re-reading
			// growthLeft==0 again before the in-flight resize finishes and
			// resets it — losing the CAS race repeatedly without ever
			// blocking. wait() is a no-op if we actually won the CAS
			// (resize() already fully completed by the time it returns, so
			// resizeWait is already nil).
			t.wait()
			storage, i, grMask = t.reload(hashed)
			continue
		}

		// Re-add 1 to keep growthLeft >= 0.
		t.growthLeft.Add(1)
		gr.mtx.Unlock()

		// If multiple threads join minus growthLeft and only one thread is resized,
		// the other threads may not receive resizeWait immediately, leading to continuous retry.
		// Add sleep to get resizeWait.
		bkc := backoff.Cpu{}
		for {
			chPtr := t.resizeWait.Load()
			if chPtr == nil {
				// Resizing is done.
				if t.storage.Load() != storage {
					break
				}
				bkc.Snooze()
				continue
			}
			<-*chPtr
			break
		}
		storage, i, grMask = t.reload(hashed)
		continue // Back to the beginning.
	}
}

func (t *TableC2[K, V]) Insert(key K, val V) (existence bool) {
	var (
		hashed  = t.h(key, t.seed)
		taG     = newFullTag(hashed)
		storage = t.storage.Load()
		grMask  = len(storage.groups) - 1
		i       = int(h1(hashed)&uint64(storage.bucketMask)) >> 3
	)

	for {
		gr := &storage.groups[i]
		gr.mtx.Lock()

		if gr.isMoved {
			gr.mtx.Unlock()
			t.wait()
			storage, i, grMask = t.reload(hashed)
			continue
		}

		ctrl := gr.control
		iter := ctrl.MatchTag(taG).NewIter()
		for j := iter.Next(); j != -1; j = iter.Next() {
			if gr.buckets[j].key == key {
				existence = true
				gr.mtx.Unlock()
				return
			}
		}

		// j == -1 -> the group is full.
		// Otherwise, there is the least one slot is empty or deleted.
		// If there is no empty slot in the group, continue probe chain.
		// Otherwise, we have the least one deleted slot and the least one empty slot.
		// An empty slot in the current group means the probe chain ends here —
		// the key is definitely not present anywhere ahead.
		j := ctrl.MatchEmptyOrDeleted().LowestSetBit()
		if j == -1 || !ctrl.MatchEmpty().AnyBitSet() {
			gr.mtx.Unlock()
			i = (i + 1) & grMask
			continue
		}

		// If j is deleted slot.
		if !ctrl.GetTag(uint(j)).SpecialIsEmpty() {
			storage.insertAt(key, val, taG, i, j)
			t.items.Add(1)
			gr.mtx.Unlock()
			return
		}

		// If j is empty slot.
		growthLeft := t.growthLeft.Add(-1)
		if growthLeft > 0 {
			storage.insertAt(key, val, taG, i, j)
			t.items.Add(1)
			gr.mtx.Unlock()
			return
		}

		// Need to resize table.
		if growthLeft == 0 {
			gr.mtx.Unlock()
			t.resize()
			// See the identical comment in Set(): without this wait, losing
			// the resize() CAS here means reloading and continuing without
			// ever blocking on the resize that's actually in flight.
			t.wait()
			storage, i, grMask = t.reload(hashed)
			continue
		}

		// Re-add 1 to keep growthLeft >= 0.
		t.growthLeft.Add(1)
		gr.mtx.Unlock()

		// If multiple threads join minus growthLeft and only one thread is resized,
		// the other threads may not receive resizeWait immediately, leading to continuous retry.
		// Add sleep to get resizeWait.
		bkc := backoff.Cpu{}
		for {
			chPtr := t.resizeWait.Load()
			if chPtr == nil {
				// Resizing is done.
				if t.storage.Load() != storage {
					break
				}
				bkc.Snooze()
				continue
			}
			<-*chPtr
			break
		}
		storage, i, grMask = t.reload(hashed)
		continue // Back to the beginning.
	}
}

func (t *TableC2[K, V]) Update(key K, val V) (preVal V, existence bool) {
	if t.items.Load() == 0 {
		return
	}

	var (
		hashed  = t.h(key, t.seed)
		storage = t.storage.Load()
		taG     = newFullTag(hashed)
		grMask  = len(storage.groups) - 1
		i       = int(h1(hashed)&uint64(storage.bucketMask)) >> 3
	)

	for {
		gr := &storage.groups[i]
		gr.mtx.Lock()

		if gr.isMoved {
			gr.mtx.Unlock()
			t.wait()
			storage, i, grMask = t.reload(hashed)
			continue
		}

		ctrl := gr.control
		iter := ctrl.MatchTag(taG).NewIter()
		for j := iter.Next(); j != -1; j = iter.Next() {
			if gr.buckets[j].key == key {
				existence = true
				preVal = gr.buckets[j].val
				gr.buckets[j].val = val
				gr.mtx.Unlock()
				return
			}
		}

		if ctrl.MatchEmpty().AnyBitSet() {
			gr.mtx.Unlock()
			return
		}

		gr.mtx.Unlock()
		i = (i + 1) & grMask
	}
}

func (t *TableC2[K, V]) Delete(key K) (val V, existence bool) {
	if t.items.Load() == 0 {
		return
	}

	var (
		hashed  = t.h(key, t.seed)
		storage = t.storage.Load()
		taG     = newFullTag(hashed)
		grMask  = len(storage.groups) - 1
		i       = int(h1(hashed)&uint64(storage.bucketMask)) >> 3
	)

	for {
		gr := &storage.groups[i]
		gr.mtx.RLock()

		if gr.isMoved {
			gr.mtx.RUnlock()
			t.wait()
			storage, i, grMask = t.reload(hashed)
			continue
		}

		ctrl := gr.control
		matched := -1
		iter := ctrl.MatchTag(taG).NewIter()
		for j := iter.Next(); j != -1; j = iter.Next() {
			if gr.buckets[j].key == key {
				matched = j
				break
			}
		}

		if matched == -1 {
			if ctrl.MatchEmpty().AnyBitSet() {
				gr.mtx.RUnlock()
				return
			}
			gr.mtx.RUnlock()
			i = (i + 1) & grMask
			continue
		}

		// Only groups with a match require a write-lock, and only for this brief moment.
		gr.mtx.RUnlock()
		gr.mtx.Lock()
		if !gr.isMoved && gr.buckets[matched].key == key {
			val = t.erase(storage, i, matched)
			gr.mtx.Unlock()
			return val, true
		}
		// Taken by another goroutine during RUnlock/Lock — return to scanning.
		// group this from the beginning using RLock (isMoved will be processed properly
		// at the beginning of the round).
		gr.mtx.Unlock()
		continue
	}
}

func (t *TableC2[K, V]) Get(key K) (val V) {
	if t.items.Load() == 0 {
		return
	}

	var (
		storage   = t.storage.Load()
		groupMask = uint(len(storage.groups)) - 1
		hashed    = t.h(key, t.seed)
		taG       = newFullTag(hashed)
		i         = uint(h1(hashed)&uint64(storage.bucketMask)) >> 3
	)

	for {
		gr := &storage.groups[i]
		gr.mtx.RLock()
		ctrl := gr.control
		iter := ctrl.MatchTag(taG).NewIter()
		for j := iter.Next(); j != -1; j = iter.Next() {
			if gr.buckets[j].key == key {
				val = gr.buckets[j].val
				gr.mtx.RUnlock()
				return
			}
		}
		if ctrl.MatchEmpty().AnyBitSet() {
			gr.mtx.RUnlock()
			return
		}
		gr.mtx.RUnlock()
		i = (i + 1) & groupMask
	}
}

func (t *TableC2[K, V]) Lookup(key K) (val V, existence bool) {
	if t.items.Load() == 0 {
		return
	}

	var (
		storage   = t.storage.Load()
		groupMask = uint(len(storage.groups)) - 1
		hashed    = t.h(key, t.seed)
		taG       = newFullTag(hashed)
		i         = uint(h1(hashed)&uint64(storage.bucketMask)) >> 3
	)

	for {
		gr := &storage.groups[i]
		gr.mtx.RLock()
		ctrl := gr.control
		iter := ctrl.MatchTag(taG).NewIter()
		for j := iter.Next(); j != -1; j = iter.Next() {
			if gr.buckets[j].key == key {
				existence = true
				val = gr.buckets[j].val
				gr.mtx.RUnlock()
				return
			}
		}
		if ctrl.MatchEmpty().AnyBitSet() {
			gr.mtx.RUnlock()
			return
		}
		gr.mtx.RUnlock()
		i = (i + 1) & groupMask
	}
}

func (t *TableC2[K, V]) Range(cb func(key K, val V) (goOn bool)) {
	// Load the storage once. A concurrent resize will swap in a new storage, but
	// the old storage's data is not cleared (GC reclaims it after all references
	// drop), so we can safely read every group in this snapshot.
	storage := t.storage.Load()

	i := uint(hash.MakeSeed() & uint64(len(storage.groups)-1))
	grMask := uint(len(storage.groups)) - 1

	// Use fixed-size stack arrays. A group holds at most 8 live entries, so
	// no heap allocation is needed per group.
	var (
		keys  [8]K
		vals  [8]V
		count uint
	)

	for range storage.groups {
		gr := &storage.groups[i]
		j := uint(hash.MakeSeed() & 7) // hash.MakeSeed() % 8

		// Hold the read lock only long enough to copy live entries.
		// RLock allows other goroutines to read concurrently; writes on this
		// group are serialized but blocked only for this brief copy window.
		gr.mtx.RLock()
		count = 0
		for range 8 {
			if gr.control.GetTag(j).IsFull() {
				keys[count] = gr.buckets[j].key
				vals[count] = gr.buckets[j].val
				count++
			}
			j = (j + 1) & 7
		}
		gr.mtx.RUnlock()

		// Call fn outside the lock so it can freely use the table.
		for k := uint(0); k < count; k++ {
			if !cb(keys[k], vals[k]) {
				return
			}
		}
		i = (i + 1) & grMask
	}
}

func (t *TableC2[K, V]) Len() int {
	return int(t.items.Load())
}

func (t *TableC2[K, V]) Cap() uint {
	return getActualUsedBuckets(t.storage.Load().bucketMask)
}

func (t *TableC2[K, V]) Seed() uint64 {
	return t.seed
}

func (t *TableC2[K, V]) resize() {
	newItems, carry := bits.Add(uint(t.items.Load()), 1, 0)
	if carry != 0 {
		panic("overflow capacity")
	}

	ch := make(chan struct{})
	// Must have checked before store resizeWait.
	// Only one go-routines in Set operation updated t.growthLeft is zero can resize,
	// but Delete operation can increase t.growthLeft and lead to t.growthLeft is 1.
	// Just one go-routine in Set operation comes and updates t.growthLeft is zero,
	// resize will be called while the resizing process is running.
	if !t.resizeWait.CompareAndSwap(nil, &ch) {
		return
	}

	var (
		numAllocBuckets      uint
		storage              = t.storage.Load()
		numActualUsedBuckets = getActualUsedBuckets(storage.bucketMask)
	)

	if newItems <= numActualUsedBuckets>>1 {
		numAllocBuckets = getAllocBuckets(int(numActualUsedBuckets))
	} else {
		numAllocBuckets = getAllocBuckets(int(max(newItems, numActualUsedBuckets+1)))
		numActualUsedBuckets = getActualUsedBuckets(numAllocBuckets - 1)
	}

	newStorage := newStorageC2[K, V](numAllocBuckets)
	t.moveGroups(storage, newStorage)
	// growthLeft is affected by inserting, deleting, and items.
	// The resize has been published, and inserts into the group that haven't been moved yet can still happen,
	// but they will be caught by the mover.
	// Deleting operations can affect to items. But at this step, all groups were moved.
	// In other words, all previous deleting operations were finished, and all new deleting operations are waiting.
	t.growthLeft.Store(int32(numActualUsedBuckets) - t.items.Load())
	t.storage.Store(newStorage)
	t.resizeWait.Store(nil)
	// SIGNAL: Close the channel to wake everyone up at once.
	close(ch)
	return
}

func (t *TableC2[K, V]) wait() {
	if chPtr := t.resizeWait.Load(); chPtr != nil {
		<-*chPtr
	}
}

func (t *TableC2[K, V]) reload(hashed uint64) (storage *storageC2[K, V], i, grMask int) {
	storage = t.storage.Load()
	grMask = len(storage.groups) - 1
	i = int(h1(hashed)&uint64(storage.bucketMask)) >> 3
	return
}

func (t *TableC2[K, V]) erase(storage *storageC2[K, V], i, j int) (preValue V) {
	var (
		preI         = i - 1
		taG          = tagDeleted
		gr           = &storage.groups[i]
		bitmaskEmpty = gr.control.MatchEmpty()
	)

	if preI < 0 {
		preI = int((storage.bucketMask+1)>>3) - 1
	}

	if storage.groups[preI].mtx.TryRLock() {
		preBitmaskEmpty := storage.groups[preI].control.MatchEmpty()
		if preBitmaskEmpty.LeadingZeros()+bitmaskEmpty.TrailingZeros() < 8 {
			taG = tagEmpty
			t.growthLeft.Add(1)
		}
		storage.groups[preI].mtx.RUnlock()
	}
	preValue = gr.buckets[j].val
	gr.control = setTag(gr.control, uint(j), taG)
	gr.buckets[j].key = pkg.ZeroValue[K]()
	gr.buckets[j].val = pkg.ZeroValue[V]()
	t.items.Add(-1)
	return
}

func (t *TableC2[K, V]) moveGroups(storage, newStorage *storageC2[K, V]) {
	for i := 0; i < len(storage.groups); i++ {
		gr := &storage.groups[i]
		gr.mtx.Lock()
		storage.moveGroup(newStorage, t.h, t.seed, i)
		gr.mtx.Unlock()
	}
}

type storageC2[K comparable, V any] struct {
	bucketMask uint
	groups     []groupC2[K, V]
}

func newStorageC2[K comparable, V any](numAllocBuckets uint) *storageC2[K, V] {
	storage := &storageC2[K, V]{
		bucketMask: numAllocBuckets - 1,
	}
	storage.groups = make([]groupC2[K, V], numAllocBuckets>>3)
	for i := range storage.groups {
		storage.groups[i] = newGroupC2[K, V]()
	}
	return storage
}

func (s *storageC2[K, V]) moveGroup(newStorage *storageC2[K, V], h HashFunc[K], seed uint64, i int) {
	gr := &s.groups[i]
	iter := gr.control.MatchFull().NewIter()
	for j := iter.Next(); j != -1; j = iter.Next() {
		hashed := h(gr.buckets[j].key, seed)
		newI, newJ := newStorage.prepareInsertIndex(hashed)
		// Don't need to reset gr.buckets[j] because someone can be using it.
		// GC will clean up when no one uses gr.buckets[j].
		newStorage.groups[newI].buckets[newJ] = gr.buckets[j]
	}
	gr.isMoved = true
}

func (s *storageC2[K, V]) prepareInsertIndex(hashed uint64) (i, j int) {
	i, j = s.findInsertIndex(hashed)
	s.groups[i].control = setTag(
		s.groups[i].control,
		uint(j),
		newFullTag(hashed),
	)
	return
}

func (s *storageC2[K, V]) findInsertIndex(hashed uint64) (int, int) {
	var (
		grMask = uint(len(s.groups)) - 1
		i      = uint(h1(hashed)&uint64(s.bucketMask)) >> 3
	)

	// Always find out the index because we have allocated more 12.5% empty bucket.
	for {
		gr := &s.groups[i]
		j := gr.control.MatchEmptyOrDeleted().LowestSetBit()
		if j != -1 {
			return int(i), j
		}
		i = (i + 1) & grMask
	}
}

func (s *storageC2[K, V]) insertAt(key K, val V, taG tag, i int, j int) (preTag tag) {
	gr := &s.groups[i]
	gr.control, preTag = replaceTag(gr.control, uint(j), taG)
	gr.buckets[j] = bucket[K, V]{
		key: key,
		val: val,
	}
	return preTag
}
