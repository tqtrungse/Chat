/*
 * Copyright (c) 2016 Amanieu d'Antras
 * Copyright (c) 2026 tqtrungse@gmail.com
 *
 * Derived from Rust hashbrown:
 * https://github.com/rust-lang/hashbrown
 *
 * This file contains modifications and a Go port of the original
 * hashbrown implementation.
 *
 * Licensed under either the Apache License, Version 2.0 or the MIT License.
 */

package swiss

import (
	"math"
	"math/bits"
	_ "unsafe"

	"xxx/pkg"
	"xxx/pkg/hash"
)

type HashFunc[K comparable] func(key K, seed uint64) uint64

type bucket[K comparable, V any] struct {
	key K
	val V
}

type group[K comparable, V any] struct {
	control control
	buckets [8]bucket[K, V]
}

func newGroup[K comparable, V any]() group[K, V] {
	return group[K, V]{
		control: repeatTag(tagEmpty),
	}
}

type Table[K comparable, V any] struct {
	seed uint64

	// It is power of two and equal to number allocated buckets - 1.
	// The capacity which user input equals to 87.5% of number allocated buckets.
	bucketMask uint

	// Number remaining keys can be inserted to the table.
	growthLeft uint

	// Number current keys is being in the table.
	items uint

	h      HashFunc[K]
	groups []group[K, V]
}

// NewTable
//
// hintCapacity is a capacity hint helps the map select the appropriate initial bucket size to
// reduce the number of grow/rehash cycles.
func NewTable[K comparable, V any](h HashFunc[K]) *Table[K, V] {
	return NewTableWithHintCap[K, V](0, h)
}

// NewTableWithHintCap
//
// hintCapacity is a capacity hint helps the map select the appropriate initial bucket size to
// reduce the number of grow/rehash cycles.
func NewTableWithHintCap[K comparable, V any](hintCap int, h HashFunc[K]) *Table[K, V] {
	numAllocBuckets := getAllocBuckets(int(math.Ceil(float64(hintCap) / 8)))
	groups := make([]group[K, V], numAllocBuckets>>3) // divided by 8 (a group has 8 buckets).
	for i := range groups {
		groups[i] = newGroup[K, V]()
	}

	return &Table[K, V]{
		seed:       hash.MakeSeed(),
		bucketMask: numAllocBuckets - 1,
		growthLeft: getActualUsedBuckets(numAllocBuckets - 1),
		items:      0,
		h:          h,
		groups:     groups,
	}
}

func NewTableWithCap[K comparable, V any](cap int, h HashFunc[K]) *Table[K, V] {
	numAllocBuckets := getAllocBuckets(cap)
	groups := make([]group[K, V], numAllocBuckets>>3) // divided by 8 (a group has 8 buckets).
	for i := range groups {
		groups[i] = newGroup[K, V]()
	}

	return &Table[K, V]{
		seed:       hash.MakeSeed(),
		bucketMask: numAllocBuckets - 1,
		growthLeft: getActualUsedBuckets(numAllocBuckets - 1),
		items:      0,
		h:          h,
		groups:     groups,
	}
}

func (t *Table[K, V]) Set(key K, val V) (preVal V, existence bool) {
	t.Reserve(1)

	var (
		foundInsertIndex bool
		insertI          int
		insertJ          int
		hashed           = t.h(key, t.seed)
		grMask           = len(t.groups) - 1
		taG              = newFullTag(hashed)
		i                = int(h1(hashed)&uint64(t.bucketMask)) >> 3
	)

	for {
		gr := &t.groups[i]
		ctrl := gr.control
		iter := ctrl.MatchTag(taG).NewIter()

		for j := iter.Next(); j != -1; j = iter.Next() {
			if gr.buckets[j].key == key {
				existence = true
				preVal = t.groups[i].buckets[j].val
				t.groups[i].buckets[j].val = val
				return
			}
		}
		if !foundInsertIndex {
			j := ctrl.MatchEmptyOrDeleted().LowestSetBit()
			if j != -1 {
				foundInsertIndex = true
				insertI = i
				insertJ = j
			}
		}
		if foundInsertIndex {
			if ctrl.MatchEmpty().AnyBitSet() {
				t.insertAt(key, val, newFullTag(hashed), insertI, insertJ)
				return
			}
		}
		i = (i + 1) & grMask
	}
}

func (t *Table[K, V]) Update(key K, val V) (preVal V, existence bool) {
	var (
		hashed = t.h(key, t.seed)
		grMask = len(t.groups) - 1
		taG    = newFullTag(hashed)
		i      = int(h1(hashed)&uint64(t.bucketMask)) >> 3
	)

	for {
		gr := &t.groups[i]
		ctrl := gr.control
		iter := ctrl.MatchTag(taG).NewIter()

		for j := iter.Next(); j != -1; j = iter.Next() {
			if gr.buckets[j].key == key {
				existence = true
				preVal = t.groups[i].buckets[j].val
				t.groups[i].buckets[j].val = val
				return
			}
		}
		if ctrl.MatchEmpty().AnyBitSet() {
			return
		}
		i = (i + 1) & grMask
	}
}

// Get
//
// NOTE: If K is string and we to lookup "string(buf)", don't pass directly string(buf) to Get.
// It is not optimize as std map, it will allocate and copy byte buffer to string.
// Use unsafe.String(&buf[0], len(buf)) to replace string(buf). It doesn't allocate any memory.
func (t *Table[K, V]) Get(key K) (val V) {
	if t.items == 0 {
		return
	}

	var (
		groupMask = uint(len(t.groups)) - 1
		hashed    = t.h(key, t.seed)
		taG       = newFullTag(hashed)
		i         = uint(h1(hashed)&uint64(t.bucketMask)) >> 3
	)

	for {
		gr := &t.groups[i]
		ctrl := gr.control
		iter := ctrl.MatchTag(taG).NewIter()

		for j := iter.Next(); j != -1; j = iter.Next() {
			if gr.buckets[j].key == key {
				return gr.buckets[j].val
			}
		}
		if ctrl.MatchEmpty().AnyBitSet() {
			return
		}
		i = (i + 1) & groupMask
	}
}

// Lookup
//
// NOTE: If K is string and we to lookup "string(buf)", don't pass directly string(buf) to Get.
// It is not optimize as std map, it will allocate and copy byte buffer to string.
// Use unsafe.String(&buf[0], len(buf)) to replace string(buf). It doesn't allocate any memory.
func (t *Table[K, V]) Lookup(key K) (val V, existence bool) {
	if t.items == 0 {
		return
	}

	var (
		groupMask = uint(len(t.groups)) - 1
		hashed    = t.h(key, t.seed)
		taG       = newFullTag(hashed)
		i         = uint(h1(hashed)&uint64(t.bucketMask)) >> 3
	)

	for {
		gr := &t.groups[i]
		ctrl := gr.control
		iter := ctrl.MatchTag(taG).NewIter()

		for j := iter.Next(); j != -1; j = iter.Next() {
			if gr.buckets[j].key == key {
				existence = true
				val = gr.buckets[j].val
				return
			}
		}
		if ctrl.MatchEmpty().AnyBitSet() {
			return
		}
		i = (i + 1) & groupMask
	}
}

//func (t *Table[K, V]) LookupRef(key K) (value *V, existence bool) {
//	if t.items == 0 {
//		return
//	}
//
//	var (
//		groupMask = uint(len(t.groups)) - 1
//		hashed    = t.hashFunc(key, t.seed)
//		taG       = newFullTag(hashed)
//		i         = uint(h1(hashed)&uint64(t.bucketMask)) >> 3
//	)
//
//	for {
//		gr := &t.groups[i]
//		ctrl := gr.control
//		iter := ctrl.MatchTag(taG).NewIter()
//
//		for j := iter.Next(); j != -1; j = iter.Next() {
//			if gr.buckets[j].key == key {
//				existence = true
//				value = &gr.buckets[j].value
//				return
//			}
//		}
//		if ctrl.MatchEmpty().AnyBitSet() {
//			return
//		}
//		i = (i + 1) & groupMask
//	}
//}

func (t *Table[K, V]) Delete(key K) {
	if t.items == 0 {
		return
	}

	var (
		hashed = t.h(key, t.seed)
		grMask = len(t.groups) - 1
		taG    = newFullTag(hashed)
		i      = int(h1(hashed)&uint64(t.bucketMask)) >> 3
	)
	for {
		gr := &t.groups[i]
		ctrl := gr.control
		iter := ctrl.MatchTag(taG).NewIter()
		for j := iter.Next(); j != -1; j = iter.Next() {
			if gr.buckets[j].key == key {
				t.erase(i, j)
				return
			}
		}

		// If we find out the empty slot, the key is not existed.
		if j := ctrl.MatchEmpty().LowestSetBit(); j != -1 {
			if ctrl.MatchEmpty().AnyBitSet() {
				return
			}
		}
		i = (i + 1) & grMask
	}
}

//func (t *Table[K, V]) DeleteReturn(key K) (value V, existence bool) {
//	if t.items == 0 {
//		return
//	}
//
//	var (
//		hashed = t.hashFunc(key, t.seed)
//		grMask = len(t.groups) - 1
//		taG    = newFullTag(hashed)
//		i      = int(h1(hashed)&uint64(t.bucketMask)) >> 3
//	)
//	for {
//		gr := &t.groups[i]
//		ctrl := gr.control
//		iter := ctrl.MatchTag(taG).NewIter()
//		for j := iter.Next(); j != -1; j = iter.Next() {
//			if gr.buckets[j].key == key {
//				return t.eraseReturn(i, j), true
//			}
//		}
//
//		// If we find out the empty slot, the key is not existed.
//		if j := ctrl.MatchEmpty().LowestSetBit(); j != -1 {
//			if ctrl.MatchEmpty().AnyBitSet() {
//				return
//			}
//		}
//		i = (i + 1) & grMask
//	}
//}

func (t *Table[K, V]) Reserve(additional uint) {
	if additional <= t.growthLeft {
		return
	}
	newItems, carry := bits.Add(t.items, additional, 0)
	if carry != 0 {
		panic("overflow capacity")
	}

	numActualUsedBuckets := getActualUsedBuckets(t.bucketMask)
	if newItems <= numActualUsedBuckets>>1 { // newBuckets <= numActualUsedBuckets / 2
		t.rehashInplace()
	} else {
		t.resize(int(max(newItems, numActualUsedBuckets+1)))
	}
}

func (t *Table[K, V]) Clear() {
	for i := range t.groups {
		gr := &t.groups[i]
		iter := gr.control.MatchFull().NewIter()
		for j := iter.Next(); j != -1; j = iter.Next() {
			gr.buckets[j] = bucket[K, V]{}
		}
		gr.control = repeatTag(tagEmpty)
	}
	t.items = 0
	t.growthLeft = getActualUsedBuckets(t.bucketMask)
}

func (t *Table[K, V]) Range(cb func(key K, val V) (goOn bool)) {
	idx := uint(hash.MakeSeed() & uint64(t.bucketMask))
	for k := uint(0); k <= t.bucketMask; k++ {
		i := idx >> 3 // idx / 8
		j := idx & 7  // idx % 8
		if t.groups[i].control.GetTag(j).IsFull() {
			if !cb(t.groups[i].buckets[j].key, t.groups[i].buckets[j].val) {
				return
			}
		}
		idx = (idx + 1) & t.bucketMask
	}
}

//func (t *Table[K, V]) RangeRef(cb func(key *K, value *V) (stop bool)) {
//	idx := uint(hash.MakeSeed() & uint64(t.bucketMask))
//	for k := uint(0); k <= t.bucketMask; k++ {
//		i := idx >> 3 // idx / 8
//		j := idx & 7  // idx % 8
//		if t.groups[i].control.GetTag(j).IsFull() {
//			if cb(&t.groups[i].buckets[j].key, &t.groups[i].buckets[j].value) {
//				return
//			}
//		}
//		idx = (idx + 1) & t.bucketMask
//	}
//}

func (t *Table[K, V]) Cap() uint {
	return getActualUsedBuckets(t.bucketMask)
}

func (t *Table[K, V]) Len() int {
	return int(t.items)
}

func (t *Table[K, V]) Seed() uint64 {
	return t.seed
}

func (t *Table[K, V]) prepareInsertIndex(hashed uint64) (i, j int) {
	i, j = t.findInsertIndex(hashed)
	t.groups[i].control = setTag(t.groups[i].control, uint(j), newFullTag(hashed))
	return
}

func (t *Table[K, V]) findInsertIndex(hashed uint64) (int, int) {
	// Always find the index because we have allocated more 12.5% empty bucket.
	var (
		grMask = uint(len(t.groups)) - 1
		i      = uint(h1(hashed)&uint64(t.bucketMask)) >> 3
	)
	for {
		gr := &t.groups[i]
		ctrl := gr.control
		j := ctrl.MatchEmptyOrDeleted().LowestSetBit()
		if j != -1 {
			return int(i), j
		}
		i = (i + 1) & grMask
	}
}

func (t *Table[K, V]) insertAt(key K, val V, taG tag, i, j int) {
	var (
		preTag tag
		gr     = &t.groups[i]
	)
	gr.control, preTag = replaceTag(gr.control, uint(j), taG)
	if preTag.SpecialIsEmpty() {
		t.growthLeft -= 1
	}
	gr.buckets[j] = bucket[K, V]{
		key: key,
		val: val,
	}
	t.items += 1
}

func (t *Table[K, V]) rehashInplace() {
	for _, g := range t.groups {
		g.control = g.control.ConvertSpecialToEmptyAndFullToDeleted()
	}

	numAllocBuckets := t.bucketMask + 1
	for idx := range numAllocBuckets {
		i := idx >> 3 // i / 8
		j := idx & 7  // i % 8

		g := &t.groups[i]
		if g.control.GetTag(j) != tagDeleted {
			continue
		}

		for {
			hashed := t.h(g.buckets[j].key, t.seed)
			newI, newJ := t.findInsertIndex(hashed)
			// In the same group.
			if uint(newI) == i {
				g.control = setTag(g.control, j, newFullTag(hashed))
				continue
			}

			var preTag tag
			newG := &t.groups[newI]
			newG.control, preTag = replaceTag(newG.control, uint(newJ), newFullTag(hashed))
			if preTag == tagEmpty {
				g.control = setTag(g.control, j, tagEmpty)
				g.buckets[j], newG.buckets[newJ] = bucket[K, V]{}, g.buckets[j]
				break
			}
			if preTag != tagDeleted {
				panic("expected DELETED tag during swap")
			}
			g.buckets[j], newG.buckets[newJ] = newG.buckets[newJ], g.buckets[j]
		}
	}
	t.growthLeft = getActualUsedBuckets(t.bucketMask) - t.items
}

func (t *Table[K, V]) resize(cap int) {
	newTable := createTable[K, V](cap, t.seed, t.h)

	for _, g := range t.groups {
		iter := g.control.MatchFull().NewIter()
		for j := iter.Next(); j != -1; j = iter.Next() {
			hashed := t.h(g.buckets[j].key, t.seed)
			newI, newJ := newTable.prepareInsertIndex(hashed)
			g.buckets[j], newTable.groups[newI].buckets[newJ] = bucket[K, V]{}, g.buckets[j]
		}
	}

	t.bucketMask = newTable.bucketMask
	t.growthLeft = newTable.growthLeft - t.items
	t.groups = newTable.groups
}

//func (t *Table[K, V]) eraseReturn(i, j int) (value V) {
//	preI := i - 1
//	if preI < 0 {
//		preI = int((t.bucketMask+1)>>3) - 1
//	}
//
//	gr := &t.groups[i]
//	preBitmaskEmpty := t.groups[preI].control.MatchEmpty()
//	bitmaskEmpty := gr.control.MatchEmpty()
//
//	taG := tagDeleted
//	if preBitmaskEmpty.LeadingZeros()+bitmaskEmpty.TrailingZeros() < 8 {
//		taG = tagEmpty
//		t.growthLeft += 1
//	}
//	value = gr.buckets[j].value
//	gr.control = setTag(gr.control, uint(j), taG)
//	gr.buckets[j].key = pkg.ZeroValue[K]()
//	gr.buckets[j].value = pkg.ZeroValue[V]()
//	t.items -= 1
//	return
//}

func (t *Table[K, V]) erase(i, j int) {
	preI := i - 1
	if preI < 0 {
		preI = int((t.bucketMask+1)>>3) - 1
	}

	gr := &t.groups[i]
	preBitmaskEmpty := t.groups[preI].control.MatchEmpty()
	bitmaskEmpty := gr.control.MatchEmpty()

	taG := tagDeleted
	if preBitmaskEmpty.LeadingZeros()+bitmaskEmpty.TrailingZeros() < 8 {
		taG = tagEmpty
		t.growthLeft += 1
	}
	gr.control = setTag(gr.control, uint(j), taG)
	gr.buckets[j].key = pkg.ZeroValue[K]()
	gr.buckets[j].val = pkg.ZeroValue[V]()
	t.items -= 1
}

func createTable[K comparable, V any](cap int, seed uint64, hashFunc HashFunc[K]) *Table[K, V] {
	numAllocBuckets := getAllocBuckets(cap)
	groups := make([]group[K, V], numAllocBuckets/8) // divided by 8 (a group has 8 buckets).
	for i := range groups {
		groups[i] = newGroup[K, V]()
	}

	return &Table[K, V]{
		seed:       seed,
		bucketMask: numAllocBuckets - 1,
		growthLeft: getActualUsedBuckets(numAllocBuckets - 1),
		items:      0,
		h:          hashFunc,
		groups:     groups,
	}
}

func getAllocBuckets(cap int) uint {
	if cap < 15 {
		if cap < 8 {
			return 8
		}
		return 16
	}
	// Load factor: 87.5% (7/8)
	adjustCapacity := (cap * 8) / 7
	return pkg.NextPowerOfTwo(adjustCapacity)
}

func getActualUsedBuckets(bucketMask uint) uint {
	if bucketMask < 8 {
		return bucketMask
	}
	return ((bucketMask + 1) * 7) / 8
}

func h1(hashed uint64) uint64 {
	return hashed
}
