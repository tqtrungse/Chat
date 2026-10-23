/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package ticketlimit

import (
	"math"
	"time"
)

type ticketRateKey struct {
	subject string
	device  uint64 // Zero identifies an aggregate subject bucket.
}

// ticketRateBucket is the GCRA state of one key: a single 48-byte allocation.
// tat is the "theoretical arrival time": the instant at which the bucket would
// be full again, in nanoseconds on the limiter's timeline. tat <= now is
// indistinguishable from a brand-new bucket.
type ticketRateBucket struct {
	key   ticketRateKey
	tat   int64
	index int // position in ticketRateTable.heap
}

// maxGCRAWindow keeps TAT+interval far from int64 overflow for any config.
const maxGCRAWindow = math.MaxInt64 / 4

// ticketRateTable holds one family of buckets (all share interval and burst).
// heap is a binary min-heap ordered by tat, so the root is always the bucket
// that becomes full again first. Expiry is therefore exact: after prune, every
// live bucket has tat > now. Admission only ever increases tat, so an update
// is a single sift-down.
// Callers synchronize map and heap access through the owning limiter's mutex.
type ticketRateTable struct {
	entries    map[ticketRateKey]*ticketRateBucket
	heap       []*ticketRateBucket
	interval   int64 // ns per token
	window     int64 // interval * burst, ns
	maxEntries int
}

// newTicketRateTable creates an empty table with a bounded refill interval and
// burst window. maxEntries limits how many distinct keys the limiter may retain.
func newTicketRateTable(
	interval time.Duration,
	burst int,
	maxEntries int,
) *ticketRateTable {
	iv := min(max(int64(interval), 1), maxGCRAWindow)
	b := max(int64(burst), 1)
	window := int64(maxGCRAWindow)
	if b <= window/iv {
		window = iv * b
	}

	return &ticketRateTable{
		entries:    make(map[ticketRateKey]*ticketRateBucket),
		interval:   iv,
		window:     window,
		maxEntries: maxEntries,
	}
}

// swap exchanges two heap positions and updates the buckets' stored indices.
func (t *ticketRateTable) swap(i, j int) {
	h := t.heap
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}

// up restores min-heap order after insertion by moving the bucket toward the
// root while its tat is smaller than its parent's.
func (t *ticketRateTable) up(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if t.heap[parent].tat <= t.heap[i].tat {
			return
		}
		t.swap(i, parent)
		i = parent
	}
}

// down restores min-heap order after increasing a bucket's tat or replacing the
// root, moving the bucket toward the leaves until neither child has a smaller tat.
func (t *ticketRateTable) down(i int) {
	n := len(t.heap)
	for {
		child := 2*i + 1
		if child >= n {
			return
		}

		right := child + 1
		if right < n && t.heap[right].tat < t.heap[child].tat {
			child = right
		}
		if t.heap[i].tat <= t.heap[child].tat {
			return
		}

		t.swap(i, child)
		i = child
	}
}

// push inserts a bucket into the heap and records its position. The caller is
// responsible for adding the bucket to entries and updating device counters.
func (t *ticketRateTable) push(b *ticketRateBucket) {
	b.index = len(t.heap)
	t.heap = append(t.heap, b)
	t.up(b.index)
}

// popMin removes and returns the bucket with the smallest tat, restoring heap
// order and clearing the unused tail pointer. The heap must be nonempty; the
// caller removes the returned bucket from entries and updates device counters.
func (t *ticketRateTable) popMin() *ticketRateBucket {
	root := t.heap[0]
	last := len(t.heap) - 1
	t.swap(0, last)
	t.heap[last] = nil
	t.heap = t.heap[:last]
	if last > 0 {
		t.down(0)
	}
	return root
}

// take computes a GCRA decision without mutating the table. On admission it
// returns the next tat and zero wait; the caller commits that tat to the bucket.
// On rejection, it returns the original tat and the wait in nanoseconds, so
// rejected requests never consume future capacity or extend the penalty.
func (t *ticketRateTable) take(tat, now int64) (newTAT, wait int64) {
	next := max(tat, now) + t.interval
	over := next - now - t.window
	if over > 0 {
		return tat, over
	}
	return next, 0
}
