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

import "math/bits"

type bitMask uint64

const (
	bitMaskStride   = 8
	bitMaskIterMask = 0xFFFFFFFFFFFFFFFF
)

// RemoveLowestBit
//
// Returns a new `BitMask` with the lowest (the rightest) bit removed.
func (b bitMask) RemoveLowestBit() bitMask {
	return b & (b - 1)
}

// AnyBitSet
//
// Returns whether the `BitMask` has at least one set bit.
func (b bitMask) AnyBitSet() bool {
	return b != 0
}

// LowestSetBit
//
// Returns the first set a bit in the `BitMask`, if there is one.
func (b bitMask) LowestSetBit() int {
	if b == 0 {
		return -1
	}
	return b.TrailingZeros()
}

// TrailingZeros
//
// Returns the number of trailing zeroes in the `BitMask`.
func (b bitMask) TrailingZeros() int {
	return bits.TrailingZeros64(uint64(b)) / bitMaskStride
}

// LeadingZeros
//
// Returns the number of leading zeroes in the `BitMask`.
func (b bitMask) LeadingZeros() int {
	return bits.LeadingZeros64(uint64(b)) / bitMaskStride
}

// bitMaskIter
//
// Iterator over the contents of a `BitMask`, returning the indices of set bits.
type bitMaskIter struct {
	mask bitMask
}

func (b bitMask) NewIter() bitMaskIter {
	return bitMaskIter{
		mask: b & bitMaskIterMask,
	}
}

// Next
//
// Returns the Next index, -1 to indicate when the end has been reached.
func (it *bitMaskIter) Next() int {
	bit := it.mask.LowestSetBit()
	if bit == -1 {
		return -1
	}
	it.mask = it.mask.RemoveLowestBit()
	return bit
}
