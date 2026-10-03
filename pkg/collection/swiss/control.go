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

type control uint64

const bitMaskMask = 0x8080808080808080

// repeatTag
//
// Replicates a tag into 8 bytes of a uint64
func repeatTag(tag tag) control {
	return control(uint64(tag) * 0x0101010101010101)
}

func replaceTag(g control, idx uint, tag tag) (control, tag) {
	preTag := g.GetTag(idx)
	g = setTag(g, idx, tag)
	return g, preTag
}

func setTag(g control, idx uint, tag tag) control {
	shift := idx << 3
	g &^= control(0xFF) << shift
	g |= control(tag) << shift
	return g
}

func (c control) GetTag(idx uint) tag {
	return tag((c >> (idx << 3)) & 0xFF) // SỬA Ở ĐÂY
}

// MatchTag
//
// Returns a `BitMask` indicating all tags in the group which *may*
// have the given value.
//
// This function may return a false positive in certain cases where
// the tag in the group differs from the searched value only in its
// lowest bit. This is fine because:
// - This never happens for `EMPTY` and `DELETED`, only full entries.
// - The check for key equality will catch these.
// - This only happens if there is at least 1 true match.
// - The chance of this happening is very low (< 1% chance per tag).
func (c control) MatchTag(tag tag) bitMask {
	// Algorithm: https://graphics.stanford.edu/~seander/bithacks.html#ValueInWord
	cmp := c ^ repeatTag(tag)
	res := (cmp - repeatTag(0x01)) & ^cmp & bitMaskMask
	return bitMask(res)
}

// MatchEmpty
//
// Returns a `bitMask` indicating all tags in the group which are `EMPTY`.
func (c control) MatchEmpty() bitMask {
	return bitMask(c & (c << 1) & bitMaskMask)
}

// MatchEmptyOrDeleted
//
// Returns a `bitMask` indicating all tags in the group which are `EMPTY` or `DELETED`.
func (c control) MatchEmptyOrDeleted() bitMask {
	return bitMask(c & bitMaskMask)
}

// MatchFull
//
// Returns a `bitMask` indicating all tags in the group which are full.
func (c control) MatchFull() bitMask {
	return bitMask((c ^ bitMaskMask) & bitMaskMask)
}

// ConvertSpecialToEmptyAndFullToDeleted
//
// Performs the following transformation on all tags in the group:
// - `EMPTY => EMPTY`
// - `DELETED => EMPTY`
// - `FULL => DELETED`
func (c control) ConvertSpecialToEmptyAndFullToDeleted() control {
	// Map high_bit = 1 (EMPTY or DELETED) to 1111_1111
	// and high_bit = 0 (FULL) to 1000_0000
	//
	// Here's this logic expanded to concrete values:
	//   let full = 1000_0000 (true) or 0000_0000 (false)
	//   !1000_0000 + 1 = 0111_1111 + 1 = 1000_0000 (no carry)
	//   !0000_0000 + 0 = 1111_1111 + 0 = 1111_1111 (no carry)
	full := (^c) & bitMaskMask
	return ^full + (full >> 7)
}
