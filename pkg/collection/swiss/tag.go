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

const (
	// tagEmpty (0xFF) Control tag value for an empty bucket.
	tagEmpty tag = 0b1111_1111

	// tagDeleted (0x80) Control tag value for a deleted bucket.
	tagDeleted tag = 0b1000_0000
)

type tag uint8

// IsFull
//
// Checks whether a control tag represents a full bucket (top bit is clear).
func (t tag) IsFull() bool {
	return t&0x80 == 0
}

// IsSpecial
//
// Checks whether a control tag represents a special value (top bit is set).
func (t tag) IsSpecial() bool {
	return t&0x80 != 0
}

// SpecialIsEmpty
//
// Checks whether a special control value is EMPTY (just check 1 bit).
func (t tag) SpecialIsEmpty() bool {
	return t&0x01 != 0
}

// newFullTag
//
// Generate a full tag from the 7 highest bits of the hash.
func newFullTag(hash uint64) tag {
	top7 := hash >> 57
	return tag(top7 & 0x7f)
}
