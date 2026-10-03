/*
 * Copyright 2026 tqtrungse@gmail.com. All rights reserved.
 * Use of this pub code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package protocol

type SecureChannel interface {
	// Seal reuse plain's storage for the encrypted output.
	// Uses Size to allocate buffer for plain.
	Seal(key *[32]byte, nonce *[12]byte, plain []byte, aad []byte) ([]byte, error)

	// Open reuse cipher's storage for the decrypted output.
	Open(key *[32]byte, nonce *[12]byte, cipher []byte, aad []byte) ([]byte, error)

	// Size returns length of cipher size.
	Size(plainSize int) int

	// IsValid checks cipher Size valid.
	IsValid(cipherSize int) bool
}
