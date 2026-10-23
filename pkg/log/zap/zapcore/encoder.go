/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package zapcore

import (
	"xxx/pkg/log/zap/buffer"
)

// Encoder is the Encoder of zapcore with one difference. EncodeEntry returns
// the Buffer type of this fork, and not the Buffer type of upstream.
//
// This difference is the reason for the fork. The Buffer of this fork takes
// its bytes from xxx/pkg/pool/slice.
//
// EncodeEntry gives the Buffer to the caller. The caller is the owner of the
// Buffer and must call Free one time. The encoder keeps no reference.
type Encoder interface {
	ObjectEncoder

	// Clone copies the encoder, ensuring that adding fields to the copy doesn't
	// affect the original.
	Clone() Encoder

	// EncodeEntry encodes an entry and fields, along with any accumulated
	// context, into a byte buffer and returns it. Any fields that are empty,
	// including fields on the `Entry` type, should be omitted.
	EncodeEntry(Entry, []Field) (*buffer.Buffer, error)
}
