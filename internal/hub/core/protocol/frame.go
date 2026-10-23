/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package protocol

import (
	"encoding/binary"

	slicepool "xxx/pkg/pool/slice"
)

// EncodeFrame returns an owned pooled buffer containing the LE16 body length
// followed by a copy of body. The caller must clear it and return it to slicepool.
func EncodeFrame(body []byte, maxFrame int) ([]byte, error) {
	if len(body) == 0 ||
		len(body) > MaxFrameSize ||
		len(body) > maxFrame {
		return nil, ErrPkgTooLarge
	}

	out := slicepool.Get(FramePrefixSize + len(body))
	out = out[:FramePrefixSize]
	binary.LittleEndian.PutUint16(out, uint16(len(body)))
	return append(out, body...), nil
}

// FrameBody validates the length prefix and body limit, then returns a view into
// frame that remains valid until frame is modified or released.
func FrameBody(frame []byte, maxFrame int) ([]byte, error) {
	if len(frame) < FramePrefixSize+1 ||
		len(frame)-FramePrefixSize > maxFrame ||
		int(binary.LittleEndian.Uint16(frame)) != len(frame)-FramePrefixSize {
		return nil, ErrPkgSizeInvalid
	}
	return frame[FramePrefixSize:], nil
}
