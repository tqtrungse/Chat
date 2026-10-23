/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package protocol

import "math"

const (
	msgKindBytes       = 2
	pbSizeBytes        = 2
	payloadHeaderSize  = msgKindBytes + pbSizeBytes
	noiseTagSize       = 16
	maxPlainSize       = math.MaxUint16 - noiseTagSize
	maxPayloadBodySize = maxPlainSize - payloadHeaderSize

	// FramePrefixSize is the LE16 frame-body length prefix in bytes.
	FramePrefixSize = 2
	MaxFrameSize    = math.MaxUint16
	// ClientHelloHeaderSize is the wire version byte plus the LE32 static key ID.
	ClientHelloHeaderSize = 1 + 4
)
