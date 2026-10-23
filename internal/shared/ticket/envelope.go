/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package ticket

import slicepool "xxx/pkg/pool/slice"

const (
	SignedVersion    = byte(2)
	SignedHeaderSize = 4
	MinSignedSize    = SignedHeaderSize + MinPayloadSize + 64
	MaxSignedSize    = SignedHeaderSize + MaxPayloadSize + 64
	SigningDomain    = "xxx/iam/ticket-signature/v2\x00"
)

// SigningMessage prefixes the unsigned envelope with the ticket-signature domain
// and returns an owned pooled buffer that does not alias unsigned. The caller
// must clear the buffer and return it to slicepool after use.
func SigningMessage(unsigned []byte) []byte {
	domainLen := len(SigningDomain)
	out := slicepool.Get(domainLen + len(unsigned))
	copy(out, SigningDomain)
	copy(out[domainLen:], unsigned)
	return out
}
