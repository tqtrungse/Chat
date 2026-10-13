/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ticket

import (
	"crypto/ed25519"

	slicepool "xxx/pkg/pool/slice"
)

const (
	SignedVersion      byte = 2
	SignedHeaderSize        = 2
	SignedUnsignedSize      = SignedHeaderSize + size
	SignedSize              = SignedUnsignedSize + ed25519.SignatureSize
	SigningDomain           = "xxx/iam/ticket-signature/v1\x00"
)

func SigningMessage(unsigned []byte) []byte {
	n := len(SigningDomain)
	message := slicepool.Get(n + len(unsigned))
	copy(message[:n], SigningDomain)
	copy(message[n:], unsigned)
	return message
}
