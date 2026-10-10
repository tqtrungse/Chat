/*
 * Copyright 2026 tqtrungse@gmail.com. All rights reserved.
 * Use of this pub code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package protocol

import (
	"encoding/binary"

	slicepool "xxx/pkg/pool/slice"
)

const (
	headerSize            = 6
	securePacketAADDomain = "xxx/hub/packet/gcm/v1\x00"
)

// securePacketAAD authenticates the encrypted packet's framing metadata as
// well as its cleartext expansion bytes. Keep the field order and endianness
// identical in the encoder and decoder.
func securePacketAAD(frameSize, pkgType, cipherSize uint16, expand []byte) []byte {
	aad := slicepool.Get(len(securePacketAADDomain)+headerSize+len(expand))
	copy(aad, securePacketAADDomain)

	header := aad[len(securePacketAADDomain) : len(securePacketAADDomain)+headerSize]
	binary.LittleEndian.PutUint16(header[:2], frameSize)
	binary.LittleEndian.PutUint16(header[2:4], pkgType)
	binary.LittleEndian.PutUint16(header[4:6], cipherSize)
	copy(aad[len(securePacketAADDomain)+headerSize:], expand)
	return aad
}
