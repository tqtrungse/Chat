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
	headerSize = 6
	seqSize    = 8

	securePacketAADDomain = "xxx/hub/packet/gcm/v1\x00"
)

// Replay protection
//
// Every frame sent after activation carries an IMPLICIT per-direction
// sequence number: it is never put on the wire, only mixed into the
// authenticated bytes (AAD for AEAD frames, MAC input for HMAC frames).
//
//   - Both directions start at 0 on the activated connection; handshake
//     frames (EncodeHandshake) are not counted.
//   - One counter per direction, shared by Encode/EncodeS (the frames live in
//     the same TCP stream, so ordering must hold across frame types).
//   - The receiver passes the seq it EXPECTS. Any mismatch (replay, drop,
//     reorder) surfaces as an authentication failure; the caller must close
//     the connection and must NOT advance its counter.
//   - Directions are already separated by distinct keys (c2s/* vs s2c/*).

// seqBytes encodes seq as little-endian, matching the rest of the protocol.
func seqBytes(seq uint64) [seqSize]byte {
	var b [seqSize]byte
	binary.LittleEndian.PutUint64(b[:], seq)
	return b
}

// securePacketAAD authenticates the encrypted packet's framing metadata, the
// implicit sequence number and the cleartext expansion bytes. Keep the field
// order and endianness identical in the encoder and decoder.
//
// Layout: domain | frameSize(2) | pkgType(2) | cipherSize(2) | seq(8) | expand
func securePacketAAD(frameSize, pkgType, cipherSize uint16, seq uint64, expand []byte) []byte {
	const domainLen = len(securePacketAADDomain)

	aad := slicepool.Get(domainLen + headerSize + seqSize + len(expand))
	copy(aad, securePacketAADDomain)

	header := aad[domainLen : domainLen+headerSize]
	binary.LittleEndian.PutUint16(header[:2], frameSize)
	binary.LittleEndian.PutUint16(header[2:4], pkgType)
	binary.LittleEndian.PutUint16(header[4:6], cipherSize)

	binary.LittleEndian.PutUint64(aad[domainLen+headerSize:domainLen+headerSize+seqSize], seq)
	copy(aad[domainLen+headerSize+seqSize:], expand)
	return aad
}
