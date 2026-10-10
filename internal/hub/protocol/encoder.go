/*
 * Copyright 2026 tqtrungse@gmail.com. All rights reserved.
 * Use of this pub code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package protocol

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"

	pbpub "xxx/api/hub/v1/proto/gen/pub"
	slicepool "xxx/pkg/pool/slice"

	"google.golang.org/protobuf/proto"
)

const maxFrameSize = 1<<16 - 1

type Encoder struct {
	secureChannel SecureChannel
	pbMarshal     proto.MarshalOptions
}

func NewEncoder(secureChannel SecureChannel) *Encoder {
	return &Encoder{
		secureChannel: secureChannel,
		pbMarshal:     proto.MarshalOptions{UseCachedSize: true},
	}
}

// EncodeHandshake encodes a pre-session packet as frame length | packet type
// | protobuf size | protobuf. It deliberately has no session HMAC because the session keys
// have not been activated yet. Only public challenge data may use this path.
func (e *Encoder) EncodeHandshake(pkgType pbpub.PacketType, msg proto.Message) ([]byte, error) {
	pbSize := e.pbMarshal.Size(msg)
	frameSize := 4 + pbSize // packet type + protobuf length + protobuf

	if pbSize > maxFrameSize || frameSize > maxFrameSize {
		return nil, ErrHandshakePacketTooLarge
	}

	needed := 2 + frameSize
	buf := slicepool.Get(needed)
	binary.LittleEndian.PutUint16(buf[:2], uint16(needed-2))
	binary.LittleEndian.PutUint16(buf[2:4], uint16(pkgType))
	binary.LittleEndian.PutUint16(buf[4:6], uint16(pbSize))

	buf, err := e.pbMarshal.MarshalAppend(buf[:6], msg)
	if err == nil {
		if len(buf)-6 == pbSize {
			return buf, nil
		}
		clear(buf)
		slicepool.Put(buf)
		return nil, ErrPkgSizeInvalid
	}
	clear(buf)
	slicepool.Put(buf)
	return nil, err
}

func (e *Encoder) Encode(
	macKey *[32]byte,
	pkgType pbpub.PacketType,
	msg proto.Message,
	expand []byte,
) ([]byte, error) {
	// -------------------------------------------------------------------
	// | Pkg Size |  Pkg Type |   Pb Size  |  Pb  |  Expand  |    HMAC   |
	// -------------------------------------------------------------------
	// |  2 bytes |  2 bytes  |   2 bytes  | xxx  |    xxx   |  32 bytes |
	// -------------------------------------------------------------------
	var (
		pbSize     = e.pbMarshal.Size(msg)
		expandSize = len(expand)
		frameSize  = 36 + pbSize + expandSize // header + protobuf + expand + HMAC
	)

	if pbSize > maxFrameSize || frameSize > maxFrameSize {
		return nil, ErrPkgTooLarge
	}
	needed := 2 + frameSize
	buf := slicepool.Get(needed)

	binary.LittleEndian.PutUint16(buf[:2], uint16(needed-2))
	binary.LittleEndian.PutUint16(buf[2:4], uint16(pkgType))
	binary.LittleEndian.PutUint16(buf[4:6], uint16(pbSize))

	buf, err := e.pbMarshal.MarshalAppend(buf[:6], msg)
	if err != nil {
		clear(buf)
		slicepool.Put(buf)
		return nil, err
	}
	if len(buf)-6 != pbSize {
		clear(buf)
		slicepool.Put(buf)
		return nil, ErrPkgSizeInvalid
	}

	if expandSize != 0 {
		buf = append(buf, expand...)
	}

	mac := hmac.New(sha256.New, macKey[:])
	mac.Write(buf)
	buf = mac.Sum(buf)
	return buf, nil
}

func (e *Encoder) EncodeS(
	secretKey *[32]byte,
	pkgType pbpub.PacketType,
	msg proto.Message,
	expand []byte,
) ([]byte, error) {
	// -------------------------------------------------------------------
	// | Pkg Size | Pkg Type |   Nonce   | Cipher Size | Cipher | Expand |
	// -------------------------------------------------------------------
	// | 2 bytes  | 2 bytes  |  12 bytes |   2 bytes   |   Pb   |   xxx  |
	// -------------------------------------------------------------------

	var (
		cipherSize = e.secureChannel.Size(e.pbMarshal.Size(msg))
		expandSize = len(expand)
		frameSize  = 16 + cipherSize + expandSize // type + nonce + size + ciphertext + expand
	)

	if cipherSize < 0 || cipherSize > maxFrameSize || frameSize > maxFrameSize {
		return nil, ErrPkgTooLarge
	}
	needed := 2 + frameSize
	buf := slicepool.Get(needed)

	binary.LittleEndian.PutUint16(buf[:2], uint16(needed-2))
	binary.LittleEndian.PutUint16(buf[2:4], uint16(pkgType))
	if _, err := rand.Read(buf[4:16]); err != nil {
		clear(buf)
		slicepool.Put(buf)
		return nil, err
	}
	binary.LittleEndian.PutUint16(buf[16:18], uint16(cipherSize))

	buf, err := e.pbMarshal.MarshalAppend(buf[:18], msg)
	if err != nil {
		clear(buf)
		slicepool.Put(buf)
		return nil, err
	}
	if len(buf)-18 != e.pbMarshal.Size(msg) {
		clear(buf)
		slicepool.Put(buf)
		return nil, ErrPkgSizeInvalid
	}

	aad := securePacketAAD(
		uint16(frameSize),
		uint16(pkgType),
		uint16(cipherSize),
		expand,
	)
	sealed, err := e.secureChannel.Seal(
		secretKey,
		(*[12]byte)(buf[4:16]),
		buf[18:],
		aad,
	)
	clear(aad)
	slicepool.Put(aad)

	if err != nil {
		clear(buf)
		slicepool.Put(buf)
		return nil, err
	}
	if len(sealed) != cipherSize {
		clear(buf)
		slicepool.Put(buf)
		return nil, ErrPkgCipherInvalid
	}

	buf = buf[:18+len(sealed)]
	if expandSize != 0 {
		buf = append(buf, expand...)
	}
	return buf, nil
}
