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

func (e *Encoder) Encode(
	hmacKey *[32]byte,
	packType pbpub.PacketType,
	msg proto.Message,
	expand []byte,
) ([]byte, error) {
	// -------------------------------------------------------
	// |  PkgType |   Pb Size  |  Pb  |  Expand  |    HMAC   |
	// -------------------------------------------------------
	// |  2 bytes |   2 bytes  | xxx  |    xxx   |  32 bytes |
	// -------------------------------------------------------
	var (
		pbSize     = e.pbMarshal.Size(msg)
		expandSize = len(expand)
		needed     = 36 + pbSize + len(expand)
		buf        = slicepool.Get(needed)
	)

	binary.LittleEndian.PutUint16(buf[:2], uint16(packType))
	binary.LittleEndian.PutUint16(buf[2:4], uint16(pbSize))

	buf, err := e.pbMarshal.MarshalAppend(buf[:4], msg)
	if err != nil {
		slicepool.Put(buf)
		return nil, err
	}

	if expandSize != 0 {
		buf = append(buf, expand...)
	}

	mac := hmac.New(sha256.New, hmacKey[:])
	mac.Write(buf)
	buf = mac.Sum(buf)
	return buf, nil
}

func (e *Encoder) EncodeS(
	secretKey *[32]byte,
	packType pbpub.PacketType,
	msg proto.Message,
	expand []byte,
) ([]byte, error) {
	// ------------------------------------------------------
	// | MsgType |   Nonce   | CipherSize | Cipher | Expand |
	// ------------------------------------------------------
	// | 2 bytes |  12 bytes |   2 bytes  |   Pb   |   xxx  |
	// ------------------------------------------------------

	var (
		cipherSize = e.secureChannel.Size(e.pbMarshal.Size(msg))
		expandSize = len(expand)
		needed     = 16 + cipherSize + expandSize
		buf        = slicepool.Get(needed)
	)

	binary.LittleEndian.PutUint16(buf[:2], uint16(packType))
	// Implement of rand.Read will panic if there is any error.
	// So we don't need check error.
	_, _ = rand.Read(buf[2:14])
	binary.LittleEndian.PutUint16(buf[14:16], uint16(cipherSize))

	buf, err := e.pbMarshal.MarshalAppend(buf[:16], msg)
	if err != nil {
		slicepool.Put(buf)
		return nil, err
	}

	sealed, err := e.secureChannel.Seal(
		secretKey,
		(*[12]byte)(buf[2:14]),
		buf[16:],
		expand,
	)
	if err != nil {
		slicepool.Put(buf)
		return nil, err
	}

	buf = buf[:16+len(sealed)]
	if expandSize != 0 {
		buf = append(buf, expand...)
	}
	return buf, nil
}
