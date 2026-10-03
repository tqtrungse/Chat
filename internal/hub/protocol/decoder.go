/*
 * Copyright 2026 tqtrungse@gmail.com. All rights reserved.
 * Use of this pub code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package protocol

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"

	pbpub "xxx/api/hub/v1/proto/gen/pub"

	"google.golang.org/protobuf/proto"
)

type Decoder struct {
	secureChannel SecureChannel
	pbUnmarshal   proto.UnmarshalOptions
}

func NewDecoder(secureChannel SecureChannel) *Decoder {
	return &Decoder{
		secureChannel: secureChannel,
		pbUnmarshal:   proto.UnmarshalOptions{DiscardUnknown: true},
	}
}

func (d *Decoder) DecodeActivePack(
	plain []byte,
	outReq *pbpub.ActiveConnReq,
) error {
	return d.pbUnmarshal.Unmarshal(plain, outReq)
}

func (d *Decoder) Decode(
	hmacKey *[32]byte,
	packet []byte,
) (proto.Message, []byte, pbpub.Code, error) {
	// -------------------------------------------------------
	// |  PkgType |   Pb Size  |  Pb  |  Expand  |    HMAC   |
	// -------------------------------------------------------
	// |  2 bytes |   2 bytes  | xxx  |    xxx   |  32 bytes |
	// -------------------------------------------------------
	if len(packet) <= 36 {
		return nil, nil, pbpub.Code_ERR_INVALID_PACK_SIZE, ErrInvalidPkgSize
	}

	size := len(packet) - 32
	mac := hmac.New(sha256.New, hmacKey[:])
	mac.Write(packet[:size])

	if ok := hmac.Equal(mac.Sum(nil), packet[size:]); !ok {
		return nil, nil, pbpub.Code_ERR_MODIFIED_PACK, ErrModifiedPkg
	}

	var (
		packType = binary.LittleEndian.Uint16(packet[:2])
		pbSize   = binary.LittleEndian.Uint16(packet[2:4])
		msg      proto.Message
	)

	if int(pbSize) > len(packet)-36 {
		return nil, nil, pbpub.Code_ERR_INVALID_PACK_SIZE, ErrInvalidPkgSize
	}

	switch pbpub.PacketType(packType) {
	case pbpub.PacketType_REQ_ACTIVE_CONN:
		msg = new(pbpub.ActiveConnReq)

	case pbpub.PacketType_REQ_DIRECT_FORWARD_MSG:
		msg = new(pbpub.DirectForwardMsgReq)

	default:
		return nil, nil, pbpub.Code_ERR_INVALID_PACK_TYPE, ErrInvalidPkgType
	}

	if err := d.pbUnmarshal.Unmarshal(packet[4:pbSize+4], msg); err != nil {
		return nil, nil, pbpub.Code_ERR_DESERIALIZE, err
	}
	return msg, packet[pbSize+4 : size], pbpub.Code_SUCCESS, nil
}

func (d *Decoder) DecodeS(
	secretKey *[32]byte,
	packet []byte,
) (proto.Message, []byte, pbpub.Code, error) {
	// ------------------------------------------------------
	// | PkgType |   Nonce   | CipherSize | Cipher | Expand |
	// ------------------------------------------------------
	// | 2 bytes |  12 bytes |   2 bytes  |   Pb   |   xxx  |
	// ------------------------------------------------------
	if len(packet) <= 16 {
		return nil, nil, pbpub.Code_ERR_INVALID_PACK_SIZE, ErrInvalidPkgSize
	}

	var (
		msgType = binary.LittleEndian.Uint16(packet[:2])
		msg     proto.Message
	)

	switch pbpub.PacketType(msgType) {
	case pbpub.PacketType_REQ_SEND_MSG:
		msg = new(pbpub.SendMsgReq)

	default:
		return nil, nil, pbpub.Code_ERR_INVALID_PACK_TYPE, ErrInvalidPkgType
	}

	cipherSize := binary.LittleEndian.Uint16(packet[14:16])
	if !d.secureChannel.IsValid(int(cipherSize)) ||
		int(cipherSize) > len(packet)-16 {
		return nil, nil, pbpub.Code_ERR_INVALID_PACK_CIPHER, ErrInvalidPkgCipher
	}

	plain, err := d.secureChannel.Open(
		secretKey,
		(*[12]byte)(packet[2:14]),
		packet[16:cipherSize+16],
		packet[cipherSize+16:],
	)
	if err != nil {
		return nil, nil, pbpub.Code_ERR_DECRYPT, err
	}

	if err = d.pbUnmarshal.Unmarshal(plain, msg); err != nil {
		return nil, nil, pbpub.Code_ERR_DESERIALIZE, err
	}
	return msg, packet[cipherSize+16:], pbpub.Code_SUCCESS, nil
}
