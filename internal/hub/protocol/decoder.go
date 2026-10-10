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
	slicepool "xxx/pkg/pool/slice"

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

func (d *Decoder) DecodeActiveProofPack(
	plain []byte,
	outReq *pbpub.ActiveConnProofReq,
) error {
	return d.pbUnmarshal.Unmarshal(plain, outReq)
}

func (d *Decoder) Decode(
	hmacKey *[32]byte,
	packet []byte,
) (proto.Message, []byte, pbpub.Code, error) {
	// -------------------------------------------------------------------
	// | Pkg Size |  Pkg Type |   Pb Size  |  Pb  |  Expand  |    HMAC   |
	// -------------------------------------------------------------------
	// |  2 bytes |  2 bytes  |   2 bytes  | xxx  |    xxx   |  32 bytes |
	// -------------------------------------------------------------------
	if !hasValidFrame(packet, 36) {
		return nil, nil, pbpub.Code_ERR_INVALID_PACK_SIZE, ErrPkgSizeInvalid
	}

	size := len(packet) - 32
	mac := hmac.New(sha256.New, hmacKey[:])
	mac.Write(packet[:size])

	if ok := hmac.Equal(mac.Sum(nil), packet[size:]); !ok {
		return nil, nil, pbpub.Code_ERR_MODIFIED_PACK, ErrPkgModified
	}

	var (
		pkgType = binary.LittleEndian.Uint16(packet[2:4])
		pbSize  = binary.LittleEndian.Uint16(packet[4:6])
		msg     proto.Message
	)

	if int(pbSize) > size-6 {
		return nil, nil, pbpub.Code_ERR_INVALID_PACK_SIZE, ErrPkgSizeInvalid
	}

	switch pbpub.PacketType(pkgType) {
	case pbpub.PacketType_REQ_ACTIVE_CONN:
		msg = new(pbpub.ActiveConnReq)

	case pbpub.PacketType_REQ_DIRECT_FORWARD_MSG:
		msg = new(pbpub.DirectForwardMsgReq)

	case pbpub.PacketType_REQ_BROKER_FORWARD_MSG:
		msg = new(pbpub.BrokerForwardMsgReq)

	default:
		return nil, nil, pbpub.Code_ERR_INVALID_PACK_TYPE, ErrPkgTypeInvalid
	}

	pbEnd := 6 + int(pbSize)
	if err := d.pbUnmarshal.Unmarshal(packet[6:pbEnd], msg); err != nil {
		return nil, nil, pbpub.Code_ERR_DESERIALIZE, err
	}
	return msg, packet[pbEnd:size], pbpub.Code_SUCCESS, nil
}

func (d *Decoder) DecodeS(
	secretKey *[32]byte,
	packet []byte,
) (proto.Message, []byte, pbpub.Code, error) {
	// -------------------------------------------------------------------
	// | Pkg Size | Pkg Type |   Nonce   | Cipher Size | Cipher | Expand |
	// -------------------------------------------------------------------
	// | 2 bytes  | 2 bytes  |  12 bytes |   2 bytes   |   Pb   |   xxx  |
	// -------------------------------------------------------------------
	if !hasValidFrame(packet, 16) {
		return nil, nil, pbpub.Code_ERR_INVALID_PACK_SIZE, ErrPkgSizeInvalid
	}

	var (
		pkgType = binary.LittleEndian.Uint16(packet[2:4])
		msg     proto.Message
	)

	switch pbpub.PacketType(pkgType) {
	case pbpub.PacketType_REQ_SEND_MSG:
		msg = new(pbpub.SendMsgReq)

	default:
		return nil, nil, pbpub.Code_ERR_INVALID_PACK_TYPE, ErrPkgTypeInvalid
	}

	cipherSize := binary.LittleEndian.Uint16(packet[16:18])
	if !d.secureChannel.IsValid(int(cipherSize)) ||
		int(cipherSize) > len(packet)-18 {
		return nil, nil, pbpub.Code_ERR_INVALID_PACK_CIPHER, ErrPkgCipherInvalid
	}

	cipherEnd := 18 + int(cipherSize)
	aad := securePacketAAD(
		binary.LittleEndian.Uint16(packet[:2]),
		pkgType,
		cipherSize,
		packet[cipherEnd:],
	)
	plain, err := d.secureChannel.Open(
		secretKey,
		(*[12]byte)(packet[4:16]),
		packet[18:cipherEnd],
		aad,
	)
	clear(aad)
	slicepool.Put(aad)

	if err != nil {
		return nil, nil, pbpub.Code_ERR_DECRYPT, err
	}
	defer clear(plain)

	if err = d.pbUnmarshal.Unmarshal(plain, msg); err != nil {
		return nil, nil, pbpub.Code_ERR_DESERIALIZE, err
	}
	return msg, packet[cipherEnd:], pbpub.Code_SUCCESS, nil
}

func hasValidFrame(packet []byte, minimumFrameSize int) bool {
	if len(packet) < 2 {
		return false
	}
	frameSize := int(binary.LittleEndian.Uint16(packet[:2]))
	return frameSize == len(packet)-2 && frameSize >= minimumFrameSize
}
