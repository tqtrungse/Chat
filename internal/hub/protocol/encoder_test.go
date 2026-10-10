/*
 * Copyright 2026 tqtrungse@gmail.com. All rights reserved.
 * Use of this pub code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package protocol

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	pbpub "xxx/api/hub/v1/proto/gen/pub"
	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestEncoder_Encode_OutputIncludesTrailingHMAC(t *testing.T) {
	encoder := NewEncoder(NewMockSecureChannel())
	hmacKey := randomKey(t)
	msg := new(pbpub.ActiveConnReq)

	packet, err := encoder.Encode(
		hmacKey,
		pbpub.PacketType_REQ_ACTIVE_CONN,
		msg,
		nil,
	)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(packet), 32)
	require.Equal(t, uint16(len(packet)-2), binary.LittleEndian.Uint16(packet[:2]))

	body := packet[:len(packet)-32]
	gotTag := packet[len(packet)-32:]

	mac := hmac.New(sha256.New, hmacKey[:])
	mac.Write(body)
	wantTag := mac.Sum(nil)

	require.True(t, hmac.Equal(gotTag, wantTag))
}

func TestEncoder_Encode_WithExpand(t *testing.T) {
	encoder := NewEncoder(NewMockSecureChannel())
	hmacKey := randomKey(t)
	msg := new(pbpub.ActiveConnReq)
	expand := []byte("expand-payload")

	packet, err := encoder.Encode(
		hmacKey,
		pbpub.PacketType_REQ_ACTIVE_CONN,
		msg,
		expand,
	)
	if err != nil {
		t.Fatalf("Encode returned error: %v", err)
	}

	pbBytes, err := proto.Marshal(msg)
	require.NoError(t, err)

	wantLen := 38 + len(pbBytes) + len(expand)
	require.Equal(t, wantLen, len(packet))
	require.Equal(t, uint16(len(packet)-2), binary.LittleEndian.Uint16(packet[:2]))
}

func TestEncoder_Encode_RejectsFrameTooLarge(t *testing.T) {
	encoder := NewEncoder(NewMockSecureChannel())
	hmacKey := randomKey(t)

	_, err := encoder.Encode(
		hmacKey,
		pbpub.PacketType_REQ_ACTIVE_CONN,
		new(pbpub.ActiveConnReq),
		make([]byte, maxFrameSize),
	)
	require.ErrorIs(t, err, ErrPkgTooLarge)
}

func TestEncoder_EncodeDecode_RoundTrip(t *testing.T) {
	secureChannel := NewMockSecureChannel()
	encoder := NewEncoder(secureChannel)
	decoder := NewDecoder(secureChannel)
	hmacKey := randomKey(t)
	msg := new(pbpub.ActiveConnReq)
	expand := []byte("expand-payload")

	packet, err := encoder.Encode(
		hmacKey,
		pbpub.PacketType_REQ_ACTIVE_CONN,
		msg,
		expand,
	)
	require.NoError(t, err)

	gotMsg, gotExpand, code, err := decoder.Decode(hmacKey, packet)
	require.NoError(t, err)
	require.Equal(t, pbpub.Code_SUCCESS, code)
	require.True(t, proto.Equal(gotMsg, msg))
	require.True(t, bytes.Equal(gotExpand, expand))
}

func TestEnCoder_EncodeDecode_RoundTrip_ForwardSendMsg(t *testing.T) {
	secureChannel := NewMockSecureChannel()
	encoder := NewEncoder(secureChannel)
	decoder := NewDecoder(secureChannel)
	hmacKey := randomKey(t)
	msg := &pbpub.BrokerForwardMsgReq{
		SendDeviceId: uint64(1),
		MsgId:        uint64(1),
	}

	packet, err := encoder.Encode(
		hmacKey,
		pbpub.PacketType_REQ_BROKER_FORWARD_MSG,
		msg,
		nil,
	)
	require.NoError(t, err)

	gotMsg, _, code, err := decoder.Decode(hmacKey, packet)
	require.NoError(t, err)
	require.Equal(t, pbpub.Code_SUCCESS, code)
	require.True(t, proto.Equal(gotMsg, msg))
}

func TestEncoderDecoder_EncodeSDecodeS_RoundTrip(t *testing.T) {
	secureChannel := NewMockSecureChannel()
	encoder := NewEncoder(secureChannel)
	decoder := NewDecoder(secureChannel)
	secretKey := randomKey(t)
	msg := &pbpub.SendMsgReq{
		ChannelId: uint64(1),
	}
	expand := []byte("expand-payload")

	packet, err := encoder.EncodeS(
		secretKey,
		pbpub.PacketType_REQ_SEND_MSG,
		msg,
		expand,
	)
	require.NoError(t, err)
	require.Equal(t, uint16(len(packet)-2), binary.LittleEndian.Uint16(packet[:2]))

	gotMsg, gotExpand, code, err := decoder.DecodeS(secretKey, packet)
	require.NoError(t, err)
	require.Equal(t, pbpub.Code_SUCCESS, code)
	require.True(t, proto.Equal(gotMsg, msg))
	require.True(t, bytes.Equal(gotExpand, expand))
}

func TestEncoderDecoder_EncodeSDecodeS_RoundTrip_NoExpand(t *testing.T) {
	secureChannel := NewMockSecureChannel()
	encoder := NewEncoder(secureChannel)
	decoder := NewDecoder(secureChannel)
	secretKey := randomKey(t)
	msg := &pbpub.SendMsgReq{
		ChannelId: uint64(1),
	}

	packet, err := encoder.EncodeS(
		secretKey,
		pbpub.PacketType_REQ_SEND_MSG,
		msg,
		nil,
	)
	require.NoError(t, err)

	gotMsg, gotExpand, code, err := decoder.DecodeS(secretKey, packet)
	require.NoError(t, err)
	require.Equal(t, pbpub.Code_SUCCESS, code)
	require.True(t, proto.Equal(gotMsg, msg))
	require.Empty(t, gotExpand)
}

func TestEncoderDecoder_EncodeS_NoncesAreUnpredictable(t *testing.T) {
	secureChannel := NewMockSecureChannel()
	encoder := NewEncoder(secureChannel)
	secretKey := randomKey(t)
	msg := new(pbpub.SendMsgReq)

	p1, err := encoder.EncodeS(
		secretKey,
		pbpub.PacketType_REQ_SEND_MSG,
		msg,
		nil,
	)
	require.NoError(t, err)

	p2, err := encoder.EncodeS(
		secretKey,
		pbpub.PacketType_REQ_SEND_MSG,
		msg,
		nil,
	)
	require.NoError(t, err)
	require.False(t, bytes.Equal(p1[4:16], p2[4:16]))
}

func TestSecurePacketAAD_BindsPacketTypeAndCipherSize(t *testing.T) {
	expand := []byte("expand")
	base := securePacketAAD(64, uint16(pbpub.PacketType_REQ_SEND_MSG), 48, expand)
	wrongType := securePacketAAD(64, uint16(pbpub.PacketType_REQ_ACTIVE_CONN), 48, expand)
	wrongCipherSize := securePacketAAD(64, uint16(pbpub.PacketType_REQ_SEND_MSG), 49, expand)
	wrongFrameSize := securePacketAAD(65, uint16(pbpub.PacketType_REQ_SEND_MSG), 48, expand)
	defer func() {
		for _, aad := range [][]byte{base, wrongType, wrongCipherSize, wrongFrameSize} {
			clear(aad[:cap(aad)])
			slicepool.Put(aad)
		}
	}()

	require.Equal(t, securePacketAADForTest(64, uint16(pbpub.PacketType_REQ_SEND_MSG), 48, expand), base)
	require.NotEqual(t, base, wrongType)
	require.NotEqual(t, base, wrongCipherSize)
	require.NotEqual(t, base, wrongFrameSize)
}

func TestEncodeHandshake_NoSessionMAC(t *testing.T) {
	secureChannel := NewMockSecureChannel()
	encoder := NewEncoder(secureChannel)
	want := &pbpub.ActiveConnChallengeResp{Nonce: make([]byte, 32)}
	packet, err := encoder.EncodeHandshake(pbpub.PacketType_RESP_ACTIVE_CONN_CHALLENGE, want)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(packet), 6)
	require.Equal(t, uint16(len(packet)-2), binary.LittleEndian.Uint16(packet[:2]))
	require.Equal(t, uint16(pbpub.PacketType_RESP_ACTIVE_CONN_CHALLENGE), binary.LittleEndian.Uint16(packet[2:4]))
	require.Equal(t, uint16(len(packet)-6), binary.LittleEndian.Uint16(packet[4:6]))

	var got pbpub.ActiveConnChallengeResp
	require.NoError(t, proto.Unmarshal(packet[6:], &got))
	require.True(t, proto.Equal(want, &got))
}
