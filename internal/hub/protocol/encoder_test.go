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
	"testing"

	pbpub "xxx/api/hub/v1/proto/gen/pub"

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

	wantLen := 4 + len(pbBytes) + len(expand) + 32
	require.Equal(t, wantLen, len(packet))
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
	require.False(t, bytes.Equal(p1[2:14], p2[2:14]))
}
