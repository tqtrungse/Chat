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
	"xxx/internal/hub/protocol/mocks"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

func TestEncoder_Encode_OutputIncludesTrailingHMAC(t *testing.T) {
	ctrl := gomock.NewController(t)
	encoder := NewEncoder(mocks.NewMockSecureChannel(ctrl))
	hmacKey := randomKey(t)
	msg := new(pbpub.ActiveConnReq)
	seq := uint64(0x0807060504030201)

	packet, err := encoder.Encode(
		hmacKey,
		seq,
		pbpub.MsgType_REQ_ACTIVE_CONN,
		msg,
		nil,
	)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(packet), 32)
	require.Equal(t, uint16(len(packet)-2), binary.LittleEndian.Uint16(packet[:2]))

	body := packet[:len(packet)-32]
	gotTag := packet[len(packet)-32:]

	mac := hmac.New(sha256.New, hmacKey[:])
	var seqLE [8]byte
	binary.LittleEndian.PutUint64(seqLE[:], seq)
	mac.Write(seqLE[:])
	mac.Write(body)
	wantTag := mac.Sum(nil)

	require.True(t, hmac.Equal(gotTag, wantTag))
}

func TestEncoder_Encode_WithExpand(t *testing.T) {
	ctrl := gomock.NewController(t)
	encoder := NewEncoder(mocks.NewMockSecureChannel(ctrl))
	hmacKey := randomKey(t)
	msg := new(pbpub.ActiveConnReq)
	expand := []byte("expand-payload")
	seq := uint64(1)

	packet, err := encoder.Encode(
		hmacKey,
		seq,
		pbpub.MsgType_REQ_ACTIVE_CONN,
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
	ctrl := gomock.NewController(t)
	encoder := NewEncoder(mocks.NewMockSecureChannel(ctrl))
	hmacKey := randomKey(t)

	_, err := encoder.Encode(
		hmacKey,
		0,
		pbpub.MsgType_REQ_ACTIVE_CONN,
		new(pbpub.ActiveConnReq),
		make([]byte, maxFrameSize),
	)
	require.ErrorIs(t, err, ErrPkgTooLarge)
}

func TestEncoder_EncodeDecode_RoundTrip(t *testing.T) {
	ctrl := gomock.NewController(t)
	ch := mocks.NewMockSecureChannel(ctrl)
	encoder := NewEncoder(ch)
	decoder := NewDecoder(ch)
	hmacKey := randomKey(t)
	msg := &pbpub.BrokerForwardMsgReq{SendDeviceId: 1, MsgId: 42}
	expand := []byte("expand-payload")
	seq := uint64(42)

	packet, err := encoder.Encode(
		hmacKey,
		seq,
		pbpub.MsgType_REQ_BROKER_FORWARD_MSG,
		msg,
		expand,
	)
	require.NoError(t, err)

	gotMsg, gotExpand, code, err := decoder.Decode(hmacKey, seq, packet)
	require.NoError(t, err)
	require.Equal(t, pbpub.Code_SUCCESS, code)
	require.True(t, proto.Equal(gotMsg, msg))
	require.True(t, bytes.Equal(gotExpand, expand))
}

func TestEnCoder_EncodeDecode_RoundTrip_ForwardSendMsg(t *testing.T) {
	ctrl := gomock.NewController(t)
	ch := mocks.NewMockSecureChannel(ctrl)
	encoder := NewEncoder(ch)
	decoder := NewDecoder(ch)
	hmacKey := randomKey(t)
	msg := &pbpub.BrokerForwardMsgReq{
		SendDeviceId: uint64(1),
		MsgId:        uint64(1),
	}
	seq := uint64(1)

	packet, err := encoder.Encode(
		hmacKey,
		seq,
		pbpub.MsgType_REQ_BROKER_FORWARD_MSG,
		msg,
		nil,
	)
	require.NoError(t, err)

	gotMsg, _, code, err := decoder.Decode(hmacKey, seq, packet)
	require.NoError(t, err)
	require.Equal(t, pbpub.Code_SUCCESS, code)
	require.True(t, proto.Equal(gotMsg, msg))
}

func TestEncoderDecoder_EncodeSDecodeS_RoundTrip(t *testing.T) {
	ch := newSecureChannelHelperTest(t)
	encoder := NewEncoder(ch)
	decoder := NewDecoder(ch)
	secretKey := randomKey(t)
	msg := &pbpub.SendMsgReq{
		ChannelId: uint64(1),
	}
	expand := []byte("expand-payload")
	seq := uint64(9)

	packet, err := encoder.EncodeS(
		secretKey,
		seq,
		pbpub.MsgType_REQ_SEND_MSG,
		msg,
		expand,
	)
	require.NoError(t, err)
	require.Equal(t, uint16(len(packet)-2), binary.LittleEndian.Uint16(packet[:2]))

	gotMsg, gotExpand, code, err := decoder.DecodeS(secretKey, seq, packet)
	require.NoError(t, err)
	require.Equal(t, pbpub.Code_SUCCESS, code)
	require.True(t, proto.Equal(gotMsg, msg))
	require.True(t, bytes.Equal(gotExpand, expand))
}

func TestEncoderDecoder_EncodeSDecodeS_RoundTrip_NoExpand(t *testing.T) {
	ch := newSecureChannelHelperTest(t)
	encoder := NewEncoder(ch)
	decoder := NewDecoder(ch)
	secretKey := randomKey(t)
	msg := &pbpub.SendMsgReq{
		ChannelId: uint64(1),
	}
	seq := uint64(0)

	packet, err := encoder.EncodeS(
		secretKey,
		seq,
		pbpub.MsgType_REQ_SEND_MSG,
		msg,
		nil,
	)
	require.NoError(t, err)

	gotMsg, gotExpand, code, err := decoder.DecodeS(secretKey, seq, packet)
	require.NoError(t, err)
	require.Equal(t, pbpub.Code_SUCCESS, code)
	require.True(t, proto.Equal(gotMsg, msg))
	require.Empty(t, gotExpand)
}

func TestEncoderDecoder_MixedFrameTypesShareSequence(t *testing.T) {
	ch := newSecureChannelHelperTest(t)
	encoder := NewEncoder(ch)
	decoder := NewDecoder(ch)
	hmacKey := randomKey(t)
	secretKey := randomKey(t)

	plainFrame, err := encoder.Encode(
		hmacKey,
		0,
		pbpub.MsgType_REQ_BROKER_FORWARD_MSG,
		new(pbpub.BrokerForwardMsgReq),
		nil,
	)
	require.NoError(t, err)
	_, _, code, err := decoder.Decode(hmacKey, 0, plainFrame)
	require.NoError(t, err)
	require.Equal(t, pbpub.Code_SUCCESS, code)

	secureFrame, err := encoder.EncodeS(
		secretKey,
		1,
		pbpub.MsgType_REQ_SEND_MSG,
		&pbpub.SendMsgReq{ChannelId: 1},
		nil,
	)
	require.NoError(t, err)
	_, _, code, err = decoder.DecodeS(secretKey, 1, secureFrame)
	require.NoError(t, err)
	require.Equal(t, pbpub.Code_SUCCESS, code)
}

func TestEncoderDecoder_EncodeS_NoncesAreUnpredictable(t *testing.T) {
	encoder := NewEncoder(newSecureChannelHelperTest(t))
	secretKey := randomKey(t)
	msg := new(pbpub.SendMsgReq)
	seq := uint64(0)

	p1, err := encoder.EncodeS(
		secretKey,
		seq,
		pbpub.MsgType_REQ_SEND_MSG,
		msg,
		nil,
	)
	require.NoError(t, err)

	p2, err := encoder.EncodeS(
		secretKey,
		seq,
		pbpub.MsgType_REQ_SEND_MSG,
		msg,
		nil,
	)
	require.NoError(t, err)
	require.False(t, bytes.Equal(p1[4:16], p2[4:16]))
}

func TestSecurePacketAAD_BindsFrameMetadataAndSequence(t *testing.T) {
	expand := []byte("expand")
	seq := uint64(0x0807060504030201)
	base := securePacketAAD(64, uint16(pbpub.MsgType_REQ_SEND_MSG), 48, seq, expand)
	wrongType := securePacketAAD(64, uint16(pbpub.MsgType_REQ_ACTIVE_CONN), 48, seq, expand)
	wrongCipherSize := securePacketAAD(64, uint16(pbpub.MsgType_REQ_SEND_MSG), 49, seq, expand)
	wrongFrameSize := securePacketAAD(65, uint16(pbpub.MsgType_REQ_SEND_MSG), 48, seq, expand)
	wrongSeq := securePacketAAD(64, uint16(pbpub.MsgType_REQ_SEND_MSG), 48, seq+1, expand)
	defer func() {
		for _, aad := range [][]byte{base, wrongType, wrongCipherSize, wrongFrameSize, wrongSeq} {
			clear(aad[:cap(aad)])
			slicepool.Put(aad)
		}
	}()

	require.Equal(t, securePacketAADForTest(64, uint16(pbpub.MsgType_REQ_SEND_MSG), 48, seq, expand), base)
	require.NotEqual(t, base, wrongType)
	require.NotEqual(t, base, wrongCipherSize)
	require.NotEqual(t, base, wrongFrameSize)
	require.NotEqual(t, base, wrongSeq)
}

func TestEncodeHandshake_NoSessionMAC(t *testing.T) {
	ctrl := gomock.NewController(t)
	encoder := NewEncoder(mocks.NewMockSecureChannel(ctrl))
	want := &pbpub.ActiveConnResp{Nonce: make([]byte, 32)}
	packet, err := encoder.EncodeHandshake(pbpub.MsgType_RESP_ACTIVE_CONN, want)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(packet), 6)
	require.Equal(t, uint16(len(packet)-2), binary.LittleEndian.Uint16(packet[:2]))
	require.Equal(t, uint16(pbpub.MsgType_RESP_ACTIVE_CONN), binary.LittleEndian.Uint16(packet[2:4]))
	require.Equal(t, uint16(len(packet)-6), binary.LittleEndian.Uint16(packet[4:6]))

	var got pbpub.ActiveConnResp
	require.NoError(t, proto.Unmarshal(packet[6:], &got))
	require.True(t, proto.Equal(want, &got))
}
