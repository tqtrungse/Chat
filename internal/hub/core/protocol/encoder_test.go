/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package protocol

import (
	"bytes"
	"testing"

	pbpub "xxx/api/hub/v2/proto/gen/pub"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestPayloadRoundTripOwnsExpandAndHasNoCrypto(t *testing.T) {
	msg := &pbpub.SendMsgReq{
		ChannelId:     2,
		DedupKey:      3,
		RecvDeviceIds: []uint64{4},
	}
	want := proto.Clone(msg)
	expand := []byte("opaque E2EE")

	plain, err := NewEncoder().EncodePayload(
		pbpub.MsgKind_REQ_SEND_MSG,
		msg,
		expand,
	)
	require.NoError(t, err)
	defer func() {
		clear(plain[:cap(plain)])
		slicepool.Put(plain)
	}()
	clear(expand[:cap(expand)])
	proto.Reset(msg)

	got, err := NewDecoder().DecodePayload(plain)
	require.NoError(t, err)
	require.Equal(t, pbpub.MsgKind_REQ_SEND_MSG, got.Kind)
	require.True(t, proto.Equal(want, got.Message))
	clear(plain[:cap(plain)])
	require.Equal(t, "opaque E2EE", string(got.Expand))
}

func TestEncodePayloadWireFormat(t *testing.T) {
	plain, err := NewEncoder().EncodePayload(
		pbpub.MsgKind_RESP_SEND_MSG,
		&pbpub.SendMsgResp{ChannelId: 128, MsgId: 1},
		[]byte{0xAA, 0xBB},
	)
	require.NoError(t, err)
	defer func() {
		clear(plain[:cap(plain)])
		slicepool.Put(plain)
	}()
	// Type 503 and protobuf size 5 are LE16; expand is outside protobuf_length.
	require.Equal(
		t,
		[]byte{
			0xF7, 0x01, 0x05, 0x00,
			0x08, 0x80, 0x01, 0x10, 0x01,
			0xAA, 0xBB,
		},
		plain,
	)
}

func TestEncodePayloadProtobufSizeBounds(t *testing.T) {
	encoder := NewEncoder()
	// bytes field 1 adds one tag byte and a three-byte length varint.
	msg := &pbpub.AuthenticateReq{Ticket: bytes.Repeat([]byte{0xA5}, 65511)}
	plain, err := encoder.EncodePayload(
		pbpub.MsgKind_REQ_AUTHENTICATE,
		msg,
		nil,
	)
	require.NoError(t, err)
	defer func() {
		clear(plain[:cap(plain)])
		slicepool.Put(plain)
	}()
	require.Len(t, plain, 65519)

	decoded, err := NewDecoder().DecodePayload(plain)
	require.NoError(t, err)
	require.True(t, proto.Equal(msg, decoded.Message))
	require.Empty(t, decoded.Expand)

	for _, size := range []int{65512, 65536} {
		msg.Ticket = make([]byte, size)
		rejected, encodeErr := encoder.EncodePayload(
			pbpub.MsgKind_REQ_AUTHENTICATE,
			msg,
			nil,
		)
		require.ErrorIs(t, encodeErr, ErrPkgTooLarge)
		require.Nil(t, rejected)
	}
}

func TestPayloadSizeBounds(t *testing.T) {
	e := NewEncoder()

	msg := &pbpub.AuthenticateResp{}
	plain, err := e.EncodePayload(
		pbpub.MsgKind_RESP_AUTHENTICATE,
		msg,
		make([]byte, maxPayloadBodySize),
	)
	require.NoError(t, err)
	defer func() {
		clear(plain[:cap(plain)])
		slicepool.Put(plain)
	}()
	require.Len(t, plain, maxPlainSize)

	decoded, err := NewDecoder().DecodePayload(plain)
	require.NoError(t, err)
	require.True(t, proto.Equal(msg, decoded.Message))
	require.Len(t, decoded.Expand, maxPayloadBodySize)

	_, err = NewDecoder().DecodePayload(make([]byte, 65520))
	require.ErrorIs(t, err, ErrPkgSizeInvalid)

	_, err = e.EncodePayload(pbpub.MsgKind_RESP_AUTHENTICATE, msg, make([]byte, 65516))
	require.ErrorIs(t, err, ErrPkgTooLarge)

	_, err = e.EncodePayload(pbpub.MsgKind_UNKNOWN, msg, nil)
	require.ErrorIs(t, err, ErrMsgKindInvalid)

	_, err = e.EncodePayload(pbpub.MsgKind_RESP_AUTHENTICATE, nil, nil)
	require.Error(t, err)
}

func TestPayloadWithProtobufAtSizeLimit(t *testing.T) {
	// channel_id=128 occupies three protobuf bytes, leaving 65512 for expand.
	msg := &pbpub.SendMsgResp{ChannelId: 128}
	expand := bytes.Repeat([]byte{0xA5}, 65512)
	encoder := NewEncoder()
	plain, err := encoder.EncodePayload(pbpub.MsgKind_RESP_SEND_MSG, msg, expand)
	require.NoError(t, err)
	defer func() {
		clear(plain[:cap(plain)])
		slicepool.Put(plain)
	}()
	require.Len(t, plain, maxPlainSize)

	decoded, err := NewDecoder().DecodePayload(plain)
	require.NoError(t, err)
	require.Equal(t, pbpub.MsgKind_RESP_SEND_MSG, decoded.Kind)
	require.True(t, proto.Equal(msg, decoded.Message))
	require.Equal(t, expand, decoded.Expand)

	_, err = encoder.EncodePayload(
		pbpub.MsgKind_RESP_SEND_MSG,
		msg,
		append(expand, 0),
	)
	require.ErrorIs(t, err, ErrPkgTooLarge)
}
