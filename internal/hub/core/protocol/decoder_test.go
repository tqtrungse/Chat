/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package protocol

import (
	"bytes"
	"encoding/binary"
	"testing"

	pbpub "xxx/api/hub/v2/proto/gen/pub"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestPayloadMessageTypes(t *testing.T) {
	for _, tc := range []struct {
		kind pbpub.MsgKind
		msg  proto.Message
	}{
		{
			pbpub.MsgKind_REQ_AUTHENTICATE,
			&pbpub.AuthenticateReq{
				Ticket:    []byte{1, 2},
				Signature: []byte{3, 4},
			},
		},
		{
			pbpub.MsgKind_RESP_AUTHENTICATE,
			&pbpub.AuthenticateResp{Code: pbpub.Code_ERR_INTERNAL},
		},
		{
			pbpub.MsgKind_REQ_SEND_MSG,
			&pbpub.SendMsgReq{
				ChannelId:     1,
				DedupKey:      2,
				RecvDeviceIds: []uint64{3, 4},
			},
		},
		{
			pbpub.MsgKind_RESP_SEND_MSG,
			&pbpub.SendMsgResp{ChannelId: 5, MsgId: 6},
		},
		{
			pbpub.MsgKind_REQ_ACK_RECV_MSG,
			&pbpub.AckRecvMsgReq{ChannelId: 7, MsgId: 8},
		},
		{
			pbpub.MsgKind_RESP_ACK_RECV_MSG,
			&pbpub.AckRecvMsgResp{Code: pbpub.Code_ERR_INTERNAL},
		},
		{
			pbpub.MsgKind_REQ_DIRECT_FORWARD_MSG,
			&pbpub.DirectForwardMsgReq{
				SendDeviceId: 9,
				MsgId:        10,
				ChannelId:    11,
			},
		},
		{
			pbpub.MsgKind_REQ_BROKER_FORWARD_MSG,
			&pbpub.BrokerForwardMsgReq{
				SendDeviceId: 12,
				MsgId:        13,
				ChannelId:    14,
			},
		},
	} {
		t.Run(
			tc.kind.String(),
			func(t *testing.T) {
				for _, empty := range []bool{false, true} {
					want := proto.Clone(tc.msg)
					if empty {
						proto.Reset(want)
					}
					protobuf, err := proto.Marshal(want)
					require.NoError(t, err)

					expand := []byte{0xFF, 0xFE} // Deliberately invalid as protobuf.
					wire := make([]byte, 4)
					binary.LittleEndian.PutUint16(wire, uint16(tc.kind))
					binary.LittleEndian.PutUint16(wire[2:], uint16(len(protobuf)))
					wire = append(wire, protobuf...)
					wire = append(wire, expand...)
					encoded, err := NewEncoder().EncodePayload(tc.kind, want, expand)
					require.NoError(t, err)
					t.Cleanup(func() {
						clear(encoded[:cap(encoded)])
						slicepool.Put(encoded)
					})
					require.Equal(t, wire, encoded)

					// Decode independently constructed bytes so encoder/decoder bugs
					// cannot cancel each other out in a round trip.
					before := bytes.Clone(wire)
					decoded, err := NewDecoder().DecodePayload(wire)
					require.NoError(t, err)
					require.Equal(t, before, wire)
					clear(wire)
					require.Equal(t, tc.kind, decoded.Kind)
					require.IsType(t, want, decoded.Message)
					require.True(t, proto.Equal(want, decoded.Message))
					require.Equal(t, expand, decoded.Expand)
				}
			},
		)
	}
}

func TestDecodeRejectsUnknownMalformedAndTruncatedPayloads(t *testing.T) {
	payload := func(kind pbpub.MsgKind, size uint16, body []byte) []byte {
		header := make([]byte, 4)
		binary.LittleEndian.PutUint16(header, uint16(kind))
		binary.LittleEndian.PutUint16(header[2:], size)
		return append(header, body...)
	}

	for name, tc := range map[string]struct {
		wire []byte
		want error
	}{
		"empty": {
			nil,
			ErrPkgSizeInvalid,
		},
		"short type": {
			[]byte{1},
			ErrPkgSizeInvalid,
		},
		"short header": {
			[]byte{1, 0, 0},
			ErrPkgSizeInvalid,
		},
		"oversized": {
			payload(pbpub.MsgKind_REQ_AUTHENTICATE, 0, make([]byte, 65516)),
			ErrPkgSizeInvalid,
		},
		"unknown type": {
			payload(pbpub.MsgKind(65535), 0, nil),
			ErrMsgKindInvalid,
		},
		"zero type": {
			payload(pbpub.MsgKind_UNKNOWN, 0, nil),
			ErrMsgKindInvalid,
		},
		"truncated protobuf": {
			payload(pbpub.MsgKind_REQ_AUTHENTICATE, 3, []byte{1}),
			ErrPkgSizeInvalid,
		},
		"malformed protobuf": {
			payload(pbpub.MsgKind_REQ_AUTHENTICATE, 1, []byte{255}),
			nil,
		},
	} {
		t.Run(
			name,
			func(t *testing.T) {
				decoded, err := NewDecoder().DecodePayload(tc.wire)
				require.Error(t, err)
				if tc.want != nil {
					require.ErrorIs(t, err, tc.want)
				}
				require.Nil(t, decoded)
			},
		)
	}
}

func FuzzDecodePayload(f *testing.F) {
	header := make([]byte, 4)
	binary.LittleEndian.PutUint16(header, uint16(pbpub.MsgKind_REQ_AUTHENTICATE))
	f.Add(header)
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _ = NewDecoder().DecodePayload(b)
	})
}
