/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package protocol

import (
	"bytes"
	"encoding/binary"

	pbpub "xxx/api/hub/v2/proto/gen/pub"

	"google.golang.org/protobuf/proto"
)

type Decoder struct{}

type Payload struct {
	Kind    pbpub.MsgKind
	Message proto.Message
	Expand  []byte
}

// NewDecoder creates a decoder for the Hub application payload format.
func NewDecoder() *Decoder {
	return &Decoder{}
}

// DecodePayload validates the payload header and decodes the typed protobuf message.
// The returned message and expand bytes are independent of the input buffer.
//
// [msg_kind: 2][protobuf_length: 2][protobuf: n][expand: remaining]
func (*Decoder) DecodePayload(plain []byte) (*Payload, error) {
	if len(plain) < payloadHeaderSize || len(plain) > maxPlainSize {
		return nil, ErrPkgSizeInvalid
	}

	var (
		kind   = pbpub.MsgKind(binary.LittleEndian.Uint16(plain))
		pbSize = int(binary.LittleEndian.Uint16(plain[msgKindBytes:]))
	)
	if pbSize > len(plain)-payloadHeaderSize {
		return nil, ErrPkgSizeInvalid
	}

	msg := messageFor(kind)
	if msg == nil {
		return nil, ErrMsgKindInvalid
	}

	err := proto.Unmarshal(plain[payloadHeaderSize:payloadHeaderSize+pbSize], msg)
	if err != nil {
		return nil, err
	}

	return &Payload{
		Kind:    kind,
		Message: msg,
		Expand:  bytes.Clone(plain[payloadHeaderSize+pbSize:]),
	}, nil
}

// messageFor allocates the protobuf message for a supported kind, or returns nil.
func messageFor(kind pbpub.MsgKind) proto.Message {
	switch kind {
	case pbpub.MsgKind_REQ_AUTHENTICATE:
		return new(pbpub.AuthenticateReq)
	case pbpub.MsgKind_RESP_AUTHENTICATE:
		return new(pbpub.AuthenticateResp)
	case pbpub.MsgKind_REQ_SEND_MSG:
		return new(pbpub.SendMsgReq)
	case pbpub.MsgKind_RESP_SEND_MSG:
		return new(pbpub.SendMsgResp)
	case pbpub.MsgKind_REQ_ACK_RECV_MSG:
		return new(pbpub.AckRecvMsgReq)
	case pbpub.MsgKind_RESP_ACK_RECV_MSG:
		return new(pbpub.AckRecvMsgResp)
	case pbpub.MsgKind_REQ_DIRECT_FORWARD_MSG:
		return new(pbpub.DirectForwardMsgReq)
	case pbpub.MsgKind_REQ_BROKER_FORWARD_MSG:
		return new(pbpub.BrokerForwardMsgReq)
	default:
		return nil
	}
}
