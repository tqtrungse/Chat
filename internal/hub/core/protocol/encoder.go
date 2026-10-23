/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package protocol

import (
	"encoding/binary"
	"math"

	pbpub "xxx/api/hub/v2/proto/gen/pub"

	slicepool "xxx/pkg/pool/slice"

	"google.golang.org/protobuf/proto"
)

type Encoder struct {
	pbOptions proto.MarshalOptions
}

// NewEncoder creates an encoder for typed protobuf payloads and optional expand bytes.
func NewEncoder() *Encoder {
	return &Encoder{
		pbOptions: proto.MarshalOptions{},
	}
}

// EncodePayload encodes the message type, protobuf length, message, and expand bytes
// into an owned pooled buffer without retaining caller buffers.
// The caller must clear the buffer and return it to slicepool after use.
func (e *Encoder) EncodePayload(
	kind pbpub.MsgKind,
	msg proto.Message,
	expand []byte,
) ([]byte, error) {
	if !validKind(kind) || msg == nil {
		return nil, ErrMsgKindInvalid
	}

	pbSize := e.pbOptions.Size(msg)
	if pbSize > math.MaxUint16 ||
		len(expand) > maxPayloadBodySize ||
		pbSize > maxPayloadBodySize-len(expand) {
		return nil, ErrPkgTooLarge
	}

	buf := slicepool.Get(payloadHeaderSize + pbSize + len(expand))
	out := buf[:payloadHeaderSize]
	binary.LittleEndian.PutUint16(out, uint16(kind))
	binary.LittleEndian.PutUint16(out[msgKindBytes:], uint16(pbSize))
	out, err := e.pbOptions.MarshalAppend(out, msg)
	if err == nil && len(out) != payloadHeaderSize+pbSize {
		err = ErrPkgSizeInvalid
	}
	if err != nil {
		clear(buf[:cap(buf)])
		slicepool.Put(buf)
		return nil, err
	}
	return append(out, expand...), nil
}

// validKind reports whether the message kind has a supported wire representation.
func validKind(kind pbpub.MsgKind) bool {
	switch kind {
	case
		pbpub.MsgKind_REQ_AUTHENTICATE,
		pbpub.MsgKind_RESP_AUTHENTICATE,
		pbpub.MsgKind_REQ_SEND_MSG,
		pbpub.MsgKind_RESP_SEND_MSG,
		pbpub.MsgKind_REQ_ACK_RECV_MSG,
		pbpub.MsgKind_RESP_ACK_RECV_MSG,
		pbpub.MsgKind_REQ_DIRECT_FORWARD_MSG,
		pbpub.MsgKind_REQ_BROKER_FORWARD_MSG:
		return true
	default:
		return false
	}
}
