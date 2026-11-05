/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package noise

import (
	"bytes"

	pbpub "xxx/api/hub/v2/proto/gen/pub"
	"xxx/internal/hub/core/auth"
	"xxx/internal/hub/core/transport"

	slicepool "xxx/pkg/pool/slice"

	flynn "github.com/flynn/noise"
	"golang.org/x/crypto/chacha20poly1305"
	"google.golang.org/protobuf/proto"
)

type responder struct {
	state    *flynn.HandshakeState
	private  []byte
	identity transport.Identity
	maxFrame int
	read     bool
}

func (r *responder) Identity() transport.Identity {
	return r.identity
}

func (r *responder) ReadClientHello(message []byte) error {
	if r.state == nil || r.read || len(message) != ClientHelloSize {
		r.Close()
		return ErrProtocol
	}

	payload, c1, c2, err := r.state.ReadMessage(nil, message)
	if err != nil || len(payload) != 0 || c1 != nil || c2 != nil {
		r.Close()
		return ErrProtocol
	}

	r.read = true
	return nil
}

func (r *responder) WriteServerHello(challenge []byte) (
	[]byte,
	transport.Channel,
	[]byte,
	error,
) {
	if r.state == nil || !r.read || len(challenge) != auth.ChallengeSize {
		r.Close()
		return nil, nil, nil, ErrProtocol
	}
	defer r.Close()

	pb := pbpub.NoiseServerHelloPayload{
		Challenge: slicepool.Get(auth.ChallengeSize),
	}
	copy(pb.Challenge, challenge)

	plBuf := slicepool.Get(proto.Size(&pb))
	defer func() {
		slicepool.PutZeroed(pb.Challenge)
		slicepool.PutZeroed(plBuf)
	}()

	payload, err := proto.MarshalOptions{}.MarshalAppend(plBuf[:0], &pb)
	if err != nil {
		return nil, nil, nil, ErrProtocol
	}

	msgBuf := slicepool.Get(chacha20poly1305.KeySize + len(payload) + chacha20poly1305.Overhead)
	message, recv, send, err := r.state.WriteMessage(msgBuf[:0], payload)
	if err != nil || recv == nil || send == nil {
		slicepool.PutZeroed(msgBuf)
		return nil, nil, nil, ErrProtocol
	}

	ch := &channel{
		recv:     recv,
		send:     send,
		maxFrame: r.maxFrame,
	}
	return message, ch, bytes.Clone(r.state.ChannelBinding()), nil
}

func (r *responder) Close() {
	clear(r.private[:cap(r.private)])
	r.private = nil
	r.state = nil
}
