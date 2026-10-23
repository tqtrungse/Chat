/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

// Package noisetest provides a reference initiator using the pinned Noise library.
package noisetest

import (
	"bytes"
	"encoding/binary"
	"errors"

	pb "xxx/api/hub/v2/proto/gen/pub"
	"xxx/internal/hub/core/auth"
	"xxx/internal/hub/core/protocol"
	"xxx/internal/shared/hubendpoint"
	slicepool "xxx/pkg/pool/slice"

	flynn "github.com/flynn/noise"
	"golang.org/x/crypto/chacha20poly1305"
	"google.golang.org/protobuf/proto"
)

var (
	ErrServerFrameInvalid = errors.New("invalid server frame")
	ErrServerHelloInvalid = errors.New("invalid server hello")
)

type Peer struct {
	State              *flynn.HandshakeState
	send, recv         *flynn.CipherState
	Binding, Challenge []byte
}

func New(pub []byte, audience string, id uint32) (*Peer, error) {
	p, err := hubendpoint.Prologue(audience, id)
	if err != nil {
		return nil, err
	}
	defer slicepool.Put(p)

	h, err := flynn.NewHandshakeState(flynn.Config{CipherSuite: flynn.NewCipherSuite(flynn.DH25519, flynn.CipherChaChaPoly, flynn.HashSHA256), Pattern: flynn.HandshakeNK, Initiator: true, PeerStatic: bytes.Clone(pub), Prologue: p})
	if err != nil {
		return nil, err
	}
	return &Peer{State: h}, nil
}
func (p *Peer) Hello(id uint32) ([]byte, error) {
	m, _, _, err := p.State.WriteMessage(nil, nil)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, protocol.FramePrefixSize, protocol.FramePrefixSize+protocol.ClientHelloHeaderSize+len(m))
	binary.LittleEndian.PutUint16(frame, uint16(protocol.ClientHelloHeaderSize+len(m)))
	frame = append(frame, hubendpoint.WireVersion)
	frame = binary.LittleEndian.AppendUint32(frame, id)
	return append(frame, m...), nil
}
func (p *Peer) Finish(frame []byte) error {
	if len(frame) < protocol.FramePrefixSize || int(binary.LittleEndian.Uint16(frame)) != len(frame)-protocol.FramePrefixSize {
		return ErrServerFrameInvalid
	}
	payload, send, recv, err := p.State.ReadMessage(nil, frame[protocol.FramePrefixSize:])
	if err != nil {
		return err
	}
	var hello pb.NoiseServerHelloPayload
	if err := proto.Unmarshal(payload, &hello); err != nil ||
		len(hello.Challenge) != auth.ChallengeSize ||
		len(hello.ProtoReflect().GetUnknown()) != 0 ||
		len(payload) != proto.Size(&hello) || send == nil || recv == nil {
		return ErrServerHelloInvalid
	}
	p.Binding = bytes.Clone(p.State.ChannelBinding())
	p.Challenge = bytes.Clone(hello.Challenge)
	p.send = send
	p.recv = recv
	p.State = nil
	return nil
}
func (p *Peer) Encrypt(plain []byte) ([]byte, error) {
	frame := make([]byte, protocol.FramePrefixSize)
	binary.LittleEndian.PutUint16(frame, uint16(len(plain)+chacha20poly1305.Overhead))
	return p.send.Encrypt(frame, frame, plain)
}
func (p *Peer) Decrypt(frame []byte) ([]byte, error) {
	return p.recv.Decrypt(nil, frame[:protocol.FramePrefixSize], frame[protocol.FramePrefixSize:])
}
