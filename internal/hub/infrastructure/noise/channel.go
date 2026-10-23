/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package noise

import (
	"encoding/binary"

	"xxx/internal/hub/core/protocol"

	slicepool "xxx/pkg/pool/slice"

	flynn "github.com/flynn/noise"
	"golang.org/x/crypto/chacha20poly1305"
)

type channel struct {
	recv     *flynn.CipherState
	send     *flynn.CipherState
	maxFrame int
}

func (ch *channel) EncryptRecord(plain []byte) ([]byte, error) {
	if ch.send == nil ||
		len(plain) > ch.maxFrame-chacha20poly1305.Overhead {
		return nil, ErrProtocol
	}

	buf := slicepool.Get(protocol.FramePrefixSize + len(plain) + chacha20poly1305.Overhead)
	prefix := buf[:protocol.FramePrefixSize]
	binary.LittleEndian.PutUint16(prefix, uint16(len(plain)+chacha20poly1305.Overhead))

	out, err := ch.send.Encrypt(prefix, prefix, plain)
	if err != nil {
		clear(buf[:cap(buf)])
		slicepool.Put(buf)
		ch.Close()
		return nil, err
	}
	return out, nil
}

func (ch *channel) DecryptRecord(frame []byte) ([]byte, error) {
	if ch.recv == nil ||
		len(frame) < protocol.FramePrefixSize+chacha20poly1305.Overhead ||
		len(frame)-protocol.FramePrefixSize > ch.maxFrame ||
		int(binary.LittleEndian.Uint16(frame)) != len(frame)-protocol.FramePrefixSize {
		ch.Close()
		return nil, ErrProtocol
	}

	buf := slicepool.Get(len(frame) - protocol.FramePrefixSize - chacha20poly1305.Overhead)
	plain, err := ch.recv.Decrypt(
		buf[:0],
		frame[:protocol.FramePrefixSize],
		frame[protocol.FramePrefixSize:],
	)
	if err != nil {
		clear(buf[:cap(buf)])
		slicepool.Put(buf)
		ch.Close()
		return nil, err
	}
	return plain, nil
}

func (ch *channel) Close() {
	ch.recv = nil
	ch.send = nil
}
