/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

// Package transport defines connection-owned Noise state. All methods except
// factory construction run on the owning connection event loop.
package transport

import "xxx/internal/shared/hubendpoint"

type Identity struct {
	Audience  string
	KeyID     uint32
	PublicKey [hubendpoint.X25519PublicKeySize]byte
	Version   uint8
}

type Channel interface {
	// EncryptRecord encrypts and frames one outgoing record, transferring buffer ownership on success.
	// The caller must clear its full capacity and return it to slicepool after use.
	// On error, the implementation releases its buffers and returns nil.
	EncryptRecord(plaintext []byte) ([]byte, error)

	// DecryptRecord authenticates and decrypts an incoming record.
	// It has the same buffer ownership contract as EncryptRecord.
	DecryptRecord(frame []byte) ([]byte, error)

	// Close releases channel state; the channel must not be used afterward.
	Close()
}

type Responder interface {
	// ReadClientHello validates and consumes the client's first Noise message.
	ReadClientHello(message []byte) error

	// WriteServerHello completes the handshake with the server challenge and
	// transfers an owned pooled message and channel on success.
	// The caller must clear and return message to slicepool, and close channel
	// when it is no longer needed. Binding is an independent owned copy.
	WriteServerHello(challenge []byte) (
		message []byte,
		channel Channel,
		binding []byte,
		err error,
	)

	// Identity returns the local Hub identity selected for this handshake.
	Identity() Identity

	// Close releases handshake resources without closing a transferred channel.
	Close()
}

type Factory interface {
	// NewResponder creates a handshake responder for the selected static key ID.
	NewResponder(keyID uint32) (Responder, error)
}
