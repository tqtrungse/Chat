/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package noise

import (
	"xxx/internal/shared/hubendpoint"

	"golang.org/x/crypto/chacha20poly1305"
)

const (
	// MinFrameBytes is the minimum supported frame-body limit in configuration.
	MinFrameBytes = 512
	// ClientHelloSize is the NK first message with an empty application payload:
	// one ephemeral X25519 public key followed by its AEAD authentication tag.
	ClientHelloSize = hubendpoint.X25519PublicKeySize + chacha20poly1305.Overhead
	// MaxKeyringBytes bounds the private keyring JSON input to 64 KiB.
	MaxKeyringBytes = 64 * 1024
	// MaxKeyringKeys bounds static keys retained for service key rotation.
	MaxKeyringKeys = 32
	// Reject owner execute, group write/execute, and all access for other users.
	keyringDisallowedPermissions = 0o137
)
