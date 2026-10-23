/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package crypto

import (
	"crypto/ed25519"
	"errors"

	"xxx/internal/hub/core/activation"
)

var (
	ErrSignerInvalidPubKey = errors.New("invalid public key length")
	ErrSignerVerify        = errors.New("signature verification failed")
)

type ed25519Signer struct{}

func NewEd25519Signer() activation.Signer {
	return new(ed25519Signer)
}

func (v *ed25519Signer) Verify(identityPub, payload, sign []byte) error {
	if len(identityPub) != ed25519.PublicKeySize {
		return ErrSignerInvalidPubKey
	}
	if !ed25519.Verify(identityPub, payload, sign) {
		return ErrSignerVerify
	}
	return nil
}
