/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package noise

import (
	"xxx/internal/hub/core/protocol"
	"xxx/internal/hub/core/transport"
	"xxx/internal/shared/hubendpoint"

	slicepool "xxx/pkg/pool/slice"

	flynn "github.com/flynn/noise"
)

var suite = flynn.NewCipherSuite(
	flynn.DH25519,
	flynn.CipherChaChaPoly,
	flynn.HashSHA256,
)

type Factory struct {
	keys     *Keyring
	audience string
	maxFrame int
}

func NewFactory(keys *Keyring, audience string, maxFrame int) (*Factory, error) {
	if keys == nil ||
		maxFrame < MinFrameBytes ||
		maxFrame > protocol.MaxFrameSize {
		return nil, ErrProtocol
	}

	err := hubendpoint.ValidateAudience(audience)
	if err != nil {
		return nil, err
	}

	return &Factory{
		keys:     keys,
		audience: audience,
		maxFrame: maxFrame,
	}, nil
}

func (f *Factory) NewResponder(id uint32) (transport.Responder, error) {
	pair, ok := f.keys.Keypair(id)
	if !ok {
		return nil, ErrProtocol
	}

	prologue, err := hubendpoint.Prologue(f.audience, id)
	if err != nil {
		clear(pair.Private[:cap(pair.Private)])
		return nil, err
	}
	defer slicepool.Put(prologue)

	state, err := flynn.NewHandshakeState(flynn.Config{
		CipherSuite:   suite,
		Pattern:       flynn.HandshakeNK,
		StaticKeypair: pair,
		Prologue:      prologue,
	})
	if err != nil {
		clear(pair.Private[:cap(pair.Private)])
		return nil, err
	}

	identity := transport.Identity{
		Audience: f.audience,
		KeyID:    id,
		Version:  hubendpoint.WireVersion,
	}
	copy(identity.PublicKey[:], pair.Public)

	return &responder{
		state:    state,
		private:  pair.Private,
		identity: identity,
		maxFrame: f.maxFrame,
	}, nil
}
