/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package noise

import (
	"bytes"
	"crypto/ecdh"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"xxx/internal/shared/hubendpoint"

	flynn "github.com/flynn/noise"
)

// KeyFile is the private provisioning format. Never send it to IAM or clients.
type KeyFile struct {
	Keys []KeyEntry `json:"keys"`
}

type KeyEntry struct {
	KeyID      uint32 `json:"key_id"`
	PrivateKey []byte `json:"private_key"`
	PublicKey  []byte `json:"public_key"`
}

// Keyring owns service static keys. It is immutable until Close; callers must
// stop all users before Close. Per-connection ephemeral/traffic keys live
// elsewhere and must never be written to this file.
type Keyring struct {
	currentKeyID uint32
	keys         map[uint32]flynn.DHKey
}

func LoadKeyring(path string, currentKeyID uint32) (*Keyring, error) {
	if currentKeyID == 0 {
		return nil, ErrKeyIDRequired
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open Noise keyring: %w", err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat Noise keyring: %w", err)
	}
	// Allow 0600/0400, and group-read for Kubernetes fsGroup Secret volumes.
	// Follow projected Secret symlinks via Open/Stat rather than rejecting them.
	if !info.Mode().IsRegular() || info.Mode().Perm()&keyringDisallowedPermissions != 0 {
		return nil, ErrKeyringPermissions
	}

	data, err := io.ReadAll(io.LimitReader(f, MaxKeyringBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read Noise keyring: %w", err)
	}
	defer clear(data[:cap(data)])

	if len(data) > MaxKeyringBytes {
		return nil, fmt.Errorf("%w: limit %d bytes", ErrKeyringTooLarge, MaxKeyringBytes)
	}

	var file KeyFile
	defer func() {
		for _, entry := range file.Keys {
			clear(entry.PrivateKey[:cap(entry.PrivateKey)])
		}
	}()

	err = decodeKeyFile(data, &file)
	if err != nil {
		// Decoder errors can include field values; don't expose secret input.
		return nil, ErrKeyringJSONInvalid
	}

	if len(file.Keys) == 0 || len(file.Keys) > MaxKeyringKeys {
		return nil, fmt.Errorf(
			"%w: want 1..%d keys, got %d",
			ErrKeyringKeyCountInvalid, MaxKeyringKeys, len(file.Keys),
		)
	}

	ring := &Keyring{
		currentKeyID: currentKeyID,
		keys:         make(map[uint32]flynn.DHKey, len(file.Keys)),
	}
	fail := func(err error) (*Keyring, error) {
		ring.Close()
		return nil, err
	}

	for _, entry := range file.Keys {
		if entry.KeyID == 0 {
			return fail(ErrKeyIDRequired)
		}

		_, exists := ring.keys[entry.KeyID]
		if exists {
			return fail(fmt.Errorf("duplicate Noise key ID %d", entry.KeyID))
		}

		privateKey, keyErr := ecdh.X25519().NewPrivateKey(entry.PrivateKey)
		if keyErr != nil || len(entry.PublicKey) != hubendpoint.X25519PublicKeySize {
			return fail(
				fmt.Errorf(
					"noise key %d: invalid private key or public key size (want %d bytes)",
					entry.KeyID, hubendpoint.X25519PublicKeySize,
				),
			)
		}

		derived := privateKey.PublicKey().Bytes()
		if subtle.ConstantTimeCompare(derived, entry.PublicKey) != 1 {
			return fail(fmt.Errorf("noise key %d: public key does not match private key", entry.KeyID))
		}

		ring.keys[entry.KeyID] = flynn.DHKey{
			Private: append([]byte(nil), entry.PrivateKey...),
			Public:  derived,
		}
	}

	_, exists := ring.keys[currentKeyID]
	if !exists {
		return fail(fmt.Errorf("noise current key ID %d is not in keyring", currentKeyID))
	}
	return ring, nil
}

func (k *Keyring) CurrentKeyID() uint32 { return k.currentKeyID }

// Keypair returns owned copies, so a connection cannot clear a shared static
// key accidentally. The caller must clear its private copy after use.
func (k *Keyring) Keypair(id uint32) (flynn.DHKey, bool) {
	pair, ok := k.keys[id]
	if !ok {
		return flynn.DHKey{}, false
	}

	return flynn.DHKey{
		Private: append([]byte(nil), pair.Private...),
		Public:  append([]byte(nil), pair.Public...),
	}, true
}

func (k *Keyring) Close() {
	if k == nil {
		return
	}
	for id, pair := range k.keys {
		clear(pair.Private[:cap(pair.Private)])
		delete(k.keys, id)
	}
	k.currentKeyID = 0
}

func decodeKeyFile(data []byte, out *KeyFile) error {
	// A single JSON value, no unknown fields. Base64 is decoded into []byte and
	// checked explicitly, rather than copied into an array with silent truncation.
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	err := decoder.Decode(out)
	if err != nil {
		return err
	}

	err = decoder.Decode(new(any))
	if err != io.EOF {
		return errTrailingJSON
	}
	return nil
}
