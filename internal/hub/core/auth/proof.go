/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

// Package auth defines the device proof bound to a completed Noise channel.
package auth

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"

	slicepool "xxx/pkg/pool/slice"
)

const (
	Domain             = "xxx/hub/device-auth/v2"
	ChallengeSize      = 32
	ChannelBindingSize = sha256.Size
)

var ErrDeviceProofInvalid = errors.New("invalid device proof")

// SigningMessage builds the domain-separated device-proof transcript from the
// channel binding, ticket hash, and server challenge. The caller owns the pooled
// result and must clear its full capacity and return it to slicepool after use.
func SigningMessage(binding, rawTicket, challenge []byte) ([]byte, error) {
	if len(binding) != ChannelBindingSize ||
		len(challenge) != ChallengeSize ||
		len(rawTicket) == 0 {
		return nil, ErrDeviceProofInvalid
	}

	hash := sha256.Sum256(rawTicket)
	out := slicepool.Get(
		4 + len(Domain) +
			4 + len(binding) +
			4 + sha256.Size +
			4 + len(challenge),
	)
	out = out[:0]
	out = binary.LittleEndian.AppendUint32(out, uint32(len(Domain)))
	out = append(out, Domain...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(binding)))
	out = append(out, binding...)
	out = binary.LittleEndian.AppendUint32(out, uint32(sha256.Size))
	out = append(out, hash[:]...)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(challenge)))
	out = append(out, challenge...)
	return out, nil
}
