/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package ticket

import (
	"crypto/ed25519"
	"encoding/binary"

	slicepool "xxx/pkg/pool/slice"

	"xxx/internal/shared/device"
	"xxx/internal/shared/hubendpoint"
)

const (
	MinPayloadSize = 112
	MaxPayloadSize = 239

	// The 111 fixed bytes are:
	// TicketID (16),
	// DeviceID (8),
	// IssuedAt (8),
	// ExpiresAt (8),
	// IdentityPub (32),
	// Audience length (2),
	// HubKeyID (4),
	// HubPublicKey (32),
	// WireVersion (1);
	// Audience adds its variable length.
	fixedPayloadSize = IDSize + 8 + 8 + 8 + ed25519.PublicKeySize + 2 + 4 + hubendpoint.X25519PublicKeySize + 1
)

// MarshalPayload fully validates the claims and encodes the binary ticket payload
// into an owned pooled buffer. The caller must clear it and return it to
// slicepool after use. It does not sign the payload.
func MarshalPayload(info *Info) ([]byte, error) {
	err := info.Validate()
	if err != nil {
		return nil, err
	}

	audienceLen := len(info.Audience)
	buf := slicepool.Get(fixedPayloadSize + audienceLen)
	buf = buf[:74]
	copy(buf, info.TicketID[:])
	binary.LittleEndian.PutUint64(buf[IDSize:], info.DeviceID.Uint64())
	binary.LittleEndian.PutUint64(buf[24:], uint64(info.IssuedAt))
	binary.LittleEndian.PutUint64(buf[32:], uint64(info.ExpiresAt))
	copy(buf[40:], info.IdentityPub[:])
	binary.LittleEndian.PutUint16(buf[72:], uint16(audienceLen))
	buf = append(buf, info.Audience...)
	buf = binary.LittleEndian.AppendUint32(buf, info.HubKeyID)
	buf = append(buf, info.HubPublicKey[:]...)
	buf = append(buf, info.WireVersion)
	return buf, nil
}

// UnmarshalPayload decodes and fully validates the public claims, including both
// public keys. It does not authenticate a signature or check expiry against the clock.
func UnmarshalPayload(buf []byte) (*Info, error) {
	meta, err := DecodePayload(buf)
	if err != nil {
		return nil, err
	}

	err = meta.Validate()
	if err != nil {
		return nil, err
	}
	return meta, nil
}

// DecodePayload checks wire boundaries and decodes claims without retaining the
// input buffer. The result is not validated or authenticated; callers must
// validate it before authorization.
func DecodePayload(buf []byte) (*Info, error) {
	if len(buf) < MinPayloadSize || len(buf) > MaxPayloadSize {
		return nil, ErrTicketInvalid
	}

	n := int(binary.LittleEndian.Uint16(buf[72:]))
	if n < 1 ||
		n > 128 ||
		len(buf) != fixedPayloadSize+n {
		return nil, ErrTicketInvalid
	}

	info := &Info{
		DeviceID:    device.ID(binary.LittleEndian.Uint64(buf[IDSize:])),
		IssuedAt:    int64(binary.LittleEndian.Uint64(buf[24:])),
		ExpiresAt:   int64(binary.LittleEndian.Uint64(buf[32:])),
		Audience:    string(buf[74 : 74+n]),
		HubKeyID:    binary.LittleEndian.Uint32(buf[74+n:]),
		WireVersion: buf[len(buf)-1],
	}
	copy(info.TicketID[:], buf[:IDSize])
	copy(info.IdentityPub[:], buf[40:72])
	copy(info.HubPublicKey[:], buf[78+n:110+n])
	return info, nil
}
