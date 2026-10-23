/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

// Package hubendpoint defines the public service identity used by Noise NK.
package hubendpoint

import (
	"crypto/ecdh"
	"encoding/binary"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"

	slicepool "xxx/pkg/pool/slice"
)

const (
	Protocol            = "Noise_NK_25519_ChaChaPoly_SHA256"
	WireVersion         = uint8(2)
	PrologueDomain      = "xxx/hub/noise/v2"
	MaxAudienceBytes    = 128
	MaxAddressBytes     = 255
	X25519PublicKeySize = 32
)

var hostnameLabelPattern = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?$`)

// Descriptor contains public data only. Address is a client-reachable TCP
// endpoint of the service, not necessarily the address of an individual pod.
// PublicKey uses a slice so encoding/json decodes Base64 without padding or
// truncating malformed keys into a fixed-size array.
type Descriptor struct {
	Address   string `json:"address"`
	Audience  string `json:"audience"`
	KeyID     uint32 `json:"key_id"`
	PublicKey []byte `json:"public_key"`
	Protocol  string `json:"protocol"`
	Version   uint8  `json:"version"`
}

// ValidateAudience requires a nonempty, bounded audience made of printable ASCII without spaces.
func ValidateAudience(audience string) error {
	if len(audience) == 0 || len(audience) > MaxAudienceBytes {
		return fmt.Errorf("hub audience must contain 1..%d bytes", MaxAudienceBytes)
	}

	for i := range len(audience) {
		if audience[i] < 0x21 || audience[i] > 0x7e {
			return fmt.Errorf("hub audience must use printable ASCII without spaces")
		}
	}
	return nil
}

// Validate checks the endpoint address, protocol, audience, and key ID, then
// rejects malformed or low-order X25519 public keys with a fixed ECDH probe.
func (d Descriptor) Validate() error {
	if d.Protocol != Protocol || d.Version != WireVersion {
		return fmt.Errorf("unsupported hub Noise protocol or wire version")
	}

	if d.KeyID == 0 {
		return fmt.Errorf("hub key ID must be nonzero")
	}

	err := ValidateAudience(d.Audience)
	if err != nil {
		return err
	}

	err = validateAddress(d.Address)
	if err != nil {
		return err
	}

	pub, err := ecdh.X25519().NewPublicKey(d.PublicKey)
	if err != nil {
		return fmt.Errorf("hub public key must contain %d bytes", X25519PublicKeySize)
	}

	// A fixed, non-secret probe rejects low-order public keys at configuration
	// time. The handshake library still validates the actual ECDH operation.
	probe, _ := ecdh.X25519().NewPrivateKey(make([]byte, 32))
	_, err = probe.ECDH(pub)
	if err != nil {
		return fmt.Errorf("invalid low-order hub public key")
	}
	return nil
}

// Prologue encodes the authenticated negotiation context defined in
// docs/protocol/noise-v2.md after validating the descriptor. Both peers must
// supply these exact bytes to Noise. The caller owns the pooled result and must
// clear it and return it to slicepool after use.
func (d Descriptor) Prologue() ([]byte, error) {
	err := d.Validate()
	if err != nil {
		return nil, err
	}
	return Prologue(d.Audience, d.KeyID)
}

// Prologue builds handshake context from the server's trusted local identity.
// A responder does not need the public endpoint address. The caller must clear
// the returned pooled buffer and return it to slicepool after use.
func Prologue(audience string, keyID uint32) ([]byte, error) {
	err := ValidateAudience(audience)
	if err != nil {
		return nil, err
	}

	if keyID == 0 {
		return nil, fmt.Errorf("hub key ID must be nonzero")
	}

	out := slicepool.Get(
		4 + len(PrologueDomain) +
			1 +
			4 + len(Protocol) +
			4 + len(audience) +
			4,
	)
	out = out[:0]
	out = appendField(out, PrologueDomain)
	out = append(out, WireVersion)
	out = appendField(out, Protocol)
	out = appendField(out, audience)
	out = binary.LittleEndian.AppendUint32(out, keyID)
	return out, nil
}

// appendField appends a little-endian uint32 byte length followed by the string bytes.
func appendField(out []byte, value string) []byte {
	out = binary.LittleEndian.AppendUint32(out, uint32(len(value)))
	return append(out, value...)
}

// validateAddress checks a bounded host:port endpoint with a nonzero numeric port
// and an IP address or valid hostname. IPv6 addresses must be bracketed.
func validateAddress(address string) error {
	if len(address) == 0 || len(address) > MaxAddressBytes {
		return fmt.Errorf("hub address must contain 1..%d bytes", MaxAddressBytes)
	}

	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" {
		return fmt.Errorf("hub address must be host:port (IPv6 requires brackets)")
	}

	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 {
		return fmt.Errorf("hub address port must be in 1..65535")
	}
	if net.ParseIP(host) != nil {
		return nil
	}

	for label := range strings.SplitSeq(strings.TrimSuffix(host, "."), ".") {
		if !hostnameLabelPattern.MatchString(label) {
			return fmt.Errorf("invalid hub address hostname")
		}
	}
	return nil
}
