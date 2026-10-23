/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package rejection

type Reason uint8

// Append only; do not rename, and do not reuse old values:
// 'name' is the metric label; renaming it will break dashboards and alerts.
const (
	// activate_conn
	ActivateDecode Reason = iota
	ActivateValidate
	ActivateTicket
	ProofDecode
	ProofInvalid
	ProofExpired

	// adapter/tcp
	FrameBadType
	FrameTooSmall
	FrameTooLarge
	ConnectionClosed
	ActivationState
	HandshakeInvalid
	HandshakeKey
	RecordDecrypt
	PayloadDecode
	PayloadValidate
	HandshakeExpired

	numReasons
)

var names = [numReasons]string{
	ActivateDecode:   "hub_activate_decode",
	ActivateValidate: "hub_activate_validate",
	ActivateTicket:   "hub_activate_ticket",
	ProofDecode:      "hub_proof_decode",
	ProofInvalid:     "hub_proof_invalid",
	ProofExpired:     "hub_proof_expired",
	FrameBadType:     "hub_frame_bad_type",
	FrameTooSmall:    "hub_frame_too_small",
	FrameTooLarge:    "hub_frame_too_large",
	ConnectionClosed: "hub_connection_closed",
	ActivationState:  "hub_activation_state",
	HandshakeInvalid: "hub_handshake_invalid",
	HandshakeKey:     "hub_handshake_key",
	RecordDecrypt:    "hub_record_decrypt",
	PayloadDecode:    "hub_payload_decode",
	PayloadValidate:  "hub_payload_validate",
	HandshakeExpired: "hub_handshake_expired",
}

// Names returns the stable labels indexed by Reason.
func Names() []string {
	return names[:]
}
