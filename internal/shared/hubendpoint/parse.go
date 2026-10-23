/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package hubendpoint

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
)

const MaxDescriptorBytes = 4096

// Parse decodes and validates the public provisioning format. Errors never
// include JSON field names or values, in case a private keyring is supplied.
func Parse(data []byte) (*Descriptor, error) {
	if len(data) > MaxDescriptorBytes {
		return nil, fmt.Errorf("hub descriptor exceeds %d bytes", MaxDescriptorBytes)
	}

	var descriptor Descriptor
	err := json.Unmarshal(
		data,
		&descriptor,
		json.RejectUnknownMembers(true),
		jsontext.AllowDuplicateNames(false),
	)
	if err != nil {
		return nil, sanitizeDecodeError(err)
	}

	err = descriptor.Validate()
	if err != nil {
		return nil, fmt.Errorf("validate hub descriptor: %w", err)
	}
	return &descriptor, nil
}

// sanitizeDecodeError exposes only the error class and byte offset, hiding
// member names and numeric values that JSON v2 errors may include from the file.
func sanitizeDecodeError(err error) error {
	switch {
	case errors.Is(err, json.ErrUnknownName):
		return ErrUnknownField
	case errors.Is(err, jsontext.ErrDuplicateName):
		return ErrDuplicateField
	}

	semErr, ok := errors.AsType[*json.SemanticError](err)
	if ok {
		return fmt.Errorf(
			"invalid hub descriptor JSON: cannot decode JSON %v at byte %d",
			semErr.JSONKind, semErr.ByteOffset,
		)
	}

	synErr, ok := errors.AsType[*jsontext.SyntacticError](err)
	if ok {
		return fmt.Errorf("invalid hub descriptor JSON: syntax error at byte %d", synErr.ByteOffset)
	}
	return errors.New("invalid hub descriptor JSON")
}
