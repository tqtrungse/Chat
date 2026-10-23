/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package noise

import "errors"

var (
	ErrProtocol               = errors.New("invalid Noise protocol state or message")
	ErrKeyIDRequired          = errors.New("noise key ID must be nonzero")
	ErrKeyringPermissions     = errors.New("noise keyring must be a regular file with owner-only access or optional group read")
	ErrKeyringJSONInvalid     = errors.New("invalid Noise keyring JSON")
	ErrKeyringKeyCountInvalid = errors.New("noise keyring has invalid key count")
	ErrKeyringTooLarge        = errors.New("noise keyring is too large")
	errTrailingJSON           = errors.New("unexpected trailing JSON")
)
