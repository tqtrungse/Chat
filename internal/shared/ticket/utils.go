/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ticket

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

// ParseTicketEncryptKeys parses entries like "1:<base64 32 bytes>".
func ParseTicketEncryptKeys(entries []string) (map[byte][]byte, error) {
	out := make(map[byte][]byte, len(entries))
	fail := func(err error) (map[byte][]byte, error) {
		for _, key := range out {
			clear(key)
		}
		return nil, err
	}

	for _, e := range entries {
		idStr, b64, ok := strings.Cut(strings.TrimSpace(e), ":")
		if !ok {
			return fail(ErrTicketBadEntry)
		}

		id, err := strconv.ParseUint(idStr, 10, 8)
		if err != nil {
			return fail(err)
		}
		if _, exists := out[byte(id)]; exists {
			return fail(fmt.Errorf("duplicate ticket encryption key ID %d", id))
		}

		k, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			clear(k)
			return fail(err)
		}

		out[byte(id)] = k
	}
	return out, nil
}
