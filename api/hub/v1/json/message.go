/*
 * Copyright 2026 tqtrungse@gmail.com. All rights reserved.
 * Use of this pub code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package json

type ExchangeKeyReq struct {
	DeviceID uint64 `json:"device_id"`

	// If the Base64 string is valid and decodes to exactly 32 bytes, it fits perfectly into a `[32]byte` array without
	// error.
	//
	// The issue arises when the Base64 string is valid but decodes to fewer or more than 32 bytes; the current decoder
	// uses a copy operation that does not signal an error—it simply pads with zeros or truncates the excess.
	// Changing the request field to `[]byte` allows for a length check (`len == 32`) before converting it to the array.
	ClientPubKey []byte `json:"client_pub_key"`
}

type ExchangeKeyResp struct {
	ServerPubKey [32]byte `json:"server_pub_key"`
}
