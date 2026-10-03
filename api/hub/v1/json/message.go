/*
 * Copyright 2026 tqtrungse@gmail.com. All rights reserved.
 * Use of this pub code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package json

type ExchangeKeyReq struct {
	DeviceID      uint64   `json:"device_id"`
	ClientPubKey  [32]byte `json:"client_pub_key"`
	ClientHmacKey [32]byte `json:"client_hmac_key"`
}

type ExchangeKeyResp struct {
	ServerPubKey [32]byte `json:"server_pub_key"`
}
