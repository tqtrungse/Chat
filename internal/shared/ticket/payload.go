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

import "encoding/binary"

func marshal(b []byte, t *Ticket) {
	binary.LittleEndian.PutUint64(b[0:8], t.DeviceID)
	binary.LittleEndian.PutUint64(b[8:16], uint64(t.ExpiresAt))

	copy(b[16:48], t.Keys.RecvEnc[:])
	copy(b[48:80], t.Keys.SendEnc[:])
	copy(b[80:112], t.Keys.RecvMac[:])
	copy(b[112:144], t.Keys.SendMac[:])
	copy(b[144:176], t.ActivationKey[:])
	copy(b[176:208], t.IdentityPub[:])
}

func unmarshal(b []byte) *Ticket {
	t := &Ticket{
		DeviceID:  binary.LittleEndian.Uint64(b[0:]),
		ExpiresAt: int64(binary.LittleEndian.Uint64(b[8:])),
	}
	copy(t.Keys.RecvEnc[:], b[16:48])
	copy(t.Keys.SendEnc[:], b[48:80])
	copy(t.Keys.RecvMac[:], b[80:112])
	copy(t.Keys.SendMac[:], b[112:144])
	copy(t.ActivationKey[:], b[144:176])
	copy(t.IdentityPub[:], b[176:208])
	return t
}

func aad(deviceID uint64) []byte {
	return binary.LittleEndian.AppendUint64(nil, deviceID)
}
