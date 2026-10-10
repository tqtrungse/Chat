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

package session

// Ticket is the state that used to live in a pending session. It is sealed
// (AES-GCM) with a cluster-wide key, so any hub can open it.
type Ticket struct {
	DeviceID      uint64
	ExpiresAt     int64
	Keys          Keys
	ActivationKey [32]byte
	IdentityPub   [32]byte
}

// Clear erases ticket key material after the ticket is consumed or rejected.
func (t *Ticket) Clear() {
	if t == nil {
		return
	}
	clear(t.Keys.RecvEnc[:])
	clear(t.Keys.SendEnc[:])
	clear(t.Keys.RecvMac[:])
	clear(t.Keys.SendMac[:])
	clear(t.ActivationKey[:])
	clear(t.IdentityPub[:])
}
