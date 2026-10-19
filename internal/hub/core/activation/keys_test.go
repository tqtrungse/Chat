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

package activation

import (
	"testing"

	"xxx/internal/hub/core/protocol"
	sharedsession "xxx/internal/shared/session"

	"github.com/stretchr/testify/require"
)

func TestDeriveConnKeys_BindsNonceAndDirection(t *testing.T) {
	var base sharedsession.Keys
	for i := range 32 {
		base.RecvEnc[i] = byte(i + 1)
		base.SendEnc[i] = byte(i + 2)
		base.RecvMac[i] = byte(i + 3)
		base.SendMac[i] = byte(i + 4)
	}
	before := base

	var nonceA, nonceB [32]byte
	for i := range 32 {
		nonceA[i] = byte(i + 5)
		nonceB[i] = byte(i + 6)
	}

	connA, err := DeriveConnKeys(&base, nonceA)
	require.NoError(t, err)
	defer connA.Clear()
	connAAgain, err := DeriveConnKeys(&base, nonceA)
	require.NoError(t, err)
	defer connAAgain.Clear()
	connB, err := DeriveConnKeys(&base, nonceB)
	require.NoError(t, err)
	defer connB.Clear()

	require.Equal(t, connA, connAAgain, "same base keys and nonce must derive identically")
	require.NotEqual(t, connA, connB, "different nonces must derive different connection keys")
	require.NotEqual(t, connA.RecvEnc, connA.SendEnc, "directions must use distinct labels")
	require.NotEqual(t, connA.RecvEnc, connA.RecvMac, "encryption and MAC must use distinct labels")
	require.Equal(t, before, base, "derivation must not mutate ticket keys")
}

func TestDeriveConnKeys_RejectsNilBase(t *testing.T) {
	_, err := DeriveConnKeys(nil, [32]byte{})
	require.ErrorIs(t, err, protocol.ErrConnKeyBaseNil)
}
