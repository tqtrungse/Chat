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

	pbpub "xxx/api/hub/v1/proto/gen/pub"
	"xxx/internal/hub/core/protocol"
	"xxx/internal/hub/core/protocol/protocoltest"
	sharedsession "xxx/internal/shared/session"

	slicepool "xxx/pkg/pool/slice"

	"github.com/stretchr/testify/require"
)

func TestDeriveConnKeys_RejectsFrameReplayAcrossConnections(t *testing.T) {
	var ticketKeys sharedsession.Keys
	for i := range 32 {
		ticketKeys.RecvEnc[i] = byte(i + 1)
		ticketKeys.SendEnc[i] = byte(i + 2)
		ticketKeys.RecvMac[i] = byte(i + 3)
		ticketKeys.SendMac[i] = byte(i + 4)
	}
	defer ticketKeys.Clear()

	var nonceA, nonceB [32]byte
	for i := range 32 {
		nonceA[i] = byte(i + 5)
		nonceB[i] = byte(i + 6)
	}
	keysA, err := DeriveConnKeys(&ticketKeys, nonceA)
	require.NoError(t, err)
	defer keysA.Clear()
	keysB, err := DeriveConnKeys(&ticketKeys, nonceB)
	require.NoError(t, err)
	defer keysB.Clear()

	encoder := protocol.NewEncoder(nil)
	decoder := protocol.NewDecoder(nil)
	frame, err := encoder.Encode(
		&keysA.RecvMac,
		0,
		pbpub.MsgType_REQ_DIRECT_FORWARD_MSG,
		new(pbpub.DirectForwardMsgReq),
		nil,
	)
	require.NoError(t, err)
	defer func() {
		clear(frame[:cap(frame)])
		slicepool.Put(frame)
	}()

	_, _, _, err = decoder.Decode(&keysB.RecvMac, 0, frame)
	require.ErrorIs(t, err, protocol.ErrPkgModified)

	ch := protocoltest.NewSecureChannel(t)
	secureEncoder := protocol.NewEncoder(ch)
	secureDecoder := protocol.NewDecoder(ch)
	secureFrame, err := secureEncoder.EncodeS(
		&keysA.RecvEnc,
		0,
		pbpub.MsgType_REQ_SEND_MSG,
		new(pbpub.SendMsgReq),
		nil,
	)
	require.NoError(t, err)
	defer func() {
		clear(secureFrame[:cap(secureFrame)])
		slicepool.Put(secureFrame)
	}()

	_, _, _, err = secureDecoder.DecodeS(&keysB.RecvEnc, 0, secureFrame)
	require.Error(t, err)
}
