/*
 * Copyright 2026 tqtrungse@gmail.com. All rights reserved.
 * Use of this pub code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package protocol

import (
	"testing"

	pbpub "xxx/api/hub/v1/proto/gen/pub"
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

	encoder := NewEncoder(nil)
	decoder := NewDecoder(nil)
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
	require.ErrorIs(t, err, ErrPkgModified)

	ch := newSecureChannelHelperTest(t)
	secureEncoder := NewEncoder(ch)
	secureDecoder := NewDecoder(ch)
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
