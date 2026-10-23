/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package integration_test

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	pbpub "xxx/api/hub/v2/proto/gen/pub"
	"xxx/internal/hub/adapter/tcp"
	"xxx/internal/hub/core/protocol"
	"xxx/internal/hub/infrastructure/noise/noisetest"
	"xxx/internal/shared/ticket/tickettest"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func readNetworkFrame(t *testing.T, c net.Conn) []byte {
	t.Helper()

	header := make([]byte, 2)
	_, err := io.ReadFull(c, header)
	require.NoError(t, err)

	body := make([]byte, int(binary.LittleEndian.Uint16(header)))
	_, err = io.ReadFull(c, body)
	require.NoError(t, err)

	return append(header, body...)
}

func TestHandshakeOverRealSocket(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	server, _, verifier, _ := serverFixture(t, tcp.Config{Addr: "tcp://127.0.0.1:0"}, ctrl)
	done := make(chan struct{})
	var runErr error
	go func() {
		runErr = server.Run()
		close(done)
	}()

	// Register cleanup before waiting for readiness, including startup failure.
	t.Cleanup(func() {
		select {
		case <-done:
			require.NoError(t, runErr)
			return
		default:
		}

		err := server.Shutdown()
		if err != nil {
			t.Errorf("shutdown: %v", err)
		}

		select {
		case <-done:
			require.NoError(t, runErr)
		case <-time.After(5 * time.Second):
			t.Error("listener did not stop after shutdown")
		}
	})

	select {
	case <-server.Ready():
	case <-done:
		require.Failf(t, "listener stopped before readiness", "runErr: %v", runErr)
	case <-time.After(5 * time.Second):
		require.Fail(t, "listener not ready")
	}

	address := server.Addr()
	require.NotEmpty(t, address)

	_, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	require.NotEqual(t, "0", port)

	conn, err := net.DialTimeout("tcp", address, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(5*time.Second)))

	peer, err := noisetest.New(tickettest.New().HubPublicKey[:], "hub.chat", 7)
	require.NoError(t, err)

	hello, err := peer.Hello(7)
	require.NoError(t, err)
	for _, buf := range hello {
		_, err = conn.Write([]byte{buf})
		require.NoError(t, err)
	}
	require.NoError(t, peer.Finish(readNetworkFrame(t, conn)))

	auth := authFrame(t, peer, verifier)
	_, err = conn.Write(auth)
	require.NoError(t, err)

	plain, err := peer.Decrypt(readNetworkFrame(t, conn))
	require.NoError(t, err)

	ack, err := protocol.NewDecoder().DecodePayload(plain)
	require.NoError(t, err)
	require.Equal(t, pbpub.MsgKind_RESP_AUTHENTICATE, ack.Kind)
	require.Equal(t, pbpub.Code_SUCCESS, ack.Message.(*pbpub.AuthenticateResp).Code)

	// A repeated auth ciphertext is rejected by the same runtime listener.
	_, err = conn.Write(auth)
	require.NoError(t, err)

	var extra [1]byte
	_, err = conn.Read(extra[:])
	require.Error(t, err)

	timeout, ok := errors.AsType[net.Error](err)
	if ok {
		require.False(t, timeout.Timeout(), "replay must close, not merely time out")
	}
}
