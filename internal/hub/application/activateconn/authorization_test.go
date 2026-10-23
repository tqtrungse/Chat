/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package activateconn

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"xxx/internal/hub/core/transport"
	"xxx/internal/shared/ticket"
	"xxx/internal/shared/ticket/tickettest"

	"xxx/pkg/nio"
	niotest "xxx/pkg/nio/testutil"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestAuthorizationRejectsOverloadAndEarlyFrame(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, overload := range []bool{false, true} {
		t.Run(
			map[bool]string{
				true:  "overload",
				false: "early frame",
			}[overload],
			func(t *testing.T) {
				envi := setup(t, ctrl, Config{})
				conn := envi.hello(t, ctrl)
				frame, _ := envi.auth(t)
				if overload {
					envi.jobs.err = errors.New("full")
				}

				action := envi.ca.HandleFrame(t.Context(), conn, frame)
				if overload {
					require.Equal(t, nio.Close, action)
					require.Empty(t, envi.jobs.jobs)
				} else {
					require.Equal(t, nio.None, action)
					require.Equal(t, stageVerifying, conn.Context().(*pendingActivation).stage)
					require.Equal(t, nio.Close, envi.ca.HandleFrame(t.Context(), conn, []byte("early record")))
					// No verification or decryption is allowed after the early frame closes it.
					envi.finishAuthorization(conn)
				}
				require.Zero(t, envi.ca.PreAuthConns())
			},
		)
	}
}

func TestAuthorizationDropsStaleAndExpiredResults(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	for _, kind := range []string{
		"released",
		"context changed",
		"stage changed",
		"auth expired",
		"total expired",
		"enqueue failed",
		"ticket expired",
	} {
		t.Run(
			kind,
			func(t *testing.T) {
				envi := setup(t, ctrl, Config{})
				conn := envi.hello(t, ctrl)
				pa := conn.Context().(*pendingActivation)
				frame, req := envi.auth(t)

				envi.expectAuthorization(conn, req).Return(tickettest.New(), nil)
				require.Equal(t, nio.None, envi.ca.HandleFrame(t.Context(), conn, frame))
				// Finish CPU work but leave its completion queued on the owner loop.

				job := envi.jobs.jobs[0]
				envi.jobs.jobs = nil
				if kind == "enqueue failed" {
					conn.EventLoop().(*niotest.Loop).Err = errors.New("stopped")
					conn.EXPECT().Close().Return(nil)
				}
				job()

				switch kind {
				case "released":
					pa.clear()

				case "context changed":
					conn.SetContext(nil)

				case "stage changed":
					pa.stage = stageRegistering

				case "auth expired":
					pa.phaseDeadline = time.Now().Add(-time.Second)
					envi.manager.
						EXPECT().
						ForceClose(conn)

				case "ticket expired":
					envi.service.
						EXPECT().
						CheckExpiry(gomock.Any(), gomock.Any()).
						Return(errors.New("expired"))

					envi.manager.
						EXPECT().
						ForceClose(conn)

				case "total expired":
					pa.deadline = time.Now().Add(-time.Second)
					envi.manager.
						EXPECT().
						ForceClose(conn)
				}
				conn.EventLoop().(*niotest.Loop).Drain()
				// No ActivateConn call is expected for any stale result.
			},
		)
	}
}

func TestAuthorizationCopiesInputsBeforeReturningToLoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	var (
		envi              = setup(t, ctrl, Config{})
		conn              = envi.hello(t, ctrl)
		pa                = conn.Context().(*pendingActivation)
		frame, req        = envi.auth(t)
		expectedBinding   = bytes.Clone(pa.binding)
		expectedChallenge = bytes.Clone(pa.challenge)
		entered           = make(chan struct{})
		resume            = make(chan struct{})
		done              = make(chan struct{})
		raw               []byte
		signature         []byte
		binding           []byte
		challenge         []byte
	)

	// Ensure the worker exits before fixture cleanup even on assertion failure.
	t.Cleanup(func() {
		close(resume)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("worker did not finish")
		}

		conn.EventLoop().(*niotest.Loop).Drain()
		for _, buf := range [][]byte{raw, signature, binding, challenge} {
			require.Equal(t, make([]byte, cap(buf)), buf[:cap(buf)])
		}
	})

	envi.expectAuthorization(conn, req).DoAndReturn(func(r, s, b, c []byte, _ transport.Identity) (*ticket.Info, error) {
		raw, signature, binding, challenge = r, s, b, c
		close(entered)
		<-resume
		require.Equal(t, req.Ticket, r)
		require.Equal(t, req.Signature, s)
		require.Equal(t, expectedBinding, b)
		require.Equal(t, expectedChallenge, c)
		return tickettest.New(), nil
	})
	require.Equal(t, nio.None, envi.ca.HandleFrame(t.Context(), conn, frame))
	job := envi.jobs.jobs[0]
	envi.jobs.jobs = nil
	go func() { defer close(done); job() }()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("verification did not start")
	}

	// Worker is blocked while the owner loop destroys original buffers.
	envi.manager.EXPECT().RemoveConn(gomock.Any(), conn)
	envi.ca.Deactivate(t.Context(), conn)
	require.True(t, pa.released.Load())
}

func TestAuthorizationSkipsCancelledQueuedJob(t *testing.T) {
	ctrl := gomock.NewController(t)
	envi := setup(t, ctrl, Config{})
	conn := envi.hello(t, ctrl)
	frame, _ := envi.auth(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	require.Equal(t, nio.None, envi.ca.HandleFrame(ctx, conn, frame))
	cancel()
	envi.manager.EXPECT().ForceClose(conn)
	// No VerifyAuthorization call: canceled work must not consume crypto CPU.
	envi.finishAuthorization(conn)
	require.Zero(t, envi.ca.PreAuthConns())
}
