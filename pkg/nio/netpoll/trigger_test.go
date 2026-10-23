//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package netpoll

import (
	"testing"
	"time"

	"xxx/pkg/log"
	errorx "xxx/pkg/nio/error"
	"xxx/pkg/nio/queue"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestTriggerRetriesAfterWakeupFailure(t *testing.T) {
	logger, closeLogger, err := log.NewLogger(nil)
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, closeLogger()) })

	p, err := OpenPoller(logger)
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, p.Close()) })
	calls := make(chan int, 2)

	// Trigger enqueues before waking. Force a wakeup error without starting
	// Polling, then restore its fd and ensure a later Trigger wakes both tasks.
	restore := breakWakeupFD(p)
	err = p.Trigger(queue.LowPriority, func(any) error { calls <- 1; return nil }, nil)
	restore()
	require.ErrorIs(t, err, unix.EBADF)
	require.Zero(t, p.wakeupCall.Load(), "a failed wakeup must allow a subsequent retry")

	done := make(chan error, 1)
	go func() { done <- p.Polling() }()

	t.Cleanup(func() {
		require.NoError(
			t,
			p.Trigger(
				queue.HighPriority, func(any) error {
					return errorx.ErrEngineShutdown
				},
				nil,
			),
		)
		select {
		case err = <-done:
			require.ErrorIs(t, err, errorx.ErrEngineShutdown)

		case <-time.After(3 * time.Second):
			t.Error("poller did not stop")
		}
	})

	require.NoError(
		t,
		p.Trigger(
			queue.LowPriority, func(any) error { calls <- 2; return nil },
			nil,
		),
	)

	for _, want := range []int{1, 2} {
		select {
		case got := <-calls:
			require.Equal(t, want, got)

		case <-time.After(3 * time.Second):
			t.Fatal("a queued task was not executed after wakeup recovered")
		}
	}
}
