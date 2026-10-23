/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package authorization

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"xxx/pkg/pool/worker"

	"github.com/stretchr/testify/require"
)

func TestExecutorBoundsRunningAndQueuedJobs(t *testing.T) {
	executor, stop := New(1, 2)
	unblock := make(chan struct{})
	started := make(chan struct{})
	finished := make(chan struct{})
	// Unblock before draining even if an assertion fails.
	t.Cleanup(func() {
		stop()

		select {
		case <-finished:
		default:
			t.Error("shutdown did not drain the queued authorization job")
		}
	})
	t.Cleanup(func() { close(unblock) })
	require.NoError(t, executor.Submit(func() { close(started); <-unblock }))

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	require.NoError(t, executor.Submit(func() { close(finished) }))
	require.ErrorIs(t, executor.Submit(func() { t.Error("overloaded job ran") }), ErrOverloaded)
	// Both running and queued work occupy slots until the jobs finish.
	require.EqualValues(t, 2, executor.inflight.Load())
}

func TestExecutorReleasesSlotsAndRejectsShutdown(t *testing.T) {
	executor, stop := New(1, 1)
	t.Cleanup(stop)
	done := make(chan struct{})
	require.NoError(t, executor.Submit(func() { close(done) }))

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not execute")
	}

	stop()
	require.Zero(t, executor.inflight.Load())
	require.ErrorIs(t, executor.Submit(func() {}), worker.ErrPoolClosed)
	require.Zero(t, executor.inflight.Load())
	require.ErrorIs(t, executor.Submit(nil), worker.ErrTaskNil)
	require.Zero(t, executor.inflight.Load())
	stop()
}

func TestExecutorConcurrentAdmissionRespectsLimit(t *testing.T) {
	const (
		submitters = 64
	)
	for _, tc := range []struct {
		name    string
		maxJobs int
	}{
		{"more submitters than slots", 7},
		{"room for every submitter", 128},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				var (
					executor, stop = New(2, tc.maxJobs)
					unblock        = make(chan struct{})
					executed       atomic.Int64
					accepted       int
				)

				t.Cleanup(func() {
					stop()
					require.Zero(t, executor.inflight.Load())
					require.EqualValues(t, accepted, executed.Load(), "rejected jobs must not execute")
				})
				t.Cleanup(func() { close(unblock) })

				var (
					job = func() {
						executed.Add(1)
						<-unblock
					}
					start   = make(chan struct{})
					results = make(chan error, submitters)
				)

				for range submitters {
					go func() {
						<-start
						results <- executor.Submit(job)
					}()
				}

				close(start)
				deadline := time.NewTimer(time.Second)
				defer deadline.Stop()

				for range submitters {
					select {
					case err := <-results:
						if err == nil {
							accepted++
						} else {
							require.ErrorIs(t, err, ErrOverloaded)
						}
					case <-deadline.C:
						t.Fatal("submission did not return while jobs were blocked")
					}
				}
				// Bounded CAS retries may reject contention before capacity is reached.
				// Every successful submission must own exactly one reservation.
				require.Positive(t, accepted)
				require.LessOrEqual(t, accepted, min(tc.maxJobs, submitters))
				require.EqualValues(t, accepted, executor.inflight.Load())

				// Once contention ends, all remaining capacity must still be usable.
				for accepted < tc.maxJobs {
					require.NoError(t, executor.Submit(job))
					accepted++
				}
				require.ErrorIs(t, executor.Submit(job), ErrOverloaded)
				require.EqualValues(t, tc.maxJobs, executor.inflight.Load())
			},
		)
	}
}

func TestExecutorReusesReservationsAfterConcurrentJobs(t *testing.T) {
	const submitters = 64

	var (
		executed       atomic.Int64
		accepted       int64
		executor, stop = New(2, 8)
	)

	t.Cleanup(stop)

	for range 3 {
		start := make(chan struct{})
		results := make(chan error, submitters)

		for range submitters {
			go func() {
				<-start
				results <- executor.Submit(func() { executed.Add(1) })
			}()
		}

		close(start)
		deadline := time.NewTimer(time.Second)

		for range submitters {
			select {
			case err := <-results:
				if err == nil {
					accepted++
				} else {
					require.ErrorIs(t, err, ErrOverloaded)
				}
			case <-deadline.C:
				t.Fatal("concurrent submissions did not finish")
			}
		}

		deadline.Stop()
		drained := make(chan struct{})
		go func() {
			executor.pool.Wait()
			close(drained)
		}()

		select {
		case <-drained:
		case <-time.After(time.Second):
			t.Fatal("accepted jobs did not finish")
		}
		require.Zero(t, executor.inflight.Load())
		require.Equal(t, accepted, executed.Load())
	}
}

func TestExecutorConcurrentSubmitAndShutdownReleaseReservations(t *testing.T) {
	const submitters = 64

	var (
		executor, stop     = New(2, 8)
		start              = make(chan struct{})
		accepted, executed atomic.Int64
		submittersDone     sync.WaitGroup
		results            = make(chan error, submitters)
	)

	t.Cleanup(stop)
	for range submitters {
		submittersDone.Go(func() {
			<-start
			err := executor.Submit(func() { executed.Add(1) })
			if err == nil {
				accepted.Add(1)
			}
			results <- err
		})
	}

	shutdownDone := make(chan struct{})
	go func() {
		<-start
		stop()
		close(shutdownDone)
	}()

	close(start)
	done := make(chan struct{})

	go func() {
		submittersDone.Wait()
		<-shutdownDone
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("concurrent submission and shutdown did not finish")
	}

	close(results)
	for err := range results {
		if err != nil && !errors.Is(err, ErrOverloaded) {
			require.ErrorIs(t, err, worker.ErrPoolClosed)
		}
	}
	require.Equal(t, accepted.Load(), executed.Load())
	require.Zero(t, executor.inflight.Load())
}
