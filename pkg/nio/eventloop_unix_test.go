//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package nio

import (
	"context"
	"net"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"xxx/pkg/log"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// gcLoadBalancer decorates only the GC fixture's load balancer. Ordinary tests
// and production keep the same promoted baseLoadBalancer.register method.
type gcLoadBalancer struct {
	loadBalancer
	initConnCount int32
	loops         int
	ready         chan struct{}
}

func (lb *gcLoadBalancer) register(el *eventloop) {
	lb.loadBalancer.register(el)
	for i := range int(lb.initConnCount) {
		c := newStreamConn("tcp", i, el, &unix.SockaddrInet4{}, &net.TCPAddr{}, &net.TCPAddr{})
		el.connections.addConn(c)
	}
	if lb.len() == lb.loops {
		close(lb.ready)
	}
}

var testBigGC = false

func BenchmarkGC4El100k(b *testing.B) {
	oldGc := debug.SetGCPercent(-1)
	b.Cleanup(func() { debug.SetGCPercent(oldGc) })

	benchServeGC(b, "tcp", ":0", true, 4, 100000)
	b.Run("Run-4-eventloop-100000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			runtime.GC()
		}
	})
}

func BenchmarkGC4El200k(b *testing.B) {
	oldGc := debug.SetGCPercent(-1)
	b.Cleanup(func() { debug.SetGCPercent(oldGc) })

	benchServeGC(b, "tcp", ":0", true, 4, 200000)
	b.Run("Run-4-eventloop-200000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			runtime.GC()
		}
	})
}

func BenchmarkGC4El500k(b *testing.B) {
	oldGc := debug.SetGCPercent(-1)
	b.Cleanup(func() { debug.SetGCPercent(oldGc) })

	benchServeGC(b, "tcp", ":0", true, 4, 500000)
	b.Run("Run-4-eventloop-500000", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			runtime.GC()
		}
	})
}

func benchServeGC(b *testing.B, network, addr string, async bool, elNum int, initConnCount int32) *benchmarkServerGC {
	b.Helper()

	var runErr error
	logger, closeLogger, err := log.NewLogger(nil)
	require.NoError(b, err)
	done := make(chan struct{})
	ts := &benchmarkServerGC{
		elNum:         elNum,
		initConnCount: initConnCount,
		initOk:        make(chan struct{}),
		booted:        make(chan struct{}),
	}

	go func() {
		runErr = Run(
			ts,
			network+"://"+addr,
			WithLockOSThread(async),
			WithNumEventLoop(elNum),
			WithTCPKeepAlive(time.Minute),
			WithTCPNoDelay(TCPDelay),
			WithLogger(logger))
		close(done)
	}()

	b.Cleanup(func() {
		select {
		case <-done:
		default:
			select {
			case <-ts.booted:
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := ts.eng.Stop(ctx); err != nil {
					b.Errorf("stop GC benchmark: %v", err)
				}
				cancel()
			default:
			}
		}

		select {
		case <-done:
			assert.NoError(b, runErr)
			assert.NoError(b, closeLogger())

		case <-time.After(5 * time.Second):
			b.Error("GC benchmark Run did not finish; logger remains owned by Run")
		}
	})

	select {
	case <-ts.initOk:
	case <-done:
		b.Fatalf("GC benchmark stopped before initialization: %v", runErr)

	case <-time.After(10 * time.Second):
		b.Fatal("GC benchmark did not initialize")
	}
	return ts
}

type benchmarkServerGC struct {
	*BuiltinEventEngine
	eng           Engine
	elNum         int
	initConnCount int32
	initOk        chan struct{}
	booted        chan struct{}
}

func (s *benchmarkServerGC) OnBoot(eng Engine) Action {
	s.eng = eng
	eng.eng.eventLoops = &gcLoadBalancer{
		loadBalancer:  eng.eng.eventLoops,
		initConnCount: s.initConnCount,
		loops:         s.elNum,
		ready:         s.initOk,
	}
	close(s.booted)
	return None
}

// TestServeGC generates fake connections asynchronously.
// The large cases are skipped unless testBigGC is set to true.
func TestServeGC(t *testing.T) {
	t.Run(
		"gc-loop",
		func(t *testing.T) {
			t.Run(
				"1-loop-10000",
				func(t *testing.T) {
					if testBigGC {
						t.Skipf("Skip when testBigGC=%t", testBigGC)
					}
					testServeGC(t, "tcp", ":0", true, true, 1, 10000)
				},
			)
			t.Run(
				"1-loop-100000",
				func(t *testing.T) {
					if !testBigGC {
						t.Skipf("Skip when testBigGC=%t", testBigGC)
					}
					testServeGC(t, "tcp", ":0", true, true, 1, 100000)
				},
			)
			t.Run(
				"1-loop-1000000",
				func(t *testing.T) {
					if !testBigGC {
						t.Skipf("Skip when testBigGC=%t", testBigGC)
					}
					testServeGC(t, "tcp", ":0", true, true, 1, 1000000)
				},
			)
			t.Run(
				"2-loop-10000",
				func(t *testing.T) {
					if testBigGC {
						t.Skipf("Skip when testBigGC=%t", testBigGC)
					}
					testServeGC(t, "tcp", ":0", true, true, 2, 10000)
				},
			)
			t.Run(
				"2-loop-100000",
				func(t *testing.T) {
					if !testBigGC {
						t.Skipf("Skip when testBigGC=%t", testBigGC)
					}
					testServeGC(t, "tcp", ":0", true, true, 2, 100000)
				},
			)
			t.Run(
				"2-loop-1000000",
				func(t *testing.T) {
					if !testBigGC {
						t.Skipf("Skip when testBigGC=%t", testBigGC)
					}
					testServeGC(t, "tcp", ":0", true, true, 2, 1000000)
				},
			)
			t.Run(
				"4-loop-10000",
				func(t *testing.T) {
					if testBigGC {
						t.Skipf("Skip when testBigGC=%t", testBigGC)
					}
					testServeGC(t, "tcp", ":0", true, true, 4, 10000)
				},
			)
			t.Run(
				"4-loop-100000",
				func(t *testing.T) {
					if !testBigGC {
						t.Skipf("Skip when testBigGC=%t", testBigGC)
					}
					testServeGC(t, "tcp", ":0", true, true, 4, 100000)
				},
			)
			t.Run(
				"4-loop-1000000",
				func(t *testing.T) {
					if !testBigGC {
						t.Skipf("Skip when testBigGC=%t", testBigGC)
					}
					testServeGC(t, "tcp", ":0", true, true, 4, 1000000)
				},
			)
			t.Run(
				"16-loop-10000",
				func(t *testing.T) {
					if testBigGC {
						t.Skipf("Skip when testBigGC=%t", testBigGC)
					}
					testServeGC(t, "tcp", ":0", true, true, 16, 10000)
				},
			)
			t.Run(
				"16-loop-100000",
				func(t *testing.T) {
					if !testBigGC {
						t.Skipf("Skip when testBigGC=%t", testBigGC)
					}
					testServeGC(t, "tcp", ":0", true, true, 16, 100000)
				},
			)
			t.Run(
				"16-loop-1000000",
				func(t *testing.T) {
					if !testBigGC {
						t.Skipf("Skip when testBigGC=%t", testBigGC)
					}
					testServeGC(t, "tcp", ":0", true, true, 16, 1000000)
				},
			)
		},
	)
}

func testServeGC(
	t *testing.T,
	network string,
	addr string,
	multicore bool,
	async bool,
	elNum int,
	initConnCount int32,
) {
	ts := &testServerGC{
		tester:        t,
		network:       network,
		addr:          addr,
		multicore:     multicore,
		async:         async,
		elNum:         elNum,
		initConnCount: initConnCount,
		ready:         make(chan struct{}),
		done:          make(chan struct{}),
		gcDone:        make(chan struct{}),
	}
	logger, closeLogger, err := log.NewLogger(nil)
	require.NoError(t, err)
	defer func() { _ = closeLogger() }()

	err = Run(
		ts,
		network+"://"+addr,
		WithLockOSThread(async),
		WithNumEventLoop(elNum),
		WithTCPKeepAlive(time.Minute),
		WithTCPNoDelay(TCPDelay),
		WithLogger(logger),
	)
	close(ts.done)

	if ts.eng.eng == nil {
		close(ts.gcDone)
	}
	assert.NoError(t, err)

	select {
	case <-ts.gcDone:
	case <-time.After(5 * time.Second):
		t.Fatal("GC fixture goroutine did not finish")
	}
}

type testServerGC struct {
	*BuiltinEventEngine
	tester        *testing.T
	eng           Engine
	network       string
	addr          string
	multicore     bool
	async         bool
	elNum         int
	initConnCount int32
	ready         chan struct{}
	done          chan struct{}
	gcDone        chan struct{}
}

func (s *testServerGC) OnBoot(eng Engine) (action Action) {
	s.eng = eng
	gcSecs := 5
	eng.eng.eventLoops = &gcLoadBalancer{
		loadBalancer:  eng.eng.eventLoops,
		initConnCount: s.initConnCount,
		loops:         s.elNum,
		ready:         s.ready,
	}
	if testBigGC {
		gcSecs = 10
	}
	go func() {
		defer close(s.gcDone)
		select {
		case <-s.ready:
			s.GC(gcSecs)
		case <-s.done:
		}
	}()

	return
}

func (s *testServerGC) GC(secs int) {
	defer func() {
		select {
		case <-s.done:
			// Run may fail during startup without marking the engine shutdown.
		default:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			assert.NoError(s.tester, s.eng.Stop(ctx))
		}
		runtime.GC()
	}()

	var gcAllTime, gcAllCount time.Duration
	gcStart := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
		}

		gcAllCount++
		now := time.Now()
		runtime.GC()
		gcTime := time.Since(now)
		gcAllTime += gcTime
		s.tester.Log(
			s.tester.Name(),
			s.network,
			"server gc:",
			gcTime,
			"average gc time:",
			gcAllTime/gcAllCount,
		)
		if time.Since(gcStart) >= time.Second*time.Duration(secs) {
			break
		}
	}
}

func TestGCLoadBalancerIsScopedToFixture(t *testing.T) {
	for _, tc := range []struct {
		name string
		lb   loadBalancer
	}{
		{"round robin", new(roundRobinLoadBalancer)},
		{"least connections", new(leastConnectionsLoadBalancer)},
		{"source address hash", new(sourceAddrHashLoadBalancer)},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				ready := make(chan struct{})
				fixture := &gcLoadBalancer{
					loadBalancer:  tc.lb,
					initConnCount: 3,
					loops:         2,
					ready:         ready,
				}

				for range 2 {
					el := &eventloop{engine: &engine{opts: &Options{}}}
					el.connections.init(0)
					fixture.register(el)
					require.EqualValues(t, 3, el.countConn())
				}

				select {
				case <-ready:
				default:
					t.Fatal("fixture did not report initialization")
				}

				el := new(eventloop)
				el.connections.init(0)
				tc.lb.register(el)
				require.Zero(t, el.countConn(), "ordinary registration must not inherit GC fixture setup")
			},
		)
	}
}
