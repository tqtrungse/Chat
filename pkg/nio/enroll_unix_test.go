//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

package nio

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"xxx/pkg/collection/swiss"
	"xxx/pkg/hash"
	"xxx/pkg/log"
	errorx "xxx/pkg/nio/error"
	"xxx/pkg/nio/netpoll"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"golang.org/x/sys/unix"
)

const enrollTestTimeout = 3 * time.Second

func TestEnrollDefaultConcurrencyLimit(t *testing.T) {
	f := newEnrollmentFixture(t, &enrollmentHandler{}, nil)
	slots := f.loop.engine.registrationSlots
	require.True(t, slots.TryAcquire(defaultMaxPendingRegistrations))
	var once sync.Once
	release := func() { once.Do(func() { slots.Release(defaultMaxPendingRegistrations) }) }
	t.Cleanup(release)

	// Both APIs reject overload before spawning preparation work. An Enroll
	// request rejected here does not take ownership of the caller's net.Conn.
	ch, err := f.loop.Register(context.Background(), &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1})
	require.ErrorIs(t, err, errorx.ErrTooManyPendingRegistrations)
	require.Nil(t, ch)
	c, peer := enrollmentSocketPair(t)
	ch, err = f.loop.Enroll(context.Background(), c)
	require.ErrorIs(t, err, errorx.ErrTooManyPendingRegistrations)
	require.Nil(t, ch)
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(enrollTestTimeout)))
	_, err = c.Write([]byte{42})
	require.NoError(t, err)
	var b [1]byte
	_, err = peer.Read(b[:])
	require.NoError(t, err)

	release()
	ch, err = f.loop.Enroll(context.Background(), c)
	require.NoError(t, err)
	result := awaitEnrollmentResult(t, ch)
	require.NoError(t, result.Err)
	require.NotNil(t, result.Conn)
	// An established connection must not consume registration capacity.
	f.barrier(t)
	assertRegistrationSlotsFree(t, slots, defaultMaxPendingRegistrations)
	require.Equal(t, int32(1), f.loop.countConn())
}

func TestEnrollLimitSharedAcrossLoopsAndIndependentAcrossEngines(t *testing.T) {
	f := newEnrollmentFixture(t, &enrollmentHandler{}, nil, 1)
	slots := f.loop.engine.registrationSlots
	require.True(t, slots.TryAcquire(1))
	defer slots.Release(1)
	otherLoop := &eventloop{engine: f.loop.engine}
	ch, err := otherLoop.Register(context.Background(), &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1})
	require.ErrorIs(t, err, errorx.ErrTooManyPendingRegistrations)
	require.Nil(t, ch)

	// Exhausting one engine must not reject registrations on another engine.
	other := newEnrollmentFixture(t, &enrollmentHandler{}, nil, 1)
	r, err := other.loop.newConnRegistration(context.Background())
	require.NoError(t, err)
	dialErr := errors.New("dial failed")
	go r.prepare(nil, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1},
		func(context.Context, string, string) (net.Conn, error) { return nil, dialErr })
	result := awaitEnrollmentResult(t, r.result)
	require.ErrorIs(t, result.Err, dialErr)
	assertRegistrationSlotsFree(t, other.loop.engine.registrationSlots, 1)
}

func TestEnrollLimitHeldUntilCanceledDialReturns(t *testing.T) {
	f := newEnrollmentFixture(t, &enrollmentHandler{}, nil, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := f.loop.newConnRegistration(ctx)
	require.NoError(t, err)
	entered, returned, gate := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	go func() {
		defer close(returned)
		r.prepare(nil, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1},
			func(ctx context.Context, _, _ string) (net.Conn, error) {
				close(entered)
				<-ctx.Done()
				<-gate
				return nil, ctx.Err()
			})
	}()
	awaitEnrollmentSignal(t, entered)
	cancel()
	result := awaitEnrollmentResult(t, r.result)
	require.ErrorIs(t, result.Err, context.Canceled)

	// Publishing cancellation must not admit another dial while the previous
	// preparation goroutine is still cleaning up.
	ch, err := f.loop.Register(context.Background(), &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1})
	require.ErrorIs(t, err, errorx.ErrTooManyPendingRegistrations)
	require.Nil(t, ch)
	release()
	awaitEnrollmentSignal(t, returned)
	assertRegistrationSlotsFree(t, f.loop.engine.registrationSlots, 1)
}

func TestEnrollLimitHeldUntilCanceledOnOpenReturns(t *testing.T) {
	entered, gate := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	h := &enrollmentHandler{onOpen: func(Conn) { entered <- struct{}{}; <-gate }}
	f := newEnrollmentFixture(t, h, nil, 1)
	t.Cleanup(release)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, _ := enrollmentSocketPair(t)
	ch, err := f.loop.Enroll(ctx, c)
	require.NoError(t, err)
	awaitEnrollmentSignal(t, entered)
	cancel()
	result := awaitEnrollmentResult(t, ch)
	require.ErrorIs(t, result.Err, context.Canceled)
	ch, err = f.loop.Register(context.Background(), &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1})
	require.ErrorIs(t, err, errorx.ErrTooManyPendingRegistrations)
	require.Nil(t, ch)

	// The loop retains its slot until OnOpen returns and the claimed fd closes.
	release()
	f.barrier(t)
	assertRegistrationSlotsFree(t, f.loop.engine.registrationSlots, 1)
	require.Equal(t, int32(1), h.closed.Load())
}

func TestEnrollLimitReturnedAfterPrepareFailure(t *testing.T) {
	f := newEnrollmentFixture(t, &enrollmentHandler{}, nil, 1)
	c, peer := net.Pipe()
	t.Cleanup(func() { _ = c.Close(); _ = peer.Close() })
	ch, err := f.loop.Enroll(context.Background(), c)
	require.NoError(t, err)
	result := awaitEnrollmentResult(t, ch)
	require.ErrorContains(t, result.Err, "failed to assert syscall.Conn")
	require.Nil(t, result.Conn)
	assertRegistrationSlotsFree(t, f.loop.engine.registrationSlots, 1)
}

func TestEnrollConcurrentAdmissionIsBounded(t *testing.T) {
	const limit, callers = 4, 32
	f := newEnrollmentFixture(t, &enrollmentHandler{}, nil, limit)
	start, gate := make(chan struct{}), make(chan struct{})
	admissions := make(chan error, callers)
	entered := make(chan struct{}, limit)
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	var wg sync.WaitGroup
	var dialing atomic.Int32
	dialErr := errors.New("dial failed")
	for range callers {
		wg.Go(func() {
			<-start
			r, err := f.loop.newConnRegistration(context.Background())
			admissions <- err
			if err != nil {
				return
			}
			r.prepare(nil, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1},
				func(context.Context, string, string) (net.Conn, error) {
					dialing.Add(1)
					entered <- struct{}{}
					<-gate
					dialing.Add(-1)
					return nil, dialErr
				})
		})
	}
	close(start)
	accepted := 0
	for range callers {
		select {
		case err := <-admissions:
			if err == nil {
				accepted++
			} else {
				require.ErrorIs(t, err, errorx.ErrTooManyPendingRegistrations)
			}
		case <-time.After(enrollTestTimeout):
			t.Fatal("concurrent admission did not return")
		}
	}
	require.Equal(t, limit, accepted)
	for range limit {
		awaitEnrollmentSignal(t, entered)
	}
	require.Equal(t, int32(limit), dialing.Load())
	release()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	awaitEnrollmentSignal(t, done)
	require.Zero(t, dialing.Load())
	assertRegistrationSlotsFree(t, f.loop.engine.registrationSlots, limit)
}

func TestEnrollCanceledBeforeDial(t *testing.T) {
	f := newEnrollmentFixture(t, &enrollmentHandler{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// An already canceled request is rejected synchronously, before dialing
	// or transferring ownership of an established net.Conn.
	ch, err := f.loop.Register(ctx, &net.UnixAddr{Name: "unused", Net: "unix"})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, ch)
	c, peer := enrollmentSocketPair(t)
	ch, err = f.loop.Enroll(ctx, c)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, ch)
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(enrollTestTimeout)))
	_, err = c.Write([]byte{42})
	require.NoError(t, err)
	var b [1]byte
	_, err = peer.Read(b[:])
	require.NoError(t, err)
	require.Equal(t, byte(42), b[0])
}

func TestEnrollCanceledDuringDial(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		name := "caller"
		if shutdown {
			name = "engine"
		}
		t.Run(name, func(t *testing.T) {
			f := newEnrollmentFixture(t, &enrollmentHandler{}, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r, err := f.loop.newConnRegistration(ctx)
			require.NoError(t, err)
			entered, returned := make(chan struct{}), make(chan struct{})

			// A listener that does not Accept can still finish the TCP
			// handshake. Inject a dial blocked on ctx.Done instead, so this
			// test exercises cancellation of an in-flight dial deterministically.
			go func() {
				defer close(returned)
				r.prepare(nil, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1},
					func(ctx context.Context, network, address string) (net.Conn, error) {
						close(entered)
						<-ctx.Done()
						return nil, ctx.Err()
					})
			}()
			awaitEnrollmentSignal(t, entered)
			want := error(context.Canceled)
			if shutdown {
				f.loop.engine.shutdown(nil)
				want = errorx.ErrEngineInShutdown
			} else {
				cancel()
			}
			result := awaitEnrollmentResult(t, r.result)
			require.ErrorIs(t, result.Err, want)
			require.Nil(t, result.Conn)
			awaitEnrollmentSignal(t, returned)
		})
	}
}

func TestEnrollCanceledBeforeRegister(t *testing.T) {
	h := &enrollmentHandler{}
	f := newEnrollmentFixture(t, h, nil)
	release := f.block(t)
	c, _ := enrollmentSocketPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := f.loop.newConnRegistration(ctx)
	require.NoError(t, err)
	go r.prepare(c, c.RemoteAddr(), new(net.Dialer).DialContext)
	awaitOriginalConnClosed(t, c)

	// Closing the original net.Conn happens after Trigger has queued register.
	// The event loop is still blocked, so ownership remains with the request.
	r.mu.Lock()
	pending := r.pending
	r.mu.Unlock()
	require.NotNil(t, pending)
	fd := pending.fd
	cancel()
	result := awaitEnrollmentResult(t, r.result)
	require.ErrorIs(t, result.Err, context.Canceled)
	require.Nil(t, result.Conn)
	_, err = unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	require.ErrorIs(t, err, unix.EBADF)

	// Reuse that fd for a different file before the stale callback runs.
	// The callback must neither register nor close the replacement fd.
	replacement, err := unix.Open("/dev/null", unix.O_RDONLY|unix.O_CLOEXEC, 0)
	require.NoError(t, err)
	if replacement != fd {
		err = unix.Dup2(replacement, fd)
		_ = unix.Close(replacement)
		require.NoError(t, err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	release()
	f.barrier(t)
	_, err = unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	require.NoError(t, err)
	require.Zero(t, h.opened.Load())
	require.Zero(t, f.loop.countConn())
}

func TestEnrollShutdownWhilePending(t *testing.T) {
	h := &enrollmentHandler{}
	f := newEnrollmentFixture(t, h, nil)
	release := f.block(t)
	c, _ := enrollmentSocketPair(t)
	ch, err := f.loop.Enroll(context.Background(), c)
	require.NoError(t, err)
	awaitOriginalConnClosed(t, c)

	// Shutdown can skip queued registration work. Its context hook must still
	// publish an error and release the pending fd before the loop exits.
	f.loop.engine.shutdown(nil)
	result := awaitEnrollmentResult(t, ch)
	require.ErrorIs(t, result.Err, errorx.ErrEngineInShutdown)
	require.Nil(t, result.Conn)
	require.False(t, f.loop.engine.isShutdown(), "the blocked event loop has not finished shutdown")
	ch, err = f.loop.Register(context.Background(), &net.UnixAddr{Name: "unused", Net: "unix"})
	require.ErrorIs(t, err, errorx.ErrEngineInShutdown)
	require.Nil(t, ch)
	release()
	awaitEnrollmentSignal(t, f.stopped)
	require.Zero(t, h.opened.Load())
	require.Zero(t, f.loop.countConn())
	ch, err = f.loop.Register(context.Background(), &net.UnixAddr{Name: "unused", Net: "unix"})
	require.ErrorIs(t, err, errorx.ErrEngineInShutdown)
	require.Nil(t, ch)
}

func TestEnrollOnOpenClose(t *testing.T) {
	h := &enrollmentHandler{openAction: Close}
	f := newEnrollmentFixture(t, h, nil)
	c, _ := enrollmentSocketPair(t)
	ch, err := f.loop.Enroll(context.Background(), c)
	require.NoError(t, err)
	result := awaitEnrollmentResult(t, ch)
	// register0 can return nil after OnOpen requests Close. The caller must
	// receive net.ErrClosed, rather than a successful result for a released conn.
	require.ErrorIs(t, result.Err, net.ErrClosed)
	require.Nil(t, result.Conn)
	f.barrier(t)
	require.Equal(t, int32(1), h.opened.Load())
	require.Equal(t, int32(1), h.closed.Load())
	require.Zero(t, f.loop.countConn())
}

func TestEnrollCanceledDuringOnOpen(t *testing.T) {
	entered, gate := make(chan int, 1), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	h := &enrollmentHandler{
		onOpen: func(c Conn) {
			entered <- c.Fd()
			<-gate
		},
	}
	f := newEnrollmentFixture(t, h, nil)
	t.Cleanup(release)
	c, _ := enrollmentSocketPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := f.loop.Enroll(ctx, c)
	require.NoError(t, err)
	var fd int
	select {
	case fd = <-entered:
	case <-time.After(enrollTestTimeout):
		t.Fatal("OnOpen did not start")
	}
	cancel()
	result := awaitEnrollmentResult(t, ch)
	require.ErrorIs(t, result.Err, context.Canceled)
	require.Nil(t, result.Conn)

	// Once OnOpen starts, the loop owns the fd. The cancellation goroutine
	// must leave it open until OnOpen returns and the loop performs cleanup.
	_, err = unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	require.NoError(t, err)
	release()
	f.barrier(t)
	_, err = unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	require.ErrorIs(t, err, unix.EBADF)
	require.Equal(t, int32(1), h.closed.Load())
	require.Zero(t, f.loop.countConn())
}

func TestEnrollSuccessSurvivesCancellation(t *testing.T) {
	f := newEnrollmentFixture(t, &enrollmentHandler{}, nil)
	c, peer := enrollmentSocketPair(t)
	ctx, cancel := context.WithCancel(NewContext(context.Background(), "session"))
	defer cancel()
	ch, err := f.loop.Enroll(ctx, c)
	require.NoError(t, err)
	result := awaitEnrollmentResult(t, ch)
	require.NoError(t, result.Err)
	require.NotNil(t, result.Conn)
	require.Equal(t, "session", result.Conn.Context())
	cancel()

	// The request context only controls registration. A published connection
	// must remain usable after the caller cancels that context.
	writeErr := make(chan error, 1)
	require.NoError(t, f.loop.Execute(context.Background(), func(context.Context) error {
		_, err := result.Conn.Write([]byte{42})
		writeErr <- err
		return nil
	}))
	select {
	case err := <-writeErr:
		require.NoError(t, err)
	case <-time.After(enrollTestTimeout):
		t.Fatal("write did not execute")
	}
	require.NoError(t, peer.SetReadDeadline(time.Now().Add(enrollTestTimeout)))
	var b [1]byte
	_, err = peer.Read(b[:])
	require.NoError(t, err)
	require.Equal(t, byte(42), b[0])
}

func TestEnrollRegisterDialsUnix(t *testing.T) {
	f := newEnrollmentFixture(t, &enrollmentHandler{}, nil)
	// Keep the socket path short enough for sockaddr_un on macOS.
	dir, err := os.MkdirTemp("", "nio-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	ln, err := net.Listen("unix", filepath.Join(dir, "s"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	ctx, cancel := context.WithTimeout(NewContext(context.Background(), "dialed"), enrollTestTimeout)
	defer cancel()
	ch, err := (Engine{eng: f.loop.engine}).Register(NewNetAddrContext(ctx, ln.Addr()))
	require.NoError(t, err)
	result := awaitEnrollmentResult(t, ch)
	require.NoError(t, result.Err)
	require.NotNil(t, result.Conn)
	require.Equal(t, "dialed", result.Conn.Context())
	peer, err := ln.Accept()
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close() })
}

func TestEnrollRegistrationErrorIsReportedAndLogged(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "enroll.log")
	f := newEnrollmentFixture(t, &enrollmentHandler{}, &log.FileConfig{Filename: filename})
	f.loop.getLogger().SetLevel(zap.DebugLevel)
	c, _ := enrollmentSocketPair(t)
	cc, err := f.loop.duplicateConn(c)
	require.NoError(t, err)
	require.NoError(t, unix.Close(cc.fd))
	// Force AddRead to fail without depending on OS-specific file support.
	cc.fd = -1
	cc.pollAttachment.FD = -1
	r, err := f.loop.newConnRegistration(context.Background())
	require.NoError(t, err)
	// This test supplies an already duplicated fd instead of running prepare.
	r.finishWork()
	r.mu.Lock()
	r.pending = cc
	r.mu.Unlock()
	require.NoError(t, f.loop.Execute(context.Background(), func(context.Context) error {
		return r.register(nil)
	}))
	result := awaitEnrollmentResult(t, r.result)
	require.Error(t, result.Err)
	require.Nil(t, result.Conn)
	f.barrier(t) // register's deferred log has finished before flushing.
	require.NoError(t, f.loop.getLogger().Sync())
	data, err := os.ReadFile(filename)
	require.NoError(t, err)
	require.Contains(t, string(data), "failed to register connection")
}

type enrollmentHandler struct {
	BuiltinEventEngine
	openAction Action
	onOpen     func(Conn)
	opened     atomic.Int32
	closed     atomic.Int32
}

func (h *enrollmentHandler) OnOpen(c Conn) ([]byte, Action) {
	h.opened.Add(1)
	if h.onOpen != nil {
		h.onOpen(c)
	}
	return nil, h.openAction
}

func (h *enrollmentHandler) OnClose(Conn, error) Action {
	h.closed.Add(1)
	return None
}

type enrollmentFixture struct {
	loop    *eventloop
	stopped chan struct{}
}

func newEnrollmentFixture(t *testing.T, h EventHandler, cfg *log.FileConfig, limits ...int64) *enrollmentFixture {
	t.Helper()
	limit := int64(defaultMaxPendingRegistrations)
	if len(limits) > 0 {
		limit = limits[0]
	}
	assertEnrollmentNoFDLeak(t)
	logger, closeLogger := log.NewLogger(cfg)
	t.Cleanup(func() { require.NoError(t, closeLogger()) })
	root, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(root)
	eng := &engine{
		listeners:         swiss.NewTable[int, *listener](hash.Int),
		opts:              &Options{Logger: logger, ReadBufferCap: 1024, WriteBufferCap: 1024},
		turnOff:           cancel,
		eventHandler:      h,
		registrationSlots: semaphore.NewWeighted(limit),
	}
	eng.concurrency.Group, eng.concurrency.ctx = eg, ctx
	// Validate needs a listener, but these tests enroll connections directly.
	// The sentinel has no real fd and is never registered with the poller.
	eng.listeners.Set(-1, &listener{fd: -1, logger: logger})
	poller, err := netpoll.OpenPoller(logger)
	require.NoError(t, err)
	el := &eventloop{
		engine: eng, listeners: eng.listeners, poller: poller,
		buffer: make([]byte, 1024), eventHandler: h,
	}
	el.connections.init(0)
	eng.eventLoops = &leastConnectionsLoadBalancer{
		baseLoadBalancer: baseLoadBalancer{eventLoops: []*eventloop{el}, size: 1},
	}
	f := &enrollmentFixture{loop: el, stopped: make(chan struct{})}
	eg.Go(el.orbit)
	go func() {
		eng.stop(root, Engine{eng: eng})
		close(f.stopped)
	}()
	t.Cleanup(func() {
		cancel()
		awaitEnrollmentSignal(t, f.stopped)
		assertRegistrationSlotsFree(t, eng.registrationSlots, limit)
	})
	return f
}

func assertRegistrationSlotsFree(t *testing.T, slots *semaphore.Weighted, limit int64) {
	t.Helper()
	require.Eventually(t, func() bool {
		if !slots.TryAcquire(limit) {
			return false
		}
		slots.Release(limit)
		return true
	}, enrollTestTimeout, time.Millisecond, "registration leaked semaphore slots")
}

func (f *enrollmentFixture) block(t *testing.T) func() {
	t.Helper()
	entered, gate := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	require.NoError(t, f.loop.Execute(context.Background(), func(context.Context) error {
		close(entered)
		<-gate
		return nil
	}))
	awaitEnrollmentSignal(t, entered)
	return release
}

func (f *enrollmentFixture) barrier(t *testing.T) {
	t.Helper()
	done := make(chan struct{})
	require.NoError(t, f.loop.Execute(context.Background(), func(context.Context) error {
		close(done)
		return nil
	}))
	awaitEnrollmentSignal(t, done)
}

func awaitEnrollmentResult(t *testing.T, ch <-chan RegisteredResult) RegisteredResult {
	t.Helper()
	var result RegisteredResult
	select {
	case value, ok := <-ch:
		require.True(t, ok, "result channel closed without a result")
		result = value
	case <-time.After(enrollTestTimeout):
		t.Fatal("registration did not complete")
	}
	select {
	case _, ok := <-ch:
		require.False(t, ok, "registration published more than one result")
	case <-time.After(enrollTestTimeout):
		t.Fatal("result channel was not closed")
	}
	return result
}

func awaitEnrollmentSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(enrollTestTimeout):
		t.Fatal("timed out waiting for enrollment test synchronization")
	}
}

func awaitOriginalConnClosed(t *testing.T, c net.Conn) {
	t.Helper()
	raw, err := c.(syscall.Conn).SyscallConn()
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return errors.Is(raw.Control(func(uintptr) {}), net.ErrClosed)
	}, enrollTestTimeout, time.Millisecond, "preparation did not close the original net.Conn")
}

func enrollmentSocketPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	require.NoError(t, err)
	files := [2]*os.File{os.NewFile(uintptr(fds[0]), "enroll-client"), os.NewFile(uintptr(fds[1]), "enroll-peer")}
	var conns [2]net.Conn
	for i, file := range files {
		t.Cleanup(func() { _ = file.Close() })
		conns[i], err = net.FileConn(file)
		require.NoError(t, err)
		require.NoError(t, file.Close())
		c := conns[i]
		t.Cleanup(func() { _ = c.Close() })
	}
	return conns[0], conns[1]
}

var enrollmentFDWarmup sync.Once

func assertEnrollmentNoFDLeak(t *testing.T) {
	t.Helper()
	// Go's runtime poller keeps process-wide fds. Initialize it before taking
	// the baseline so those fds are not mistaken for leaks on the first test.
	enrollmentFDWarmup.Do(func() {
		c, peer := enrollmentSocketPair(t)
		require.NoError(t, c.Close())
		require.NoError(t, peer.Close())
	})
	before := enrollmentFDCount(t)
	t.Cleanup(func() {
		require.Eventually(t, func() bool {
			return enrollmentFDCount(t) == before
		}, enrollTestTimeout, time.Millisecond, "enrollment leaked fds: before=%d, after=%d", before, enrollmentFDCount(t))
	})
}

func enrollmentFDCount(t *testing.T) int {
	t.Helper()
	dir := "/dev/fd"
	if runtime.GOOS == "linux" {
		dir = "/proc/self/fd"
	}
	// Read names only: fstatat on a kqueue entry in macOS /dev/fd can fail
	// with EBADF even though the descriptor is open.
	f, err := os.Open(dir)
	require.NoError(t, err)
	defer f.Close() //nolint:errcheck
	names, err := f.Readdirnames(-1)
	require.NoError(t, err)
	return len(names)
}
