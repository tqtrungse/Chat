/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Modified from the original gnet source by tqtrungse@gmail.com in 2026.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nio

import (
	"context"
	"io"
	"net"
	"net/url"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"xxx/pkg"
	"xxx/pkg/buffer"
	"xxx/pkg/collection/swiss"
	"xxx/pkg/hash"
	errorx "xxx/pkg/nio/error"
	"xxx/pkg/nio/socket"
	workerpool "xxx/pkg/pool/worker"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Action is an action that occurs after the completion of an event.
type Action int

const (
	// None indicates that no action should occur following an event.
	None Action = iota

	// Close closes the connection.
	Close

	// Shutdown shutdowns the engine.
	Shutdown

	// defaultNumberWorkers sets up the capacity of worker pool, 256 * 1024. (by ants)
	defaultNumberWorkers = 8192
)

var (
	allEngines sync.Map

	// shutdownPollInterval is how often we poll to check whether engine has been shut down during gnet.Stop().
	shutdownPollInterval = 500 * time.Millisecond

	// MaxStreamBufferCap is the default buffer size for each stream-oriented connection(TCP/Unix).
	MaxStreamBufferCap = 64 * 1024 // 64KB

	defaultWorkerPool, closePool = workerpool.New(workerpool.Config{NumWorkers: defaultNumberWorkers}, nil)
)

type (
	// EventHandler represents the engine events' callbacks for the Run call.
	// Each event has an Action return value that is used manage the state
	// of the connection and engine.
	EventHandler interface {
		// OnBoot fires when the engine is ready for accepting connections.
		// The parameter engine has information and various utilities.
		OnBoot(eng Engine) (action Action)

		// OnShutdown fires when the engine is being shut down, it is called right after
		// all event-loops and connections are closed.
		OnShutdown(eng Engine)

		// OnOpen fires when a new connection has been opened.
		//
		// The Conn c has information about the connection such as its local and remote addresses.
		// The parameter out is the return value which is going to be sent back to the remote.
		// Sending large amounts of data back to the remote in OnOpen is usually not recommended.
		OnOpen(c Conn) (out []byte, action Action)

		// OnClose fires when a connection has been closed.
		// The parameter err is the last known connection error.
		OnClose(c Conn, err error) (action Action)

		// OnTraffic fires when a socket receives data from the remote.
		//
		// Also check out the comments on Reader and Writer interfaces.
		OnTraffic(c Conn) (action Action)

		// OnTick fires immediately after the engine starts and will fire again
		// following the duration specified by the delay return value.
		OnTick() (delay time.Duration, action Action)
	}

	//// Runnable defines the common protocol of an execution on an event-loop.
	//// This interface should be implemented and passed to an event-loop in some way,
	//// then the event-loop will invoke Run to perform the execution.
	//// !!!Caution: Run must not contain any blocking operations like heavy disk or
	//// network I/O, or else it will block the event-loop.
	//Runnable interface {
	//	// Run is about to be executed by the event-loop.
	//	Run(ctx context.Context) error
	//}

	// EventLoop provides a set of methods for manipulating the event-loop.
	EventLoop interface {
		// Register connects to the given address and registers the connection to the current event-loop,
		// it's concurrency-safe.
		Register(ctx context.Context, addr net.Addr) (<-chan RegisteredResult, error)

		// Enroll is like Register, but it accepts an established net.Conn instead of a net.Addr,
		// it's concurrency-safe.
		Enroll(ctx context.Context, c net.Conn) (<-chan RegisteredResult, error)

		// Execute will execute the given runnable on the event-loop at some time in the future,
		// it's concurrency-safe.
		Execute(ctx context.Context, runnable RunnableFunc) error

		//// Schedule is like Execute, but it allows you to specify when the runnable is executed.
		//// In other words, the runnable will be executed when the delay duration is reached,
		//// it's concurrency-safe.
		//// TODO: not supported yet, implement this.
		//Schedule(ctx context.Context, runnable Runnable, delay time.Duration) error

		// Close closes the given Conn that belongs to the current event-loop.
		// It must be called on the same event-loop that the connection belongs to.
		// This method is not concurrency-safe, you must invoke it on the event loop.
		Close(Conn) error
	}

	// Reader is an interface that consists of a number of methods for reading that Conn must implement.
	//
	// Note that the methods in this interface are not concurrency-safe for concurrent use,
	// you must invoke them within any method in EventHandler.
	Reader interface {
		io.Reader
		io.WriterTo

		// Next returns the next n bytes and advances the inbound buffer.
		// buf must not be used in a new goroutine. Otherwise, use Read instead.
		//
		// If the number of the available bytes is less than requested,
		// a pair of (0, io.ErrShortBuffer) is returned.
		Next(n int) (buf []byte, err error)

		// Peek returns the next n bytes without advancing the inbound buffer,
		// the returned bytes remain valid until a Discard is called.
		// buf must neither be used in a new goroutine nor anywhere after the call
		// to Discard, make a copy of buf manually or use Read otherwise.
		//
		// If the number of the available bytes is less than requested,
		// a pair of (0, io.ErrShortBuffer) is returned.
		Peek(n int) (buf []byte, err error)

		// Discard advances the inbound buffer with next n bytes, returning the number of bytes discarded.
		Discard(n int) (discarded int, err error)

		// InboundBuffered returns the number of bytes that can be read from the current buffer.
		InboundBuffered() int
	}

	// Writer is an interface that consists of a number of methods for writing that Conn must implement.
	Writer interface {
		io.Writer     // not concurrency-safe
		io.ReaderFrom // not concurrency-safe

		// SendTo transmits a message to the given address, it's not concurrency-safe.
		// It is available only for UDP sockets, an ErrUnsupportedOp will be returned
		// when it is called on a non-UDP socket.
		// This method should be used only when you need to send a message to a specific
		// address over the UDP socket, otherwise you should use Conn.Write() instead.
		SendTo(buf []byte, addr net.Addr) (n int, err error)

		// Writev writes multiple byte slices to remote synchronously, it's not concurrency-safe,
		// you must invoke it within any method in EventHandler.
		Writev(bs [][]byte) (n int, err error)

		// Flush writes any buffered data to the underlying connection, it's not concurrency-safe,
		// you must invoke it within any method in EventHandler.
		Flush() error

		// OutboundBuffered returns the number of bytes that can be read from the current buffer.
		// it's not concurrency-safe, you must invoke it within any method in EventHandler.
		OutboundBuffered() int

		// AsyncWrite writes bytes to remote asynchronously, it's concurrency-safe,
		// you don't have to invoke it within any method in EventHandler,
		// usually you would call it in an individual goroutine.
		//
		// Note that it will go synchronously with UDP, so it is needless to call
		// this asynchronous method, we may disable this method for UDP and just
		// return ErrUnsupportedOp in the future, therefore, please don't rely on
		// this method to do something important under UDP, if you're working with UDP,
		// just call Conn.Write to send back your data.
		AsyncWrite(buf []byte, callback AsyncCallback) (err error)

		// AsyncWritev writes multiple byte slices to remote asynchronously,
		// you don't have to invoke it within any method in EventHandler,
		// usually you would call it in an individual goroutine.
		AsyncWritev(bs [][]byte, callback AsyncCallback) (err error)
	}

	// Socket is a set of functions which manipulate the underlying file descriptor of a connection.
	//
	// Note that the methods in this interface are concurrency-safe for concurrent use,
	// you don't have to invoke them within any method in EventHandler.
	Socket interface {
		// Gfd returns the gfd of socket.
		// Gfd() gfd.GFD

		// Fd returns the underlying file descriptor.
		Fd() int

		// Dup returns a copy of the underlying file descriptor.
		// It is the caller's responsibility to close fd when finished.
		// Closing c does not affect fd, and closing fd does not affect c.
		//
		// The returned file descriptor is different from the
		//  connection. Attempting to change the properties of the original
		// using this duplicate may or may not have the desired effect.
		Dup() (int, error)

		// SetReadBuffer sets the size of the operating system's
		// receive buffer associated with the connection.
		SetReadBuffer(size int) error

		// SetWriteBuffer sets the size of the operating system's
		// transmit buffer associated with the connection.
		SetWriteBuffer(size int) error

		// SetLinger sets the behavior of Close on a connection which still
		// has data waiting to be sent or to be acknowledged.
		//
		// If secs < 0 (the default), the operating system finishes sending the
		// data in the background.
		//
		// If secs == 0, the operating system discards any unsent or
		// unacknowledged data.
		//
		// If secs > 0, the data is sent in the background as with sec < 0. On
		// some operating systems after sec seconds have elapsed any remaining
		// unsent data may be discarded.
		SetLinger(secs int) error

		// SetKeepAlivePeriod tells the operating system to send keep-alive
		// messages on the connection and sets period between TCP keep-alive probes.
		SetKeepAlivePeriod(d time.Duration) error

		// SetKeepAlive enables/disables the TCP keepalive with all socket options:
		// TCP_KEEPIDLE, TCP_KEEPINTVL and TCP_KEEPCNT. idle is the value for TCP_KEEPIDLE,
		// intvl is the value for TCP_KEEPINTVL, cnt is the value for TCP_KEEPCNT,
		// ignored when enabled is false.
		//
		// With TCP keep-alive enabled, idle is the time (in seconds) the connection
		// needs to remain idle before TCP starts sending keep-alive probes,
		// intvl is the time (in seconds) between individual keep-alive probes.
		// TCP will drop the connection after sending cnt probes without getting
		// any replies from the peer; then the socket is destroyed, and OnClose
		// is triggered.
		//
		// If one of idle, intvl, or cnt is less than 1, an error is returned.
		SetKeepAlive(enabled bool, idle, intvl time.Duration, cnt int) error

		// SetNoDelay controls whether the operating system should delay
		// packet transmission in hopes of sending fewer packets (Nagle's
		// algorithm).
		// The default is true (no delay), meaning that data is sent as soon as possible after a Write.
		SetNoDelay(noDelay bool) error
	}

	// Conn is an interface of underlying connection.
	Conn interface {
		Reader // all methods in Reader are not concurrency-safe.
		Writer // some methods in Writer are concurrency-safe, some are not.
		Socket // all methods in Socket are concurrency-safe.

		// EventLoop returns the event-loop that the connection belongs to.
		// The returned EventLoop is concurrency-safe.
		EventLoop() EventLoop

		// Context returns a user-defined context, it's concurrency-safe.
		Context() (ctx any)

		// SetContext sets a user-defined context, it's concurrency-safe.
		SetContext(ctx any)

		// LocalAddr is the connection's local socket address, it's not concurrency-safe,
		// you must invoke it within any method in EventHandler.
		LocalAddr() net.Addr

		// RemoteAddr is the connection's remote address, it's not concurrency-safe,
		// you must invoke it within any method in EventHandler.
		RemoteAddr() net.Addr

		// Wake triggers an OnTraffic event for the current connection, it's concurrency-safe.
		Wake(callback AsyncCallback) error

		// CloseWithCallback closes the current connection, it's concurrency-safe.
		// Usually you should provide a non-nil callback for this method,
		// otherwise your better choice is Close().
		CloseWithCallback(callback AsyncCallback) error

		// Close closes the current connection, implements net.Conn, it's concurrency-safe.
		Close() error

		//// SetDeadline implements net.Conn.
		//SetDeadline(time.Time) error
		//
		//// SetReadDeadline implements net.Conn.
		//SetReadDeadline(time.Time) error
		//
		//// SetWriteDeadline implements net.Conn.
		//SetWriteDeadline(time.Time) error
	}
)

type RegisteredResult struct {
	Conn Conn
	Err  error
}

// Engine represents an engine context which provides some functions.
type Engine struct {
	// eng is the internal engine struct.
	eng *engine
}

// Validate checks whether the engine is available.
func (e Engine) Validate() error {
	if e.eng == nil || e.eng.listeners.Len() == 0 {
		return errorx.ErrEmptyEngine
	}
	if e.eng.isShutdown() {
		return errorx.ErrEngineInShutdown
	}
	return nil
}

// CountConnections counts the number of currently active connections and returns it.
func (e Engine) CountConnections() (count int) {
	if e.Validate() != nil {
		return -1
	}

	e.eng.eventLoops.iterate(func(_ int, el *eventloop) bool {
		count += int(el.countConn())
		return true
	})
	return
}

// Register registers the new connection to the event-loop that is chosen
// based off of the algorithm set by WithLoadBalancing.
// You should call either of the NewNetConnContext or NewNetAddrContext
// and pass the returned context to this method. net.Conn will precede
// net.Addr if both are present in the context.
//
// Note that you need to switch to another load-balancing algorithm over
// the default RoundRobin when starting the engine, to avoid data race
// issue if you plan on calling this method from somewhere later on.
func (e Engine) Register(ctx context.Context) (<-chan RegisteredResult, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}

	if e.eng.eventLoops.len() == 0 {
		return nil, errorx.ErrEmptyEngine
	}

	c, ok := FromNetConnContext(ctx)
	if ok {
		return e.eng.eventLoops.next(c.RemoteAddr()).Enroll(ctx, c)
	}

	addr, ok := FromNetAddrContext(ctx)
	if ok {
		return e.eng.eventLoops.next(addr).Register(ctx, addr)
	}

	return nil, errorx.ErrInvalidNetworkAddress
}

// Dup returns a copy of the underlying file descriptor of listener.
// It is the caller's responsibility to close dupFD when finished.
// Closing listener does not affect dupFD, and closing dupFD does not affect listener.
//
// Note that this method is only available when the engine has only one listener.
func (e Engine) Dup() (fd int, err error) {
	if err = e.Validate(); err != nil {
		return -1, err
	}

	if e.eng.listeners.Len() > 1 {
		return -1, errorx.ErrUnsupportedOp
	}

	e.eng.listeners.Range(func(_ int, ln *listener) (goOn bool) {
		fd, err = ln.dup()
		return true
	})
	return
}

// DupListener is like Dup, but it duplicates the listener with the given network and address.
// This is useful when there are multiple listeners.
func (e Engine) DupListener(network, addr string) (fd int, err error) {
	fd = -1
	if err = e.Validate(); err != nil {
		return
	}

	e.eng.listeners.Range(func(_ int, ln *listener) (goOn bool) {
		if ln.network == network && ln.address == addr {
			fd, err = ln.dup()
			return false
		}
		return true
	})
	if fd != -1 {
		return
	}

	err = errorx.ErrInvalidNetworkAddress
	return
}

// Stop gracefully shuts down this Engine without interrupting any active event-loops,
// it waits indefinitely for connections and event-loops to be closed and then shuts down.
func (e Engine) Stop(ctx context.Context) error {
	if err := e.Validate(); err != nil {
		return err
	}

	e.eng.shutdown(nil)

	ticker := time.NewTicker(shutdownPollInterval)
	defer ticker.Stop()
	defer closePool()
	for {
		if e.eng.isShutdown() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// BuiltinEventEngine is a built-in implementation of EventHandler which feeds
// each method with an empty implementation, you can embed it within your custom
// struct when you don't intend to implement the entire EventHandler.
type BuiltinEventEngine struct{}

// OnBoot fires when the engine is ready for accepting connections.
// The parameter engine has information and various utilities.
func (*BuiltinEventEngine) OnBoot(_ Engine) (action Action) {
	return
}

// OnShutdown fires when the engine is being shut down, it is called right after
// all event-loops and connections are closed.
func (*BuiltinEventEngine) OnShutdown(_ Engine) {
}

// OnOpen fires when a new connection has been opened.
// The parameter out is the return value which is going to be sent back to the remote.
func (*BuiltinEventEngine) OnOpen(_ Conn) (out []byte, action Action) {
	return
}

// OnClose fires when a connection has been closed.
// The parameter err is the last known connection error.
func (*BuiltinEventEngine) OnClose(_ Conn, _ error) (action Action) {
	return
}

// OnTraffic fires when a local socket receives data from the remote.
func (*BuiltinEventEngine) OnTraffic(_ Conn) (action Action) {
	return
}

// OnTick fires immediately after the engine starts and will fire again
// following the duration specified by the delay return value.
func (*BuiltinEventEngine) OnTick() (delay time.Duration, action Action) {
	return
}

type (
	// AsyncCallback is a callback that will be invoked after the asynchronous function finishes.
	//
	// Note that the parameter gnet.Conn might have been already released when it's UDP protocol,
	// thus it shouldn't be accessed.
	// This callback will be executed in event-loop, thus it must not block, otherwise,
	// it blocks the event-loop.
	AsyncCallback func(c Conn, err error) error

	// RunnableFunc defines the common protocol of an execution on an event-loop.
	// This interface should be implemented and passed to an event-loop in some way,
	// then the event-loop will invoke Run to perform the execution.
	// !!!Caution: Run must not contain any blocking operations like heavy disk or
	// network I/O, or else it will block the event-loop.
	RunnableFunc func(ctx context.Context) error
)

// Run starts handling events on the specified address.
//
// Address should use a scheme prefix and be formatted
// like `tcp://192.168.0.10:9851` or `unix://socket`.
// Valid network schemes:
//
//	tcp   - bind to both IPv4 and IPv6
//	tcp4  - IPv4
//	tcp6  - IPv6
//	udp   - bind to both IPv4 and IPv6
//	udp4  - IPv4
//	udp6  - IPv6
//	unix  - Unix Domain Socket
//
// The "tcp" network scheme is assumed when one is not specified.
func Run(eventHandler EventHandler, protoAddr string, opts ...Option) error {
	listeners, options, err := createListeners([]string{protoAddr}, opts...)
	if err != nil {
		return err
	}
	defer func() {
		for _, ln := range listeners {
			ln.close()
		}
	}()
	return run(eventHandler, listeners, options, []string{protoAddr})
}

// Rotate is like Run but accepts multiple network addresses.
func Rotate(eventHandler EventHandler, addrs []string, opts ...Option) error {
	listeners, options, err := createListeners(addrs, opts...)
	if err != nil {
		return err
	}
	defer func() {
		for _, ln := range listeners {
			ln.close()
		}
	}()
	return run(eventHandler, listeners, options, addrs)
}

func createListeners(addrs []string, opts ...Option) ([]*listener, *Options, error) {
	options := loadOptions(opts...)
	if options.Logger == nil {
		return nil, nil, errorx.ErrNilLogger
	}

	//if options.Logger == nil {
	//	if options.LogPath != "" {
	//		logger, logFlusher, _ = logging.CreateLoggerAsLocalFile(options.LogPath, options.LogLevel)
	//	}
	//	options.Logger = logger
	//} else {
	//	logger = options.Logger
	//	logFlusher = nil
	//}

	options.Logger.Debug("default logging level", zap.String("level", options.Logger.GetLevel().String()))

	// The maximum number of operating system threads that the Go program can use is initially set to 10000,
	// which should also be the maximum number of I/O event-loops locked to OS threads that users can start up.
	if options.LockOSThread && options.NumEventLoop > 10000 {
		options.Logger.Error(
			"too many event-loops under LockOSThread mode, should be less than 10,000 while you are trying to set up",
			zap.Int("event-loops", options.NumEventLoop),
		)
		return nil, nil, errorx.ErrTooManyEventLoopThreads
	}

	if options.EdgeTriggeredIOChunk > 0 {
		options.EdgeTriggeredIO = true
		options.EdgeTriggeredIOChunk = int(pkg.NextPowerOfTwo(options.EdgeTriggeredIOChunk))
	} else if options.EdgeTriggeredIO {
		options.EdgeTriggeredIOChunk = 1 << 20 // 1MB
	}

	rbc := options.ReadBufferCap
	switch {
	case rbc <= 0:
		options.ReadBufferCap = MaxStreamBufferCap
	case rbc <= buffer.RingDefaultBufferSize:
		options.ReadBufferCap = buffer.RingDefaultBufferSize
	default:
		options.ReadBufferCap = int(pkg.NextPowerOfTwo(rbc))
	}

	wbc := options.WriteBufferCap
	switch {
	case wbc <= 0:
		options.WriteBufferCap = MaxStreamBufferCap
	case wbc <= buffer.RingDefaultBufferSize:
		options.WriteBufferCap = buffer.RingDefaultBufferSize
	default:
		options.WriteBufferCap = int(pkg.NextPowerOfTwo(wbc))
	}

	var hasUDP, hasUnix bool
	for _, addr := range addrs {
		proto, _, err := parseProtoAddr(addr)
		if err != nil {
			return nil, nil, err
		}
		hasUDP = hasUDP || strings.HasPrefix(proto, "udp")
		hasUnix = hasUnix || proto == "unix"
	}

	// SO_REUSEPORT enables duplicate address and port bindings across various
	// Unix-like OSs, whereas there is platform-specific inconsistency:
	// Linux implemented SO_REUSEPORT with load balancing for incoming connections
	// while *BSD implemented it for only binding to the same address and port, which
	// makes it pointless to enable SO_REUSEPORT on *BSD and Darwin for gnet with
	// multiple event-loops because only the first or last event-loop will be constantly
	// woken up to accept incoming connections and handle I/O events while the rest of
	// event-loops remain idle.
	// Thus, we disable SO_REUSEPORT on *BSD and Darwin by default.
	//
	// Note that FreeBSD 12 introduced a new socket option named SO_REUSEPORT_LB
	// with the capability of load balancing, it's the equivalent of Linux's SO_REUSEPORT.
	// Also note that DragonFlyBSD 3.6.0 extended SO_REUSEPORT to distribute workload to
	// available sockets, which makes it the same as Linux's SO_REUSEPORT.
	goos := runtime.GOOS
	if options.ReusePort && options.NumEventLoop > 1 &&
		(goos != "linux" && goos != "dragonfly" && goos != "freebsd") {
		options.ReusePort = false
	}

	// Despite the fact that SO_REUSEPORT can be set on a Unix domain socket
	// via setsockopt() without reporting an error, SO_REUSEPORT is actually
	// not supported for sockets of AF_UNIX. Thus, we avoid setting it on the
	// Unix domain sockets.
	// As of this commit https://git.kernel.org/pub/scm/linux/kernel/git/netdev/net.git/commit/?id=5b0af621c3f6,
	// EOPNOTSUPP will be returned when trying to set SO_REUSEPORT on an AF_UNIX socket on Linux. We therefore
	// avoid setting it on Unix domain sockets on all UNIX-like platforms to keep this behavior consistent.
	if options.ReusePort && hasUnix {
		options.ReusePort = false
	}

	// If there is UDP address in the list, we have no choice but to enable SO_REUSEPORT anyway,
	// also disable edge-triggered I/O for UDP by default.
	if hasUDP {
		options.ReusePort = true
		options.EdgeTriggeredIO = false
	}

	listeners := make([]*listener, len(addrs))
	for i, a := range addrs {
		proto, addr, err := parseProtoAddr(a)
		if err != nil {
			return nil, nil, err
		}
		ln, err := initListener(proto, addr, options)
		if err != nil {
			return nil, nil, err
		}
		listeners[i] = ln
	}

	return listeners, options, nil
}

func run(eventHandler EventHandler, listeners []*listener, options *Options, addrs []string) error {
	numEventLoop := determineEventLoops(options)
	maxConns := determineMaxConns(options)
	options.Logger.Info(
		"Launching NIO",
		zap.Int("event-loops", numEventLoop),
		zap.Int("max-conns", maxConns),
		zap.String("listening on", strings.Join(addrs, " | ")),
	)

	lns := swiss.NewTableWithCap[int, *listener](len(listeners), hash.Int)
	for _, ln := range listeners {
		lns.Set(ln.fd, ln)
	}
	rootCtx, shutdown := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(rootCtx)
	eng := engine{
		listeners:    lns,
		opts:         options,
		turnOff:      shutdown,
		eventHandler: eventHandler,
		concurrency: struct {
			*errgroup.Group
			ctx context.Context
		}{eg, ctx},
	}
	switch options.LB {
	case RoundRobin:
		eng.eventLoops = new(roundRobinLoadBalancer)
	case LeastConnections:
		eng.eventLoops = new(leastConnectionsLoadBalancer)
	case SourceAddrHash:
		eng.eventLoops = new(sourceAddrHashLoadBalancer)
	}

	e := Engine{&eng}
	switch eng.eventHandler.OnBoot(e) {
	case None, Close:
	case Shutdown:
		return nil
	default:
		return errorx.ErrUnsupportedOp
	}

	if err := eng.start(ctx, numEventLoop, maxConns); err != nil {
		eng.closeEventLoops()
		eng.opts.Logger.Error("NIO engine is stopping with error", zap.Error(err))
		return err
	}
	defer eng.stop(rootCtx, e)

	for _, addr := range addrs {
		allEngines.Store(addr, &eng)
	}

	return nil
}

func parseProtoAddr(protoAddr string) (string, string, error) {
	// Percent-encode "%" in the address to avoid url.Parse error.
	// This is for cases like this: udp://[ff02::3%lo0]:9991
	protoAddr = strings.ReplaceAll(protoAddr, "%", "%25")

	if runtime.GOOS == "windows" {
		if strings.HasPrefix(protoAddr, "unix://") {
			parts := strings.SplitN(protoAddr, "://", 2)
			if parts[1] == "" {
				return "", "", errorx.ErrInvalidNetworkAddress
			}
			return parts[0], parts[1], nil
		}
	}

	u, err := url.Parse(protoAddr)
	if err != nil {
		return "", "", err
	}

	switch u.Scheme {
	case "":
		return "", "", errorx.ErrInvalidNetworkAddress
	case "tcp", "tcp4", "tcp6", "udp", "udp4", "udp6":
		if u.Host == "" || u.Path != "" {
			return "", "", errorx.ErrInvalidNetworkAddress
		}
		return u.Scheme, u.Host, nil
	case "unix":
		hostPath := path.Join(u.Host, u.Path)
		if hostPath == "" {
			return "", "", errorx.ErrInvalidNetworkAddress
		}
		return u.Scheme, hostPath, nil
	default:
		return "", "", errorx.ErrUnsupportedProtocol
	}
}

func setKeepAlive(fd int, enabled bool, idle, intvl time.Duration, cnt int) error {
	if intvl == 0 {
		intvl = idle / 5
	}
	if cnt == 0 {
		cnt = 5
	}
	return socket.SetKeepAlive(fd, enabled, int(idle.Seconds()), int(intvl.Seconds()), cnt)
}

func determineEventLoops(opts *Options) int {
	numEventLoop := 1
	if opts.NumEventLoop > 0 {
		numEventLoop = opts.NumEventLoop
	}
	return numEventLoop
}

func determineMaxConns(opts *Options) int {
	maxConns := max(opts.MaxConnections, 0)
	return maxConns
}
