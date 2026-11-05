/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package log

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"xxx/pkg/log/zap/buffer"
)

const (
	// isoTimeLayout is the time format of zapcore.ISO8601TimeEncoder. This
	// package builds the drop notice directly. Therefore, the notice must use
	// the same format as each entry from the encoder.
	isoTimeLayout = "2006-01-02T15:04:05.000Z0700"

	// dropReportInterval is the minimum time between two drop notices. Each
	// notice gives a count. Therefore, a longer interval loses no data.
	dropReportInterval = time.Second
)

// asyncSink is a zapcore.BufferWriteSyncer. It sends each encoded entry to
// one writer goroutine. It does not write the entry on the goroutine of the
// caller.
//
// WriteBuffer takes the buffer of the encoder, puts the buffer in a queue of
// a fixed size, and returns. WriteBuffer does not wait and does not fail. If
// the queue is full, WriteBuffer discards the entry and counts it. This
// behavior is the purpose of the type. In a container, stdout is a pipe to
// the log collector. A writing to a full pipe waits, and it stops the
// goroutine that wrote the log. That goroutine can be an event loop.
//
// The sink copies no bytes. The forked core gives the ownership of the buffer
// to the sink, and the sink frees the buffer after the write. Therefore, the
// bytes move from the pool of the encoder to stdout with no copy between
// them.
//
// Only one goroutine is the owner of the bufio.Writer. Therefore, the entries
// need no lock, and the bytes of two entries cannot mix, at any size.
//
// A SIGKILL, an out-of-memory condition or a fatal error of the runtime can
// stop the process before Stop. Then the sink loses the entries in the queue
// and in the buffer. FlushInterval limits the loss from the buffer, and
// QueueSize limits the loss from the queue. Sync empties both, and zap calls
// Sync for DPanic and the levels above DPanic. Therefore, stdout shows the
// entry that explains a Fatal before the process exits.
type asyncSink struct {
	// queue holds each encoded entry to write. The sink is the owner of each
	// buffer in the queue. Exactly one path frees each buffer: the write
	// method, or the discard in WriteBuffer.
	queue chan *buffer.Buffer

	// sync holds each flush barrier to acknowledge. A barrier uses this
	// channel and not the queue. Therefore, a full queue cannot block Sync.
	sync chan chan struct{}

	quit          chan struct{}
	stopped       chan struct{}
	flushInterval time.Duration
	syncTimeout   time.Duration
	now           func() time.Time
	stopOnce      atomic.Bool

	// diagnostics receives the failures of the sink. It must not be the
	// sink. If stdout fails, a report through the queue cannot leave the
	// sink, because the sink cannot empty that queue.
	diagnostics io.Writer

	dropped atomic.Uint64
	errSlot atomic.Pointer[error]

	// The writer goroutine is the owner of the fields below. They need no
	// synchronization.
	bw         *bufio.Writer
	syncFn     func() error
	reported   uint64
	lastReport time.Time
}

// newAsyncSink starts the writer goroutine for dst. The caller must apply
// the defaults to cfg before this call.
func newAsyncSink(
	cfg *Config,
	dst io.Writer,
	diagnostics io.Writer,
	now func() time.Time,
) *asyncSink {
	s := &asyncSink{
		queue:         make(chan *buffer.Buffer, cfg.QueueSize),
		sync:          make(chan chan struct{}),
		quit:          make(chan struct{}),
		stopped:       make(chan struct{}),
		flushInterval: cfg.FlushInterval,
		syncTimeout:   cfg.SyncTimeout,
		now:           now,
		diagnostics:   diagnostics,
		bw:            bufio.NewWriterSize(dst, cfg.BufferSize),
		syncFn:        syncFuncFor(dst),
	}
	go s.loop()
	return s
}

// WriteBuffer puts buf in the queue. The sink is the owner of buf after this
// call, and the sink frees buf one time. The writer goroutine frees buf after
// to write. This method frees buf if the queue is full.
//
// The forked core gives the buffer of the encoder to this method and frees
// nothing. The original core frees the buffer immediately after the write, and
// the encoder then gives the same bytes to the next entry. A queue of the
// bytes alone would therefore show the next entry in the position of this
// entry. The ownership removes that condition, and it also removes the copy.
//
// WriteBuffer always reports success. If the sink returned an error, zap would
// write a diagnostic for each entry after it. The caller can also do nothing
// with that error. setErr reports a failure, and Sync returns it.
func (s *asyncSink) WriteBuffer(buf *buffer.Buffer) error {
	select {
	case s.queue <- buf:
	default:
		buf.Free()
		s.dropped.Add(1)
	}
	return nil
}

// Write puts a copy of p in the queue. It satisfies zapcore.WriteSyncer.
//
// The usual path does not use this method, because the forked core finds
// WriteBuffer and gives the buffer of the encoder. This method serves a caller
// that holds a byte slice and keeps the ownership of that slice. Therefore,
// the copy is necessary here.
func (s *asyncSink) Write(p []byte) (int, error) {
	buf := buffer.Get()
	_, _ = buf.Write(p)
	return len(p), s.WriteBuffer(buf)
}

// Sync writes each entry that is in the queue before this call. Then it
// flushes the buffer. Then it flushes to storage, if the destination
// supports this operation.
//
// zap calls Sync for DPanic and the levels above DPanic. It makes this call
// on the goroutine that wrote the log, before zap panics or exits. That
// goroutine puts the entry in the queue before it sends the barrier.
// Therefore, the queue holds that entry when the writer goroutine takes the
// barrier, and the drain of the queue writes it.
func (s *asyncSink) Sync() error {
	done := make(chan struct{})
	timer := time.NewTimer(s.syncTimeout)
	defer timer.Stop()

	select {
	case s.sync <- done:
	case <-s.stopped:
		// The writer goroutine emptied the queue and exited. No data
		// remains to flush.
		return s.err()
	case <-timer.C:
		return ErrSyncTimeout
	}

	select {
	case <-done:
		return s.err()
	case <-s.stopped:
		return s.err()
	case <-timer.C:
		return ErrSyncTimeout
	}
}

// Stop writes each entry that is in the queue, flushes the data, and waits
// for the writer goroutine to exit. You can call Stop from more than one
// goroutine at the same time.
//
// Stop closes the quit channel one time. Each call then waits by itself.
// Stop does not save the result of the first call.
//   - If the writer goroutine has already exited, a later call returns the
//     same error immediately.
//   - If the first call returned ErrSyncTimeout, a later call waits again
//     for Config.SyncTimeout.
//
// The closeFn from NewLogger saves the result of the first call. Each later
// call of closeFn returns that result. It does not wait again.
// Therefore, the production path calls Stop one time.
func (s *asyncSink) Stop() error {
	if s.stopOnce.CompareAndSwap(false, true) {
		// Emit event to drain remaining logs.
		close(s.quit)
	}

	timer := time.NewTimer(s.syncTimeout)
	defer timer.Stop()

	select {
	case <-s.stopped:
		return s.err()
	case <-timer.C:
		return ErrSyncTimeout
	}
}

// Dropped gives the number of entries that the sink discarded because the
// queue was full. Export this number as a metric. A value that is more than
// zero and that increases shows that the log stream loses data. Then the
// collector or the quantity of logs needs attention.
func (s *asyncSink) Dropped() uint64 {
	return s.dropped.Load()
}

// syncFuncFor gives the function that flushes dst to storage. It gives nil
// if dst has no storage to flush.
//
// Terminals, other character devices, pipes, and sockets do not support
// fsync. This function gets the mode from Stat. It does not use Fd, because
// Fd can change the descriptor to blocking mode.
//
// Also, fsync can return EBADF for a pipe when the writes are correct. Therefore,
// it is safer to test the type of the stream than to test for this error code.
// A test for this error code also hides a descriptor that is not valid.
func syncFuncFor(dst io.Writer) func() error {
	f, ok := dst.(*os.File)
	if !ok {
		// A writer from a test or a net.Conn has no fsync behavior.
		return nil
	}

	info, err := f.Stat()
	if err == nil && info.Mode()&(os.ModeCharDevice|os.ModeNamedPipe|os.ModeSocket) != 0 {
		return nil
	}
	return func() error {
		syncErr := f.Sync()
		if syncErr == nil || isUnsyncable(syncErr) {
			return nil
		}
		return syncErr
	}
}

func (s *asyncSink) loop() {
	defer close(s.stopped)

	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case buf := <-s.queue:
			s.write(buf)

		case done := <-s.sync:
			s.drain(len(s.queue))
			close(done)

		case <-ticker.C:
			s.reportDropped(false)
			s.flush()

		case <-s.quit:
			s.drain(cap(s.queue))
			return
		}
	}
}

// drain writes at most limit entries from the queue. It stops early if the
// queue is empty. Then it flushes the buffer, and it flushes to storage.
//
// drain does not wait for the producers, because each producer must stop
// before Stop. The limit also stops a producer that does not obey this rule.
// Such a producer keeps the queue full, and an unbounded loop then never
// ends. Therefore, Sync and Stop always return.
//
// Each entry goes through write. Therefore, drain also frees each buffer that
// the queue still holds.
func (s *asyncSink) drain(limit int) {
drained:
	for range limit {
		select {
		case buf := <-s.queue:
			s.write(buf)
		default:
			break drained
		}
	}
	s.reportDropped(true)
	s.flush()
	s.syncStorage()
}

func (s *asyncSink) write(buf *buffer.Buffer) {
	// Write the notice before the first entry after the gap. Then the notice
	// is at the position of the loss. In the usual case this test costs one
	// atomic load, on the writer goroutine and not on a producer.
	s.reportDropped(false)

	// A bufio.Writer copies the bytes into its own buffer, or it writes the
	// bytes to stdout. Both operations are complete when Write returns.
	// Therefore, the free below is correct, and it gives the bytes back for
	// the next entry.
	_, err := s.bw.Write(buf.Bytes())
	s.setErr(err)
	buf.Free()
}

func (s *asyncSink) flush() {
	if s.bw.Buffered() == 0 {
		return
	}
	s.setErr(s.bw.Flush())
}

func (s *asyncSink) syncStorage() {
	if s.syncFn != nil {
		s.setErr(s.syncFn())
	}
}

// reportDropped writes a notice for the entries that the sink lost after the
// last notice.
//
// This function builds the notice directly and writes the notice into the
// stream. It does not use the logger. Therefore, a report about a full queue
// adds nothing to that queue. The notice is between the other entries, at
// the position of the gap.
func (s *asyncSink) reportDropped(immediate bool) {
	total := s.dropped.Load()
	if total == s.reported {
		return
	}

	t := s.now()
	if !immediate && !s.lastReport.IsZero() && t.Sub(s.lastReport) < dropReportInterval {
		return
	}

	delta := total - s.reported
	s.reported = total
	s.lastReport = t

	buf := make([]byte, 0, 160)
	buf = append(buf, `{"level":"warn","ts":"`...)
	buf = t.AppendFormat(buf, isoTimeLayout)
	buf = append(buf, `","msg":"log entries dropped","dropped":`...)
	buf = strconv.AppendUint(buf, delta, 10)
	buf = append(buf, `,"dropped_total":`...)
	buf = strconv.AppendUint(buf, total, 10)
	buf = append(buf, "}\n"...)

	_, err := s.bw.Write(buf)
	s.setErr(err)
}

// setErr keeps the first failure and reports it one time.
//
// setErr reports only the first failure. A bufio.Writer keeps its write
// error and fails each call after that error with the same error. A report
// for each call would make one line on stderr for each entry.
func (s *asyncSink) setErr(err error) {
	if err == nil {
		return
	}
	if s.errSlot.CompareAndSwap(nil, &err) {
		_, _ = fmt.Fprintf(s.diagnostics, "logger: stdout sink error: %v\n", err)
	}
}

func (s *asyncSink) err() error {
	err := s.errSlot.Load()
	if err != nil {
		return *err
	}
	return nil
}

// isUnsyncable tells if err is a known error from a sync operation. Such an
// error comes from a file descriptor that is not seekable. Examples are
// stdout and stderr.
//
// On some platforms, an operation like fsync on a stream returns ENOTTY or
// EINVAL. These errors do not show a loss of data from the buffer. This
// function ignores an error only if each part of the error is one of these
// codes. It reports an error that also has a different part.
func isUnsyncable(err error) bool {
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		errs := e.Unwrap()
		if len(errs) == 0 {
			return false
		}
		for _, child := range errs {
			if !isUnsyncable(child) {
				return false
			}
		}
		return true

	case interface{ Unwrap() error }:
		return isUnsyncable(e.Unwrap())
	}

	return errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTTY)
}
