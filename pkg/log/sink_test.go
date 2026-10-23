/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package log

import (
	"bytes"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

type diagnosticBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *diagnosticBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *diagnosticBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type transientFailureSink struct{ calls atomic.Int32 }

func (s *transientFailureSink) Write(p []byte) (int, error) {
	if s.calls.Add(1) == 1 {
		return 0, syscall.ENOSPC
	}
	return len(p), nil
}

func (*transientFailureSink) Sync() error { return nil }

func TestBackgroundFlushReportsAndRetainsFirstWriteError(t *testing.T) {
	var diagnostics diagnosticBuffer
	sink := &transientFailureSink{}
	buffered := &zapcore.BufferedWriteSyncer{
		WS: &reportingWriteSyncer{
			WriteSyncer: sink,
			errorOutput: newRateLimitedWriter(&diagnostics, sinkErrorReportInterval, time.Now),
		},
		Size:          1024,
		FlushInterval: time.Millisecond,
	}
	t.Cleanup(func() { _ = buffered.Stop() })
	_, err := buffered.Write([]byte("pending entry"))
	require.NoError(t, err)

	// Wait for the real background loop, without an explicit Sync call.
	require.Eventually(
		t,
		func() bool {
			return strings.Contains(diagnostics.String(), "file sink error")
		},
		time.Second,
		time.Millisecond,
	)
	require.Contains(t, diagnostics.String(), syscall.ENOSPC.Error())

	// The destination would accept another write, but bufio retains the first
	// failure. No reset is attempted, so pending bytes are not discarded.
	require.ErrorIs(t, buffered.Sync(), syscall.ENOSPC)
	_, err = buffered.Write([]byte("later entry"))
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.ErrorIs(t, buffered.Stop(), syscall.ENOSPC)
	require.EqualValues(t, 1, sink.calls.Load())
	require.Equal(t, 1, strings.Count(diagnostics.String(), "file sink error"))
}

func TestFileSinkDiagnosticsAreRateLimited(t *testing.T) {
	var diagnostics bytes.Buffer
	now := time.Now()
	sink := &reportingWriteSyncer{
		WriteSyncer: syncTestSink{
			writeErr: syscall.ENOSPC,
			syncErr:  syscall.EIO,
		},
		errorOutput: newRateLimitedWriter(
			&diagnostics,
			sinkErrorReportInterval,
			func() time.Time {
				return now
			},
		),
	}
	_, err := sink.Write([]byte("entry"))
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.ErrorIs(t, sink.Sync(), syscall.EIO)
	require.Equal(t, 1, strings.Count(diagnostics.String(), "file sink error"))

	now = now.Add(sinkErrorReportInterval)
	require.ErrorIs(t, sink.Sync(), syscall.EIO)
	require.Equal(t, 2, strings.Count(diagnostics.String(), "file sink error"))
	require.Contains(t, diagnostics.String(), "1 diagnostic message(s) suppressed\n")
}

func TestFileSinkDiagnosticFailureDoesNotReplaceSinkError(t *testing.T) {
	sink := &reportingWriteSyncer{
		WriteSyncer: syncTestSink{writeErr: syscall.ENOSPC, syncErr: syscall.EIO},
		errorOutput: syncTestSink{writeErr: syscall.EPIPE},
	}
	n, err := sink.Write([]byte("entry"))
	require.Zero(t, n)
	require.ErrorIs(t, err, syscall.ENOSPC)
	require.NotErrorIs(t, err, syscall.EPIPE)

	err = sink.Sync()
	require.ErrorIs(t, err, syscall.EIO)
	require.NotErrorIs(t, err, syscall.EPIPE)
}

type shortWriteSink struct{}

func (shortWriteSink) Write(p []byte) (int, error) { return len(p) / 2, nil }
func (shortWriteSink) Sync() error                 { return nil }

func TestFileSinkReportsShortWrite(t *testing.T) {
	var diagnostics bytes.Buffer
	sink := &reportingWriteSyncer{
		WriteSyncer: shortWriteSink{},
		errorOutput: &diagnostics,
	}
	n, err := sink.Write([]byte("entry"))
	require.Equal(t, 2, n)
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Contains(t, diagnostics.String(), io.ErrShortWrite.Error())
}

func TestFileSinkSuccessfulOperationsDoNotReportDiagnostics(t *testing.T) {
	var diagnostics bytes.Buffer
	sink := &reportingWriteSyncer{
		WriteSyncer: syncTestSink{},
		errorOutput: &diagnostics,
	}
	n, err := sink.Write([]byte("entry"))
	require.NoError(t, err)
	require.Equal(t, len("entry"), n)
	require.NoError(t, sink.Sync())
	require.Empty(t, diagnostics.String())
}

func TestRateLimitedWriterReportsSuppressedCountAndResets(t *testing.T) {
	var diagnostics bytes.Buffer
	now := time.Now()
	writer := newRateLimitedWriter(
		&diagnostics,
		sinkErrorReportInterval,
		func() time.Time {
			return now
		},
	)
	write := func(message string) {
		t.Helper()
		n, err := writer.Write([]byte(message))
		require.NoError(t, err)
		require.Equal(t, len(message), n)
	}

	write("first\n")
	write("suppressed one\n")
	now = now.Add(sinkErrorReportInterval - time.Nanosecond)
	write("suppressed two\n")
	require.Equal(t, "first\n", diagnostics.String())

	// The exact interval boundary permits the next diagnostic.
	now = now.Add(time.Nanosecond)
	write("second\n")
	require.Equal(t, "first\nlogger: 2 diagnostic message(s) suppressed\nsecond\n", diagnostics.String())

	now = now.Add(sinkErrorReportInterval)
	write("third\n")
	require.Equal(t, 1, strings.Count(diagnostics.String(), "suppressed\n"))
	require.True(t, strings.HasSuffix(diagnostics.String(), "second\nthird\n"))
}

func TestRateLimitedWriterDiagnosticsAreBestEffort(t *testing.T) {
	for _, tc := range []struct {
		name string
		sink zapcore.WriteSyncer
	}{
		{
			name: "write error",
			sink: syncTestSink{
				writeErr: syscall.EPIPE,
				syncErr:  syscall.EIO,
			},
		},
		{
			name: "short write",
			sink: shortWriteSink{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()
			writer := newRateLimitedWriter(
				tc.sink,
				sinkErrorReportInterval,
				func() time.Time {
					return now
				},
			)
			for i := range 3 {
				if i == 2 {
					// Also exercise a failed suppressed-count report.
					now = now.Add(sinkErrorReportInterval)
				}
				n, err := writer.Write([]byte("diagnostic\n"))
				require.NoError(t, err)
				require.Equal(t, len("diagnostic\n"), n)
			}
			require.NoError(t, writer.Sync())
		})
	}
}

func TestRateLimitedWriterConcurrentWrites(t *testing.T) {
	const writers = 100

	var diagnostics bytes.Buffer
	now := time.Now()
	writer := newRateLimitedWriter(
		&diagnostics,
		sinkErrorReportInterval,
		func() time.Time {
			return now
		},
	)

	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			n, err := writer.Write([]byte("diagnostic\n"))
			if err != nil || n != len("diagnostic\n") {
				t.Errorf("Write() = (%d, %v)", n, err)
			}
		})
	}
	wg.Wait()
	require.Equal(t, "diagnostic\n", diagnostics.String())

	now = now.Add(sinkErrorReportInterval)
	_, err := writer.Write([]byte("next\n"))
	require.NoError(t, err)
	require.Equal(
		t,
		"diagnostic\nlogger: 99 diagnostic message(s) suppressed\nnext\n",
		diagnostics.String(),
	)
}

func TestLoggerSharesLimiterBetweenSinkAndZapDiagnostics(t *testing.T) {
	var diagnostics bytes.Buffer
	now := time.Now()
	logger, closeLogger, err := newLogger(
		&FileConfig{
			Filename:      filepath.Join(t.TempDir(), "test.log"),
			FlushInterval: time.Hour,
		},
		&diagnostics, func() time.Time { return now },
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.ErrorIs(t, closeLogger(), syscall.ENOSPC) })

	// Replace only the destination before the first write; keep the constructor's
	// actual diagnostic wiring to verify that both paths share one limiter.
	reporter, ok := logger.bufferedSyncer.WS.(*reportingWriteSyncer)
	require.True(t, ok)

	reporter.WriteSyncer = syncTestSink{writeErr: syscall.ENOSPC}
	logger.Info("buffered entry")
	require.ErrorIs(t, logger.Sync(), syscall.ENOSPC)

	firstDiagnostic := diagnostics.String()
	require.Contains(t, firstDiagnostic, "file sink error")

	logger.Info("failed entry one")
	logger.Info("failed entry two")
	require.Equal(t, firstDiagnostic, diagnostics.String())

	now = now.Add(sinkErrorReportInterval)
	logger.Info("failed entry three")
	require.Contains(t, diagnostics.String(), "2 diagnostic message(s) suppressed\n")
	require.Equal(t, 1, strings.Count(diagnostics.String(), "write error:"))
	require.Contains(t, diagnostics.String(), syscall.ENOSPC.Error())
}
