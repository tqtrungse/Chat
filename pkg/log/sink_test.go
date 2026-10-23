/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package log

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// syncBuffer is an io.Writer that is safe for concurrent use. A test uses it
// to read back what the writer goroutine wrote.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *syncBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *syncBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// gateWriter blocks each Write call until release closes. This parks the
// writer goroutine of the sink, so a test can fill the queue on purpose.
type gateWriter struct {
	release chan struct{}
	entered chan struct{}
	once    sync.Once

	mu  sync.Mutex
	buf bytes.Buffer
	err error
}

func newGateWriter() *gateWriter {
	return &gateWriter{
		release: make(chan struct{}),
		entered: make(chan struct{}),
	}
}

func (w *gateWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return 0, w.err
	}
	return w.buf.Write(p)
}

func (w *gateWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func (w *gateWriter) unblock() { close(w.release) }

// failingWriter fails every write with a fixed error.
type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

// TestWrite_CopiesPayload is the regression test for the most dangerous
// requirement of the sink. zap reuses the buffer of the encoder as soon as
// Write returns. Therefore, a sink that queued the slice of the caller would
// publish whatever bytes the next entry wrote into that slice.
func TestWrite_CopiesPayload(t *testing.T) {
	var out syncBuffer
	cfg := Config{}
	cfg.applyDefault()
	sink := newAsyncSink(&cfg, &out, io.Discard, time.Now)
	defer func() { _ = sink.Stop() }()

	payload := []byte("first\n")
	_, err := sink.Write(payload)
	require.NoError(t, err)

	// Simulate zap reusing the buffer before the writer goroutine runs.
	copy(payload, []byte("SECON"))

	require.NoError(t, sink.Sync())
	assert.Equal(t, "first\n", out.String())
}

func TestWrite_PreservesOrder(t *testing.T) {
	var out syncBuffer
	cfg := Config{}
	cfg.applyDefault()
	sink := newAsyncSink(&cfg, &out, io.Discard, time.Now)
	defer func() { _ = sink.Stop() }()

	const n = 500
	for i := range n {
		_, err := sink.Write([]byte(fmt.Sprintf("%d\n", i)))
		require.NoError(t, err)
	}
	require.NoError(t, sink.Sync())

	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	require.Len(t, lines, n)
	for i := range n {
		require.Equal(t, fmt.Sprintf("%d", i), lines[i], "entries must stay in queue order")
	}
}

// TestWrite_DropsWhenQueueFull parks the writer goroutine, fills the queue
// past its limit, and checks that the producer does not block and does not
// lose the count.
func TestWrite_DropsWhenQueueFull(t *testing.T) {
	gate := newGateWriter()
	cfg := Config{
		QueueSize:  1,
		BufferSize: minBufferSize,
	}
	cfg.applyDefault()
	sink := newAsyncSink(&cfg, gate, io.Discard, time.Now)

	// The first entry reaches the destination and parks there. This leaves
	// the queue free to fill behind it.
	_, err := sink.Write([]byte("blocking entry that exceeds the buffer\n"))
	require.NoError(t, err)
	<-gate.entered

	const extra = 50
	for i := range extra {
		// Each of these calls must return at once. None must wait for the
		// parked writer goroutine.
		_, writeErr := sink.Write([]byte(fmt.Sprintf("entry %d\n", i)))
		require.NoError(t, writeErr)
	}

	assert.Positive(t, sink.Dropped(), "a full queue must drop and count entries")
	assert.Less(t, sink.Dropped(), uint64(extra+1), "entries that fit must not be dropped")

	gate.unblock()
	require.NoError(t, sink.Stop())
}

// TestReportDropped_EmitsNoticeInline checks that the gap is marked in the
// stream itself, as valid JSON. The sink does not log this notice through
// itself.
func TestReportDropped_EmitsNoticeInline(t *testing.T) {
	var out syncBuffer
	cfg := Config{}
	cfg.applyDefault()
	sink := newAsyncSink(&cfg, &out, io.Discard, time.Now)

	sink.dropped.Store(7)
	_, err := sink.Write([]byte("after the gap\n"))
	require.NoError(t, err)
	require.NoError(t, sink.Sync())
	require.NoError(t, sink.Stop())

	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	require.NotEmpty(t, lines)

	var notice map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &notice), "the drop notice must be valid JSON")
	assert.Equal(t, "warn", notice["level"])
	assert.Equal(t, "log entries dropped", notice["msg"])
	assert.EqualValues(t, 7, notice["dropped"])
	assert.EqualValues(t, 7, notice["dropped_total"])

	ts, ok := notice["ts"].(string)
	require.True(t, ok)
	_, err = time.Parse(isoTimeLayout, ts)
	assert.NoError(t, err, "the notice timestamp must use the same layout as the encoder")
}

func TestReportDropped_RateLimitedBetweenFlushes(t *testing.T) {
	var out syncBuffer

	now := time.Now()
	clock := func() time.Time { return now }

	cfg := Config{FlushInterval: time.Millisecond}
	cfg.applyDefault()
	sink := newAsyncSink(&cfg, &out, io.Discard, clock)

	// Several background flushes happen while the clock stands still.
	// Therefore, the interval check must collapse them into one notice.
	sink.dropped.Store(3)
	time.Sleep(20 * time.Millisecond)

	require.NoError(t, sink.Stop())
	assert.Equal(t, 1, strings.Count(out.String(), `"msg":"log entries dropped"`))
}

// TestSync_DrainsEverythingQueuedBefore covers the barrier. Sync must not
// return while an entry queued before it is still in flight.
func TestSync_DrainsEverythingQueuedBefore(t *testing.T) {
	var out syncBuffer
	cfg := &Config{FlushInterval: time.Hour}
	cfg.applyDefault()
	sink := newAsyncSink(cfg, &out, io.Discard, time.Now)
	defer func() { _ = sink.Stop() }()

	const n = 200
	for i := range n {
		_, err := sink.Write([]byte(fmt.Sprintf("%d\n", i)))
		require.NoError(t, err)
	}

	require.NoError(t, sink.Sync())
	assert.Equal(
		t,
		n,
		strings.Count(out.String(), "\n"),
		"Sync must flush every entry queued before it",
	)
}

func TestSync_TimesOutOnWedgedDestination(t *testing.T) {
	gate := newGateWriter()
	cfg := Config{
		QueueSize:   1,
		BufferSize:  minBufferSize,
		SyncTimeout: 50 * time.Millisecond,
	}
	cfg.applyDefault()
	sink := newAsyncSink(&cfg, gate, io.Discard, time.Now)

	_, err := sink.Write([]byte("an entry long enough to bypass the buffer\n"))
	require.NoError(t, err)
	<-gate.entered

	start := time.Now()
	assert.ErrorIs(t, sink.Sync(), ErrSyncTimeout)
	assert.Less(t, time.Since(start), time.Second, "Sync must give up rather than hang")

	gate.unblock()
	require.NoError(t, sink.Stop())
}

func TestSink_ReportsFirstWriteErrorOnce(t *testing.T) {
	var diagnostics syncBuffer
	sentinel := errors.New("disk is unhappy")

	cfg := &Config{BufferSize: minBufferSize}
	cfg.applyDefault()
	sink := newAsyncSink(cfg, failingWriter{err: sentinel}, &diagnostics, time.Now)

	for range 5 {
		_, err := sink.Write([]byte("an entry long enough to bypass the buffer\n"))
		require.NoError(t, err, "Write must stay successful even when the destination fails")
	}
	_ = sink.Sync()

	assert.ErrorIs(t, sink.Stop(), sentinel, "the first failure must be retained")
	assert.Equal(
		t,
		1,
		strings.Count(diagnostics.String(), "stdout sink error"),
		"a broken destination must not produce one diagnostic per entry",
	)
}

func TestStop_IsIdempotentAndStopsWriterGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()

	var out syncBuffer
	cfg := Config{}
	cfg.applyDefault()
	sink := newAsyncSink(&cfg, &out, io.Discard, time.Now)

	_, err := sink.Write([]byte("entry\n"))
	require.NoError(t, err)

	require.NoError(t, sink.Stop())
	require.NoError(t, sink.Stop(), "Stop must be idempotent")
	assert.Equal(t, "entry\n", out.String())

	// The writer goroutine owns the only reference to the destination.
	// Therefore, a leak here would keep writing after shutdown.
	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	assert.LessOrEqual(t, runtime.NumGoroutine(), before, "the writer goroutine must exit")
}

// TestWrite_AfterStopDoesNotPanic documents the shutdown contract. A log
// call after close is not supported, but the call must fail quietly. The
// call must not panic on a closed channel.
func TestWrite_AfterStopDoesNotPanic(t *testing.T) {
	var out syncBuffer
	cfg := Config{QueueSize: 1}
	cfg.applyDefault()
	sink := newAsyncSink(&cfg, &out, io.Discard, time.Now)
	require.NoError(t, sink.Stop())

	assert.NotPanics(t, func() {
		for range 10 {
			_, _ = sink.Write([]byte("after stop\n"))
		}
	})
	assert.NoError(t, sink.Sync(), "Sync after Stop must return the sink's error, not hang")
}

func TestSyncFuncFor(t *testing.T) {
	assert.Nil(t, syncFuncFor(io.Discard), "a plain writer has no fsync semantics")

	r, w, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = r.Close()
		_ = w.Close()
	})
	assert.Nil(t, syncFuncFor(w), "a pipe must not be fsynced")

	f, err := os.CreateTemp(t.TempDir(), "sink")
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	require.NotNil(t, syncFuncFor(f), "a regular file must be fsynced")
	assert.NoError(t, syncFuncFor(f)())
}

func TestIsUnsyncable(t *testing.T) {
	assert.True(t, isUnsyncable(syscall.ENOTTY))
	assert.True(t, isUnsyncable(syscall.EINVAL))
	assert.True(t, isUnsyncable(fmt.Errorf("wrapped: %w", syscall.ENOTTY)))
	assert.True(t, isUnsyncable(errors.Join(syscall.EINVAL, syscall.ENOTTY)))
	assert.False(t, isUnsyncable(nil))
	assert.False(t, isUnsyncable(errors.New("some other error")))
	// A mixed error still indicates a real failure.
	assert.False(t, isUnsyncable(errors.Join(syscall.ENOTTY, errors.New("real failure"))))
	assert.False(t, isUnsyncable(syscall.EBADF), "a bad descriptor must still be reported")
}

// TestSampling_ThinsLowLevelsButNeverError is the regression test for the
// split between the two cores. The sampler of zapcore does not see the
// level. Therefore, a sampler around the whole core would thin out the exact
// entries that an incident needs.
func TestSampling_ThinsLowLevelsButNeverError(t *testing.T) {
	logger, closeFn, path := newTestLogger(
		t,
		&Config{
			DisableSampling:  false,
			SampleFirst:      2,
			SampleThereafter: 1_000_000,
		},
	)

	const n = 50
	for range n {
		logger.Info("repeated message")
	}
	for range n {
		logger.Error("repeated failure")
	}

	require.NoError(t, logger.Sync())
	require.NoError(t, closeFn())

	var infos, errorsSeen int
	for _, entry := range readLogLines(t, path) {
		switch entry["level"] {
		case "info":
			infos++
		case "error":
			errorsSeen++
		}
	}

	assert.Less(t, infos, n, "repeated Info entries must be sampled")
	assert.Positive(t, infos)
	assert.Equal(t, n, errorsSeen, "Error entries must never be sampled")
}

// TestFatal_FlushesBeforeExit runs in a child process, because the Fatal
// method of zap calls os.Exit. This is the only way to prove that the
// asynchronous sink does not lose the entry that explains why the process
// died.
func TestFatal_FlushesBeforeExit(t *testing.T) {
	const childEnv = "LOG_TEST_FATAL_CHILD"

	if os.Getenv(childEnv) == "1" {
		logger, closeFn, err := NewLogger(Config{DisableSampling: true})
		if err != nil {
			os.Exit(97)
		}
		logger.Fatal("fatal entry", zap.String("reason", "deliberate"))
		_ = closeFn()
		os.Exit(98) // This line does not run. Fatal exits the process.
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestFatal_FlushesBeforeExit$", "-test.v")
	cmd.Env = append(os.Environ(), childEnv+"=1")

	out, err := cmd.Output()
	require.Error(t, err, "the child must exit non-zero")

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, 1, exitErr.ExitCode(), "zap's Fatal must exit with status 1")

	assert.Contains(
		t,
		string(out),
		`"msg":"fatal entry"`,
		"the Fatal entry must reach stdout before the process exits",
	)
	assert.Contains(t, string(out), `"reason":"deliberate"`)
}

// TestDPanic_FlushesThroughTheBarrier covers the same path as Fatal, but it
// does not exit the process. zapcore calls Sync for each level above Error.
func TestDPanic_FlushesThroughTheBarrier(t *testing.T) {
	logger, _, path := newTestLogger(t, &Config{FlushInterval: time.Hour})

	// DevelopmentMode is off. Therefore, DPanic writes a log entry, and it
	// does not panic.
	logger.DPanic("about to go wrong")

	// This test makes no call to Sync. zapcore must sync because of the
	// level of the entry itself.
	entries := readLogLines(t, path)
	require.Len(t, entries, 1)
	assert.Equal(t, "dpanic", entries[0]["level"])
}

func TestLogger_SyncReportsSinkFailure(t *testing.T) {
	sentinel := errors.New("destination gone")
	logger, closeFn, err := newLogger(
		&Config{
			BufferSize:      minBufferSize,
			DisableSampling: true,
		},
		failingWriter{err: sentinel},
		io.Discard,
		time.Now,
	)
	require.NoError(t, err)

	logger.Info("an entry long enough to bypass the buffer and reach the destination")
	_ = logger.Sync()

	assert.ErrorIs(t, closeFn(), sentinel)
}
