/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package log

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// readLogLines reads path and unmarshals every non-empty line as a JSON log
// entry. It fails the test immediately if the file can't be read or a line
// isn't valid JSON, since every entry produced by the zap JSON encoder must
// parse.
func readLogLines(t *testing.T, path string) []map[string]any {
	t.Helper()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	var entries []map[string]any
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var entry map[string]any
		require.NoErrorf(t, json.Unmarshal([]byte(line), &entry), "line: %s", line)
		entries = append(entries, entry)
	}
	require.NoError(t, scanner.Err())
	return entries
}

// newFileLogger is a small helper that builds a Logger writing to a fresh
// file under t.TempDir(), and returns both the Logger and the log path.
func newFileLogger(t *testing.T, cfg *FileConfig) (*Logger, func() error, string) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	full := &FileConfig{
		Filename:      path,
		MaxSize:       cfg.MaxSize,
		MaxBackups:    cfg.MaxBackups,
		MaxAge:        cfg.MaxAge,
		Compress:      cfg.Compress,
		BufferSize:    cfg.BufferSize,
		FlushInterval: cfg.FlushInterval,
	}
	full.Load(nil)
	logger, closeFunc, err := NewLogger(full)
	require.NoError(t, err)
	return logger, closeFunc, path
}

func TestNewLoggerNormalizesConfigWithoutLoad(t *testing.T) {
	for _, tc := range []struct {
		name          string
		maxSize       int
		bufferSize    int
		flushInterval time.Duration
	}{
		{
			name:          "zero values",
			maxSize:       0,
			bufferSize:    0,
			flushInterval: 0,
		},
		{
			name:          "negative values",
			maxSize:       -1,
			bufferSize:    -1,
			flushInterval: -time.Second,
		},
		{
			name:          "explicit buffer with default max size",
			maxSize:       0,
			bufferSize:    1024,
			flushInterval: 0,
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				cfg := &FileConfig{
					Filename:      filepath.Join(t.TempDir(), "test.log"),
					MaxSize:       tc.maxSize,
					BufferSize:    tc.bufferSize,
					FlushInterval: tc.flushInterval,
				}
				original := *cfg
				logger, closeLogger, err := NewLogger(cfg)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, closeLogger()) })
				require.Equal(t, original, *cfg, "constructor must not mutate caller's config")
				require.Equal(t, defaultMaxSize, logger.lumberjack.MaxSize)

				expectedBufferSize := tc.bufferSize
				if expectedBufferSize <= 0 {
					expectedBufferSize = defaultBufferSize
				}
				require.Equal(t, expectedBufferSize, logger.bufferedSyncer.Size)
				require.Equal(t, defaultFlushInterval, logger.bufferedSyncer.FlushInterval)
				require.NotPanics(t, func() { logger.Info("first write") })
				require.NoError(t, logger.Sync())
				require.Len(t, readLogLines(t, cfg.Filename), 1)
			},
		)
	}
}

func TestNewLogger_NilConfig_UsesStdout(t *testing.T) {
	logger, closeFunc, err := NewLogger(nil)
	require.NoError(t, err)
	require.NotNil(t, logger)

	assert.Nil(t, logger.lumberjack, "no lumberjack writer should be created when logging to stdout")
	assert.Nil(t, logger.bufferedSyncer, "stdout must not be buffered")
	assert.Equal(t, zapcore.InfoLevel, logger.GetLevel(), "default level should be Info")

	// The stdout sink handles unsupported fsync operations separately from
	// writes; closing an unused logger must not surface an error.
	assert.NoError(t, closeFunc())
}

func TestDPanicAndHigherFlushBufferedEntries(t *testing.T) {
	for _, level := range []string{
		"dpanic",
		"panic",
		"fatal",
	} {
		t.Run(
			level,
			func(t *testing.T) {
				logger, closeLogger, path := newFileLogger(t, &FileConfig{
					BufferSize:    64 * 1024,
					FlushInterval: time.Hour,
				})
				t.Cleanup(func() { require.NoError(t, closeLogger()) })
				syncer := &countingSyncer{WriteSyncer: logger.bufferedSyncer.WS}
				logger.bufferedSyncer.WS = syncer
				logger.Info("before failure")
				logger.Warn("last warning")

				switch level {
				case "dpanic":
					logger.DPanic("failure")

				case "panic":
					require.Panics(t, func() { logger.Panic("failure") })

				case "fatal":
					// Exercise Fatal's write path without terminating the test process.
					logger.logger = logger.logger.WithOptions(zap.WithFatalHook(zapcore.WriteThenGoexit))
					done := make(chan struct{})
					go func() {
						defer close(done)
						logger.Fatal("failure")
					}()
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Fatal("Fatal did not finish")
					}
				}

				require.EqualValues(
					t,
					1,
					syncer.syncCalls.Load(),
					"zap must sync the failure exactly once",
				)
				// Inspect before Sync or close: the failure must flush its context.
				entries := readLogLines(t, path)
				require.Len(t, entries, 3)
				require.Equal(t, "before failure", entries[0]["msg"])
				require.Equal(t, "last warning", entries[1]["msg"])
				require.Equal(t, level, entries[2]["level"])
				require.Equal(t, "failure", entries[2]["msg"])
			},
		)
	}
}

func TestNewLogger_FileConfig_WritesExpectedFields(t *testing.T) {
	logger, closeFunc, path := newFileLogger(t, &FileConfig{MaxSize: 1})
	require.NotNil(t, logger)
	require.NotNil(
		t,
		logger.lumberjack,
		"a lumberjack writer should be created when a file is configured",
	)

	logger.Info("hello world", zap.String("key", "value"), zap.Int("count", 3))
	require.NoError(t, logger.Sync())

	entries := readLogLines(t, path)
	require.Len(t, entries, 1)

	entry := entries[0]
	assert.Equal(t, "info", entry["level"])
	assert.Equal(t, "hello world", entry["msg"])
	assert.Equal(t, "value", entry["key"])
	assert.EqualValues(t, 3, entry["count"])
	assert.NotEmpty(t, entry["ts"], "ISO8601 timestamp should be present")
	assert.NotEmpty(t, entry["caller"], "AddCaller should populate the caller field")

	require.NoError(t, closeFunc())
}

func TestCallerSkip_ReportsCallSiteNotWrapper(t *testing.T) {
	logger, closeFunc, path := newFileLogger(t, &FileConfig{})

	logger.Info("caller check") // the reported caller must point at this file, not logger.go
	require.NoError(t, logger.Sync())
	require.NoError(t, closeFunc())

	entries := readLogLines(t, path)
	require.Len(t, entries, 1)

	caller, ok := entries[0]["caller"].(string)
	require.True(t, ok)
	// AddCallerSkip(1) should skip past the Logger.Info wrapper method, so
	// the reported caller is this test file, not logger.go.
	assert.Truef(
		t,
		strings.Contains(caller, "logger_test.go"),
		"expected caller to point at the test file, got %q", caller,
	)
	assert.Falsef(
		t,
		strings.Contains(caller, "logger.go"),
		"caller should not point at the Logger wrapper itself, got %q", caller,
	)
}

func TestSetLevel_GetLevel(t *testing.T) {
	logger, closeFunc, err := NewLogger(nil)
	require.NoError(t, err)
	defer func() { _ = closeFunc() }()

	assert.Equal(t, zapcore.InfoLevel, logger.GetLevel())

	logger.SetLevel(zapcore.DebugLevel)
	assert.Equal(t, zapcore.DebugLevel, logger.GetLevel())

	logger.SetLevel(zapcore.ErrorLevel)
	assert.Equal(t, zapcore.ErrorLevel, logger.GetLevel())
}

func TestLogging_RespectsConfiguredLevel(t *testing.T) {
	logger, closeFunc, path := newFileLogger(t, &FileConfig{})
	logger.SetLevel(zapcore.WarnLevel)

	logger.Debug("debug msg")
	logger.Info("info msg")
	logger.Warn("warn msg")
	logger.Error("error msg")

	require.NoError(t, logger.Sync())
	require.NoError(t, closeFunc())

	entries := readLogLines(t, path)
	require.Len(t, entries, 2, "only Warn and Error should pass the Warn level filter")
	assert.Equal(t, "warn", entries[0]["level"])
	assert.Equal(t, "error", entries[1]["level"])
}

func TestSync_FlushesWithoutStoppingLogger(t *testing.T) {
	logger, closeFunc, path := newFileLogger(t, &FileConfig{})

	logger.Info("first")
	require.NoError(t, logger.Sync())

	// The logger must still be usable after Sync, unlike after Close.
	logger.Info("second")
	require.NoError(t, logger.Sync())

	entries := readLogLines(t, path)
	require.Len(t, entries, 2)
	assert.Equal(t, "first", entries[0]["msg"])
	assert.Equal(t, "second", entries[1]["msg"])

	require.NoError(t, closeFunc())
}

func TestClose_FlushesBufferedEntriesEvenWithoutExplicitSync(t *testing.T) {
	// Use a large flush interval and a large buffer so nothing would reach
	// disk on its own before Close runs; this isolates Close's own flush
	// behavior from the background flush loop.
	logger, closeFunc, path := newFileLogger(t, &FileConfig{
		BufferSize:    64 * 1024,
		FlushInterval: time.Hour,
	})

	logger.Info("buffered before close")
	require.NoError(t, closeFunc())

	entries := readLogLines(t, path)
	require.Len(t, entries, 1)
	assert.Equal(t, "buffered before close", entries[0]["msg"])
}

func TestClose_IsIdempotent(t *testing.T) {
	logger, closeFunc, _ := newFileLogger(t, &FileConfig{})
	logger.Info("one entry")

	err1 := closeFunc()
	err2 := closeFunc()
	assert.NoError(t, err1)
	assert.Equal(t, err1, err2, "repeated Close calls must return the same result")
}

func TestClose_IsSafeForConcurrentUse(t *testing.T) {
	logger, closeFunc, _ := newFileLogger(t, &FileConfig{})
	logger.Info("one entry")

	const goroutines = 20
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = closeFunc()
		}(i)
	}
	wg.Wait()

	for i := 1; i < goroutines; i++ {
		assert.Equal(t, errs[0], errs[i], "all concurrent Close calls must observe the same result")
	}
}

func TestConcurrentLogging_NoRace(t *testing.T) {
	logger, closeFunc, path := newFileLogger(t, &FileConfig{})

	const goroutines = 8
	const perGoroutine = 50

	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := range perGoroutine {
				logger.Info("concurrent", zap.Int("goroutine", id), zap.Int("i", j))
				if j%10 == 0 {
					logger.SetLevel(logger.GetLevel())
				}
			}
		}(i)
	}
	wg.Wait()

	require.NoError(t, logger.Sync())
	require.NoError(t, closeFunc())

	entries := readLogLines(t, path)
	assert.Len(t, entries, goroutines*perGoroutine)
}

func TestNewLogger_BufferDefaults_LoadedBeforeConstruction(t *testing.T) {
	logger, closeFunc, _ := newFileLogger(t, &FileConfig{})
	defer func() { _ = closeFunc() }()

	assert.Equal(t, 256*1024, logger.bufferedSyncer.Size)
	assert.Equal(t, 30*time.Second, logger.bufferedSyncer.FlushInterval)
}

func TestNewLogger_BufferConfig_PassedThroughWhenSet(t *testing.T) {
	logger, closeFunc, _ := newFileLogger(t, &FileConfig{
		BufferSize:    4096,
		FlushInterval: 5 * time.Second,
	})
	defer func() { _ = closeFunc() }()

	assert.Equal(t, 4096, logger.bufferedSyncer.Size)
	assert.Equal(t, 5*time.Second, logger.bufferedSyncer.FlushInterval)
}

func TestNewLogger_LumberjackConfig_PassedThrough(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.log")

	cfg := &FileConfig{
		Filename:   path,
		MaxSize:    7,
		MaxBackups: 3,
		MaxAge:     14,
		Compress:   true,
	}
	cfg.Load(nil)
	logger, closeFunc, err := NewLogger(cfg)
	require.NoError(t, err)
	defer func() { _ = closeFunc() }()

	require.NotNil(t, logger.lumberjack)
	assert.Equal(t, path, logger.lumberjack.Filename)
	assert.Equal(t, 7, logger.lumberjack.MaxSize)
	assert.Equal(t, 3, logger.lumberjack.MaxBackups)
	assert.Equal(t, 14, logger.lumberjack.MaxAge)
	assert.True(t, logger.lumberjack.Compress)
}

func TestIsUnsyncable(t *testing.T) {
	assert.True(t, isUnsyncable(syscall.ENOTTY))
	assert.True(t, isUnsyncable(syscall.EINVAL))
	assert.True(t, isUnsyncable(&os.PathError{Op: "sync", Path: "stdout", Err: syscall.ENOTTY}))
	assert.True(t, isUnsyncable(errors.Join(syscall.EINVAL, syscall.ENOTTY)))
	assert.False(t, isUnsyncable(errors.Join(syscall.EIO, syscall.ENOTTY)))
	assert.False(t, isUnsyncable(syscall.EBADF))
	assert.False(t, isUnsyncable(nil))
	assert.False(t, isUnsyncable(errors.New("some other error")))
}

func TestErrorRemainsBufferedUntilSync(t *testing.T) {
	logger, closeLogger, path := newFileLogger(
		t,
		&FileConfig{
			BufferSize:    64 * 1024,
			FlushInterval: time.Hour,
		},
	)
	t.Cleanup(func() { require.NoError(t, closeLogger()) })

	logger.Info("before error")
	logger.Error("failure")
	require.Empty(t, readLogLines(t, path), "file entries must remain buffered")
	require.NoError(t, logger.Sync())

	entries := readLogLines(t, path)
	require.Len(t, entries, 2)
	require.Equal(t, "before error", entries[0]["msg"])
	require.Equal(t, "error", entries[1]["level"])
}

func TestNewLoggerBufferSizeValidation(t *testing.T) {
	for _, tt := range []struct {
		name       string
		bufferSize int
		wantErr    bool
	}{
		{
			name:       "default buffer",
			bufferSize: 256 * 1024,
			wantErr:    false,
		},
		{
			name:       "equal to rotation size",
			bufferSize: 1024 * 1024,
			wantErr:    false,
		},
		{
			name:       "one byte over rotation size",
			bufferSize: 1024*1024 + 1,
			wantErr:    true,
		},
	} {
		t.Run(
			tt.name,
			func(t *testing.T) {
				cfg := FileConfig{
					Filename:   filepath.Join(t.TempDir(), "app.log"),
					MaxSize:    1,
					BufferSize: tt.bufferSize,
				}
				cfg.Load(nil)

				logger, closeLogger, err := NewLogger(&cfg)
				if tt.wantErr {
					require.ErrorIs(t, err, ErrBufferSizeOverMaxSize)
					require.Nil(t, logger)
					require.Nil(t, closeLogger)
					return
				}
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, closeLogger()) })

				logger.Info("valid buffer")
				require.NoError(t, logger.Sync())
			},
		)
	}
}

func TestBufferedLoggingRotatesWithoutLosingEntries(t *testing.T) {
	const count = 140

	logger, closeLogger, path := newFileLogger(t, &FileConfig{
		MaxSize:       1,
		BufferSize:    64 * 1024,
		FlushInterval: time.Hour,
	})
	t.Cleanup(func() { require.NoError(t, closeLogger()) })

	payload := strings.Repeat("x", 8*1024)
	for i := range count {
		logger.Info("rotation entry", zap.Int("id", i), zap.String("payload", payload))
	}
	require.NoError(t, closeLogger())

	paths, err := filepath.Glob(filepath.Join(filepath.Dir(path), "*.log"))
	require.NoError(t, err)
	require.Greater(t, len(paths), 1, "entries must exceed the rotation size")

	seen := make(map[int]bool, count)
	for _, p := range paths {
		for _, entry := range readLogLines(t, p) {
			id := int(entry["id"].(float64))
			require.False(t, seen[id], "duplicate entry %d", id)

			seen[id] = true
			require.Equal(t, payload, entry["payload"])
		}
	}

	for i := range count {
		require.True(t, seen[i], "missing entry %d", i)
	}
}

type syncTestSink struct {
	writeErr error
	syncErr  error
}

func (s syncTestSink) Write(p []byte) (int, error) {
	if s.writeErr != nil {
		return 0, s.writeErr
	}
	return len(p), nil
}

func (s syncTestSink) Sync() error { return s.syncErr }

func TestSyncAndClosePreserveWriteErrors(t *testing.T) {
	for _, tt := range []struct {
		name     string
		writeErr error
		syncErr  error
	}{
		{
			name:     "broken pipe",
			writeErr: syscall.EPIPE,
			syncErr:  syscall.EINVAL,
		},
		{
			name:     "io error",
			writeErr: syscall.EIO,
			syncErr:  syscall.ENOTTY,
		},
		{
			name:     "invalid write",
			writeErr: syscall.EINVAL,
			syncErr:  syscall.EINVAL,
		},
		{
			name:     "bad descriptor",
			writeErr: syscall.EBADF,
			syncErr:  syscall.EINVAL,
		},
	} {
		t.Run(
			tt.name,
			func(t *testing.T) {
				buffered := &zapcore.BufferedWriteSyncer{
					WS: stdoutWriteSyncer{syncTestSink{
						writeErr: tt.writeErr,
						syncErr:  tt.syncErr,
					}},
					Size:          1024,
					FlushInterval: time.Hour,
				}
				logger := &Logger{syncer: buffered, bufferedSyncer: buffered}
				t.Cleanup(func() { _ = logger.close() })
				_, err := logger.bufferedSyncer.Write([]byte("buffered entry"))
				require.NoError(t, err)
				require.ErrorIs(t, logger.Sync(), tt.writeErr)

				closeErr := logger.close()
				require.ErrorIs(t, closeErr, tt.writeErr)
				require.Equal(t, closeErr, logger.close())
			},
		)
	}
}

func TestStdoutSyncFiltersOnlyUnsupportedSyncErrors(t *testing.T) {
	for _, tt := range []struct {
		name    string
		err     error
		wantErr error
	}{
		{
			name:    "success",
			err:     nil,
			wantErr: nil,
		},
		{
			name:    "invalid sync",
			err:     syscall.EINVAL,
			wantErr: nil,
		},
		{
			name:    "terminal sync",
			err:     syscall.ENOTTY,
			wantErr: nil,
		},
		{
			name:    "wrapped unsupported sync",
			err:     fmt.Errorf("sync: %w", syscall.EINVAL),
			wantErr: nil,
		},
		{
			name:    "bad descriptor",
			err:     syscall.EBADF,
			wantErr: syscall.EBADF},
		{
			name:    "io error",
			err:     syscall.EIO,
			wantErr: syscall.EIO,
		},
		{
			name:    "mixed errors",
			err:     fmt.Errorf("sync: %w", errors.Join(syscall.EIO, syscall.EINVAL)),
			wantErr: syscall.EIO,
		},
	} {
		t.Run(
			tt.name,
			func(t *testing.T) {
				ws := stdoutWriteSyncer{syncTestSink{syncErr: tt.err}}
				if tt.wantErr == nil {
					require.NoError(t, ws.Sync())
				} else {
					require.ErrorIs(t, ws.Sync(), tt.wantErr)
				}
			},
		)
	}
}

func TestStdoutPipeSync(t *testing.T) {
	for _, broken := range []bool{false, true} {
		t.Run(
			fmt.Sprintf("broken=%v", broken),
			func(t *testing.T) {
				reader, writer, err := os.Pipe()
				require.NoError(t, err)

				t.Cleanup(func() { _ = reader.Close() })
				t.Cleanup(func() { _ = writer.Close() })

				original := os.Stdout
				os.Stdout = writer
				logger, closeLogger, err := NewLogger(nil)
				os.Stdout = original
				require.NoError(t, err)

				t.Cleanup(func() { _ = closeLogger() })

				if broken {
					require.NoError(t, reader.Close())
				}

				var diagnostics bytes.Buffer
				logger.logger = logger.logger.WithOptions(zap.ErrorOutput(zapcore.AddSync(&diagnostics)))
				logger.Info("pipe entry")
				if broken {
					// Writes happen immediately; zap reports their errors directly.
					require.Contains(t, diagnostics.String(), "write error")
					require.NoError(t, logger.Sync())
					require.NoError(t, closeLogger())
					return
				}

				require.NoError(t, reader.SetReadDeadline(time.Now().Add(time.Second)))
				out := make([]byte, 1024)
				n, err := reader.Read(out)
				require.NoError(t, err)
				require.Contains(t, string(out[:n]), "pipe entry")
				require.Empty(t, diagnostics.String())
				require.NoError(t, logger.Sync())
				require.NoError(t, closeLogger())
			},
		)
	}
}

type countingSyncer struct {
	zapcore.WriteSyncer
	syncCalls atomic.Int32
}

func (s *countingSyncer) Sync() error {
	s.syncCalls.Add(1)
	return s.WriteSyncer.Sync()
}

func TestStdoutWritesImmediatelyAndRemainsOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stdout.log")
	file, err := os.Create(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })

	original := os.Stdout
	os.Stdout = file
	logger, closeLogger, err := NewLogger(nil)
	os.Stdout = original
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeLogger()) })
	logger.Info("immediate entry")

	// Inspect before Sync or closeFn: stdout entries must already be written.
	entries := readLogLines(t, path)
	require.Len(t, entries, 1)
	require.Equal(t, "immediate entry", entries[0]["msg"])
	require.NoError(t, closeLogger())
	_, err = file.WriteString("still open\n")
	require.NoError(t, err)
}

func TestNewLoggerChecksFileAtConstruction(t *testing.T) {
	for _, tt := range []struct {
		name    string
		prepare func(t *testing.T) string
		wantErr bool
	}{
		{
			name: "creates parent directories and file",
			prepare: func(t *testing.T) string {
				return filepath.Join(t.TempDir(), "nested", "logs", "app.log")
			},
			wantErr: false,
		},
		{
			name: "parent is a file",
			prepare: func(t *testing.T) string {
				parent := filepath.Join(t.TempDir(), "parent")
				require.NoError(t, os.WriteFile(parent, []byte("occupied"), 0o644))
				return filepath.Join(parent, "app.log")
			},
			wantErr: true,
		},
		{
			name: "filename is a directory",
			prepare: func(t *testing.T) string {
				return t.TempDir()
			},
			wantErr: true,
		},
	} {
		t.Run(
			tt.name,
			func(t *testing.T) {
				cfg := FileConfig{Filename: tt.prepare(t)}
				cfg.Load(nil)
				logger, closeLogger, err := NewLogger(&cfg)
				if tt.wantErr {
					require.Error(t, err)
					require.Nil(t, logger)
					require.Nil(t, closeLogger)
					return
				}
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, closeLogger()) })
				require.FileExists(t, cfg.Filename)
				require.Empty(t, readLogLines(t, cfg.Filename))
			},
		)
	}
}

func TestLevelMethods_AllWriteAtCorrectLevel(t *testing.T) {
	logger, closeFunc, path := newFileLogger(t, &FileConfig{})
	logger.SetLevel(zapcore.DebugLevel)

	logger.Debug("d")
	logger.Info("i")
	logger.Warn("w")
	logger.Error("e")

	require.NoError(t, logger.Sync())
	require.NoError(t, closeFunc())

	entries := readLogLines(t, path)
	require.Len(t, entries, 4)

	levels := make([]string, len(entries))
	for i, e := range entries {
		levels[i] = e["level"].(string)
	}
	assert.Equal(t, []string{"debug", "info", "warn", "error"}, levels)
}
