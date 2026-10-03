/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
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

package log

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
	logger, closeFunc := NewLogger(full)
	return logger, closeFunc, path
}

func TestNewLogger_NilConfig_UsesStdout(t *testing.T) {
	logger, closeFunc := NewLogger(nil)
	require.NotNil(t, logger)

	assert.Nil(t, logger.lumberjack, "no lumberjack writer should be created when logging to stdout")
	assert.Equal(t, zapcore.InfoLevel, logger.GetLevel(), "default level should be Info")

	// Stdout is typically non-seekable in test environments, which Stop
	// filters via isUnsyncable; either way Close must not surface an error
	// for a completely unused logger.
	assert.NoError(t, closeFunc())
}

func TestNewLogger_FileConfig_WritesExpectedFields(t *testing.T) {
	logger, closeFunc, path := newFileLogger(t, &FileConfig{MaxSize: 1})
	require.NotNil(t, logger)
	require.NotNil(t, logger.lumberjack, "a lumberjack writer should be created when a file is configured")

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

	logger.Info("caller check") // this line's number is asserted below
	require.NoError(t, logger.Sync())
	require.NoError(t, closeFunc())

	entries := readLogLines(t, path)
	require.Len(t, entries, 1)

	caller, ok := entries[0]["caller"].(string)
	require.True(t, ok)
	// AddCallerSkip(1) should skip past the Logger.Info wrapper method, so
	// the reported caller is this test file, not logger.go.
	assert.Truef(t, strings.Contains(caller, "logger_test.go"), "expected caller to point at the test file, got %q", caller)
	assert.Falsef(t, strings.Contains(caller, "logger.go"), "caller should not point at the Logger wrapper itself, got %q", caller)
}

func TestSetLevel_GetLevel(t *testing.T) {
	logger, closeFunc := NewLogger(nil)
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

func TestNewLogger_BufferDefaults_PassedThroughUnset(t *testing.T) {
	logger, closeFunc, _ := newFileLogger(t, &FileConfig{})
	defer func() { _ = closeFunc() }()

	// NewLogger does not itself substitute 256kB/30s; it passes the zero
	// values through and zapcore.BufferedWriteSyncer applies its own
	// defaults lazily on first write.
	assert.Equal(t, 0, logger.bufferedSyncer.Size)
	assert.Equal(t, time.Duration(0), logger.bufferedSyncer.FlushInterval)
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
	logger, closeFunc := NewLogger(cfg)
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
	assert.True(t, isUnsyncable(errors.Join(errors.New("wrapped"), syscall.ENOTTY)))
	assert.False(t, isUnsyncable(nil))
	assert.False(t, isUnsyncable(errors.New("some other error")))
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
