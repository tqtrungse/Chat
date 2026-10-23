/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package log

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// readLogLines reads path and decodes each line that is not empty as a JSON
// log entry. It fails the test at once if it cannot read the file, or if a
// line is not valid JSON. Each entry from the zap JSON encoder must parse,
// and the drop notice of the sink must also parse.
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

// newTestLogger makes a Logger that writes to a new file under t.TempDir()
// and gives the Logger and the path of that file.
//
// Sampling is off unless the test asks for it. Most tests count entries, and
// a sampler would remove some of a message that repeats.
func newTestLogger(t *testing.T, cfg *Config) (*Logger, func() error, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "test.log")
	f, err := os.Create(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	var full Config
	if cfg != nil {
		full = *cfg
	}
	if cfg == nil || cfg.SampleFirst <= 0 {
		full.DisableSampling = true
	}

	logger, closeFn, err := newLogger(&full, f, io.Discard, time.Now)
	require.NoError(t, err)
	t.Cleanup(func() { _ = closeFn() })
	return logger, closeFn, path
}

func TestNewLogger_NilConfig_WritesToStdout(t *testing.T) {
	logger, closeFn, err := NewLogger(Config{})
	require.NoError(t, err)
	require.NotNil(t, logger)

	require.NotNil(t, logger.sink, "an async sink must always be created")
	assert.Equal(t, zapcore.InfoLevel, logger.GetLevel(), "default level should be Info")
	assert.Zero(t, logger.Dropped())

	assert.NoError(t, closeFn())
}

func TestNewLogger_RejectsBufferSizeBelowMinimum(t *testing.T) {
	logger, closeFn, err := NewLogger(Config{BufferSize: 1})
	assert.ErrorIs(t, err, ErrBufferSizeTooSmall)
	assert.Nil(t, logger)
	assert.Nil(t, closeFn)
}

func TestNewLogger_DoesNotMutateCallerConfig(t *testing.T) {
	cfg := Config{}
	_, closeFn, err := newLogger(&cfg, io.Discard, io.Discard, time.Now)
	require.NoError(t, err)
	defer func() { _ = closeFn() }()

	cfgExpected := Config{}
	cfgExpected.applyDefault()
	assert.Equal(t, cfgExpected, cfg, "NewLogger must apply defaults to a copy")
}

func TestLogger_WritesExpectedFields(t *testing.T) {
	logger, closeFn, path := newTestLogger(t, nil)

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

	require.NoError(t, closeFn())
}

func TestCallerSkip_ReportsCallSiteNotWrapper(t *testing.T) {
	logger, closeFn, path := newTestLogger(t, nil)

	logger.Info("caller check") // The assertion below checks the number of this line.
	require.NoError(t, logger.Sync())
	require.NoError(t, closeFn())

	entries := readLogLines(t, path)
	require.Len(t, entries, 1)

	caller, ok := entries[0]["caller"].(string)
	require.True(t, ok)
	// AddCallerSkip(1) skips the Logger.Info wrapper method. Therefore, the
	// reported caller is this test file, and not logger.go.
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
	logger, _, _ := newTestLogger(t, nil)
	assert.Equal(t, zapcore.InfoLevel, logger.GetLevel())

	logger.SetLevel(zapcore.DebugLevel)
	assert.Equal(t, zapcore.DebugLevel, logger.GetLevel())

	logger.SetLevel(zapcore.ErrorLevel)
	assert.Equal(t, zapcore.ErrorLevel, logger.GetLevel())
}

func TestLogging_RespectsConfiguredLevel(t *testing.T) {
	logger, closeFn, path := newTestLogger(t, nil)
	logger.SetLevel(zapcore.WarnLevel)

	logger.Debug("debug msg")
	logger.Info("info msg")
	logger.Warn("warn msg")
	logger.Error("error msg")

	require.NoError(t, logger.Sync())
	require.NoError(t, closeFn())

	entries := readLogLines(t, path)
	require.Len(t, entries, 2, "only Warn and Error should pass the Warn level filter")
	assert.Equal(t, "warn", entries[0]["level"])
	assert.Equal(t, "error", entries[1]["level"])
}

// TestLevelSplit_NoDuplicateEntries protects the split between the two
// cores. The low enabler and the high enabler must not overlap. An overlap
// would write each Error entry two times.
func TestLevelSplit_NoDuplicateEntries(t *testing.T) {
	logger, closeFn, path := newTestLogger(t, nil)
	logger.SetLevel(zapcore.DebugLevel)

	logger.Debug("d")
	logger.Info("i")
	logger.Warn("w")
	logger.Error("e")

	require.NoError(t, logger.Sync())
	require.NoError(t, closeFn())

	entries := readLogLines(t, path)
	require.Len(t, entries, 4, "each entry must be written exactly once")

	levels := make([]string, len(entries))
	for i, e := range entries {
		levels[i] = e["level"].(string)
	}
	assert.Equal(t, []string{"debug", "info", "warn", "error"}, levels)
}

func TestSync_FlushesWithoutStoppingLogger(t *testing.T) {
	logger, closeFn, path := newTestLogger(t, nil)

	logger.Info("first")
	require.NoError(t, logger.Sync())

	// The Logger stays usable after Sync. It is not usable after closeFn.
	logger.Info("second")
	require.NoError(t, logger.Sync())

	entries := readLogLines(t, path)
	require.Len(t, entries, 2)
	assert.Equal(t, "first", entries[0]["msg"])
	assert.Equal(t, "second", entries[1]["msg"])

	require.NoError(t, closeFn())
}

func TestClose_FlushesQueuedEntriesWithoutExplicitSync(t *testing.T) {
	// The flush interval is far longer than the test. Therefore, no entry
	// reaches the file by itself, and the test isolates the drain of closeFn
	// from the background flush loop.
	logger, closeFn, path := newTestLogger(t, &Config{
		BufferSize:    64 * 1024,
		FlushInterval: time.Hour,
	})

	logger.Info("queued before close")
	require.NoError(t, closeFn())

	entries := readLogLines(t, path)
	require.Len(t, entries, 1)
	assert.Equal(t, "queued before close", entries[0]["msg"])
}

func TestClose_IsIdempotent(t *testing.T) {
	logger, closeFn, _ := newTestLogger(t, nil)
	logger.Info("one entry")

	err1 := closeFn()
	err2 := closeFn()
	assert.NoError(t, err1)
	assert.Equal(t, err1, err2, "repeated close calls must return the same result")
}

func TestClose_IsSafeForConcurrentUse(t *testing.T) {
	logger, closeFn, _ := newTestLogger(t, nil)
	logger.Info("one entry")

	const goroutines = 20
	var wg sync.WaitGroup
	errs := make([]error, goroutines)

	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = closeFn()
		}(i)
	}
	wg.Wait()

	for i := 1; i < goroutines; i++ {
		assert.Equal(t, errs[0], errs[i], "all concurrent close calls must observe the same result")
	}
}

func TestConcurrentLogging_NoRace(t *testing.T) {
	const (
		goroutines   = 8
		perGoroutine = 50
	)

	// The queue must hold each entry. Therefore, this test measures
	// correctness, and not the drop path. The drop path has its own tests.
	logger, closeFn, path := newTestLogger(t, &Config{QueueSize: goroutines * perGoroutine * 2})

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
	require.NoError(t, closeFn())

	assert.Zero(t, logger.Dropped())
	entries := readLogLines(t, path)
	assert.Len(t, entries, goroutines*perGoroutine)
}

func TestComponent_TagsEntriesAndDropsCaller(t *testing.T) {
	logger, closeFn, path := newTestLogger(t, nil)

	logger.Component("redis").Warn("driver said something")
	require.NoError(t, logger.Sync())
	require.NoError(t, closeFn())

	entries := readLogLines(t, path)
	require.Len(t, entries, 1)
	assert.Equal(t, "redis", entries[0]["component"])
	assert.NotContains(t, entries[0], "caller", "a component logger must not report a caller")
}

// TestComponent_SharesShutdown checks that a derived Logger does not get its
// own sync.Once. A close through either handle must stop the shared sink one
// time only.
func TestComponent_SharesShutdown(t *testing.T) {
	logger, closeFn, path := newTestLogger(t, nil)
	component := logger.Component("redis")

	logger.Info("root")
	component.Info("derived")

	require.NoError(t, closeFn())
	require.NoError(t, component.close(), "closing a component must reuse the first result")

	entries := readLogLines(t, path)
	require.Len(t, entries, 2)
}

func TestWith_SharesSink(t *testing.T) {
	logger, closeFn, path := newTestLogger(t, nil)

	logger.With(zap.String("request_id", "abc")).Info("tagged")
	require.NoError(t, logger.Sync())
	require.NoError(t, closeFn())

	entries := readLogLines(t, path)
	require.Len(t, entries, 1)
	assert.Equal(t, "abc", entries[0]["request_id"])
}
