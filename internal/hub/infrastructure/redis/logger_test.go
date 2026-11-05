/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package redis

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"xxx/pkg/log"
	"xxx/pkg/log/logtest"

	"github.com/alicebob/miniredis/v2"
	redisv9 "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

// logtest.New replaces os.Stdout for the call, so no test below runs parallel.

func readEntry(t *testing.T, path string) map[string]any {
	t.Helper()

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var entry map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(string(data))), &entry))
	return entry
}

func readEntries(t *testing.T, path string) []map[string]any {
	t.Helper()

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	content := strings.TrimSpace(string(data))
	if content == "" {
		return nil
	}

	var entries []map[string]any
	for line := range strings.SplitSeq(content, "\n") {
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		entries = append(entries, entry)
	}
	return entries
}

func TestGoredisLoggerPrintf(t *testing.T) {
	logger, closeLogger, path := logtest.New(t, log.Config{})
	t.Cleanup(func() { _ = closeLogger() })

	newGoredisLogger(logger).Printf(
		t.Context(),
		"pubsub: reconnecting to new endpoint %s (was %s)",
		"10.0.0.7:6379",
		"10.0.0.3:6379",
	)
	require.NoError(t, closeLogger())

	entry := readEntry(t, path)
	require.NotContains(t, entry, "caller")
	require.Equal(t, "warn", entry["level"])
	require.Equal(t, "pubsub: reconnecting to new endpoint %s (was %s)", entry["msg"])
	require.Equal(t, "redis", entry["component"])
	require.Equal(
		t,
		"pubsub: reconnecting to new endpoint 10.0.0.7:6379 (was 10.0.0.3:6379)",
		entry["params"],
	)
}

// stringerArg counts how many times fmt calls its String method.
// The test below confirms one thing: an entry that the gate rejects does
// not format its argument.
type stringerArg struct {
	rendered *int
}

func (a stringerArg) String() string {
	*a.rendered++
	return "value"
}

func TestGoredisLoggerPrintfGatedByLevel(t *testing.T) {
	logger, closeLogger, path := logtest.New(t, log.Config{})
	t.Cleanup(func() { _ = closeLogger() })
	logger.SetLevel(zapcore.ErrorLevel)

	rendered := 0
	newGoredisLogger(logger).Printf(t.Context(), "redis: %s", stringerArg{rendered: &rendered})
	require.NoError(t, closeLogger())

	require.Empty(t, readEntries(t, path))
	require.Zero(t, rendered, "Sprintf costs a pass over the arguments")
}

func TestNewGoredisLoggerNilPanics(t *testing.T) {
	// newGoredisLogger must get a logger that is not nil. InstallLogger is
	// the only path that can get a nil logger. InstallLogger does not call
	// newGoredisLogger in that case.
	require.Panics(t, func() { newGoredisLogger(nil) })
}

func TestInstallLoggerNilDiscards(t *testing.T) {
	t.Cleanup(func() { InstallLogger(nil) })

	require.NotPanics(t, func() { InstallLogger(nil) })
}

// TestGoredisLoggerReachesGoredis covers the wiring that the other tests in
// this file assume.
//
// InstallLogger makes a real client of go-redis give its line to this
// bridge. Without InstallLogger, that line goes to os.Stderr instead.
func TestGoredisLoggerReachesGoredis(t *testing.T) {
	logger, closeLogger, path := logtest.New(t, log.Config{})
	t.Cleanup(func() { _ = closeLogger() })
	t.Cleanup(func() { InstallLogger(nil) })

	mr := miniredis.RunT(t)
	rdb := redisv9.NewClient(&redisv9.Options{
		Addr: mr.Addr(),
		// DialerRetries of 1 removes the fixed 100ms backoff between dial
		// retries. The test then reaches the bridge at once.
		DialerRetries: 1,
	})
	t.Cleanup(func() { _ = rdb.Close() })

	InstallLogger(logger)
	mr.Close()
	_ = rdb.Ping(t.Context()).Err()

	require.NoError(t, closeLogger())

	entries := readEntries(t, path)
	found := false
	for _, entry := range entries {
		if entry["component"] == "redis" {
			found = true
			break
		}
	}
	require.True(t, found, "a line from go-redis must reach the bridge: %v", entries)
}
