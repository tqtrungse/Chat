/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package scylla

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	"xxx/pkg/log"
	"xxx/pkg/log/logtest"

	"github.com/stretchr/testify/require"
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

func TestGocqlLoggerPrint(t *testing.T) {
	logger, closeLogger, path := logtest.New(t, log.Config{})
	t.Cleanup(func() { _ = closeLogger() })

	newGocqlLogger(logger).Print("unable to dial ", "node1")
	require.NoError(t, closeLogger())

	entry := readEntry(t, path)
	require.Equal(t, "warn", entry["level"])
	require.Equal(t, msgNotice, entry["msg"])
	require.Equal(t, "gocql", entry["component"])
	require.Equal(t, "unable to dial node1", entry["detail"])
}

func TestGocqlLoggerPrintf(t *testing.T) {
	logger, closeLogger, path := logtest.New(t, log.Config{})
	t.Cleanup(func() { _ = closeLogger() })

	newGocqlLogger(logger).Printf("gocql: unable to dial %q: %v", "node1", "timeout")
	require.NoError(t, closeLogger())

	entry := readEntry(t, path)
	require.Equal(t, "warn", entry["level"])
	require.Equal(t, msgNotice, entry["msg"])
	require.Equal(t, "gocql", entry["component"])
	require.Equal(t, `gocql: unable to dial "node1": timeout`, entry["detail"])
}

func TestGocqlLoggerPrintlnTrimsTrailingNewline(t *testing.T) {
	logger, closeLogger, path := logtest.New(t, log.Config{})
	t.Cleanup(func() { _ = closeLogger() })

	newGocqlLogger(logger).Println("control falling back to initial contact points")
	require.NoError(t, closeLogger())

	entry := readEntry(t, path)
	require.Equal(t, "control falling back to initial contact points", entry["detail"])
	require.NotContains(t, entry["detail"], "\n")
}

func TestNewGocqlLoggerNilDiscards(t *testing.T) {
	require.NotPanics(
		t,
		func() {
			l := newGocqlLogger(nil)
			l.Print("x")
			l.Printf("%s", "x")
			l.Println("x")
		},
	)
}
