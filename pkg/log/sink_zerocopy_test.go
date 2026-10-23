/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package log

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// readEntries gives each line of the log file as a map. It also gives the
// number of drop notices, because a notice is not an entry of a caller.
func readEntries(t *testing.T, path string) (entries []map[string]any, notices int) {
	t.Helper()

	file, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var entry map[string]any
		err = json.Unmarshal(line, &entry)
		require.NoError(t, err, "every line must be valid JSON, got %q", line)

		if entry["msg"] == "log entries dropped" {
			notices++
			continue
		}
		entries = append(entries, entry)
	}
	require.NoError(t, scanner.Err())
	return entries, notices
}

// TestZeroCopy_EveryEntryArrivesIntact is the test of the transfer of
// ownership. The sink copies no bytes, so an error in the ownership shows
// here as a line that is not valid JSON, as a missing entry, or as an entry
// two times.
//
// Each goroutine writes a short entry and a long entry, one after the other.
// A buffer that returns to the pool too early therefore shows the next entry
// at the position of this entry, and the length of the two entries is
// different. Then the tail of the longer entry stays after the shorter entry.
func TestZeroCopy_EveryEntryArrivesIntact(t *testing.T) {
	const (
		goroutines = 8
		perG       = 2000
		total      = goroutines * perG
	)

	logger, closeFn, path := newTestLogger(t, &Config{
		QueueSize:  1 << 16,
		BufferSize: 1 << 20,
	})

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()

			for i := range perG {
				if i%2 == 0 {
					logger.Info("short", zap.Int("g", g), zap.Int("i", i))
					continue
				}
				logger.Info(
					"a much longer message that fills more of the encoder buffer",
					zap.Int("g", g),
					zap.Int("i", i),
					zap.String("pad", "0123456789012345678901234567890123456789"),
				)
			}
		}(g)
	}
	wg.Wait()
	require.NoError(t, closeFn())

	entries, notices := readEntries(t, path)
	require.Zero(t, notices, "the queue is large, so the sink must discard nothing")
	require.Zero(t, logger.Dropped())
	require.Len(t, entries, total)

	// Each pair of g and i must be present one time. A buffer that two
	// entries share gives a missing pair and a pair two times.
	seen := make(map[[2]int]int, total)
	for _, entry := range entries {
		g, ok := entry["g"].(float64)
		require.True(t, ok, "entry has no field g: %v", entry)

		i, ok := entry["i"].(float64)
		require.True(t, ok, "entry has no field i: %v", entry)

		seen[[2]int{int(g), int(i)}]++
	}

	assert.Len(t, seen, total)
	for key, count := range seen {
		assert.Equal(t, 1, count, "g=%d i=%d is present %d times", key[0], key[1], count)
	}
}

// TestZeroCopy_DropPathKeepsStreamValid covers the second path that frees a
// buffer. A full queue makes WriteBuffer free the buffer of the entry that it
// discards. A free that is absent loses the bytes from the pool, and a second
// free gives one buffer to two entries. The second fault shows here as a line
// that is not valid JSON.
func TestZeroCopy_DropPathKeepsStreamValid(t *testing.T) {
	const total = 20000

	logger, closeFn, path := newTestLogger(t, &Config{
		QueueSize:     1,
		BufferSize:    64,
		FlushInterval: time.Millisecond,
	})

	for i := range total {
		logger.Info("entry", zap.Int("i", i), zap.String("pad", "0123456789"))
	}
	require.NoError(t, closeFn())

	var (
		dropped          = logger.Dropped()
		entries, notices = readEntries(t, path)
		written          = uint64(len(entries))
	)

	require.Positive(t, dropped, "a queue of one entry must give drops")
	require.Positive(t, notices, "the sink must report the drops in the stream")

	// Each entry is written or discarded. No entry is both, and none is lost
	// without a count.
	assert.Equal(t, uint64(total), written+dropped)
}

// TestZeroCopy_DropNoticeReportsTheFinalTotal checks that the last notice
// agrees with Dropped. drain forces a notice, so each drop reaches the
// stream.
func TestZeroCopy_DropNoticeReportsTheFinalTotal(t *testing.T) {
	const total = 5000

	logger, closeFn, path := newTestLogger(t, &Config{
		QueueSize:     1,
		BufferSize:    64,
		FlushInterval: time.Millisecond,
	})

	for i := range total {
		logger.Info("entry", zap.Int("i", i))
	}
	require.NoError(t, closeFn())

	file, err := os.Open(path)
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	var lastTotal float64
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		var entry map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &entry))

		if entry["msg"] != "log entries dropped" {
			continue
		}
		value, ok := entry["dropped_total"].(float64)
		require.True(t, ok, "notice has no field dropped_total: %v", entry)
		lastTotal = value
	}
	require.NoError(t, scanner.Err())

	assert.Equal(t, float64(logger.Dropped()), lastTotal)
}
