/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package reject

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"xxx/pkg/log"

	"github.com/stretchr/testify/require"
)

type testReason uint8

const (
	badFrame testReason = iota
	badProof
)

type logEntry struct {
	Level        string `json:"level"`
	Reason       string `json:"reason"`
	SinceLastLog uint64 `json:"since_last_log"`
	Error        string `json:"error"`
}

func captureStats(t *testing.T, interval time.Duration) (*Stats[testReason], func() []logEntry) {
	t.Helper()

	cfg := log.FileConfig{
		Filename: filepath.Join(t.TempDir(), "reject.log"),
	}
	cfg.Load(nil)

	logger, closeLogger, err := log.NewLogger(&cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, closeLogger()) })

	stats := New[testReason](
		logger,
		interval,
		[]string{"bad_frame", "bad_proof"},
	)
	stats.now = func() int64 { return int64(time.Minute) }

	logFunc := func() []logEntry {
		t.Helper()

		require.NoError(t, logger.Sync())
		raw, err := os.ReadFile(cfg.Filename)
		require.NoError(t, err)

		var entries []logEntry
		for line := range strings.SplitSeq(strings.TrimSpace(string(raw)), "\n") {
			if line == "" {
				continue
			}
			var entry logEntry
			require.NoError(t, json.Unmarshal([]byte(line), &entry))
			entries = append(entries, entry)
		}
		return entries
	}

	return stats, logFunc
}

func TestRecordSamplesPerReasonAndCountsEveryRejection(t *testing.T) {
	stats, entries := captureStats(t, 5*time.Second)
	now := int64(time.Minute)
	stats.now = func() int64 { return now }
	stats.Record(badFrame, errors.New("invalid frame"))
	for range 9 {
		stats.Record(badFrame, nil)
	}
	stats.Record(badProof, nil)

	now += int64(5*time.Second - time.Nanosecond)
	stats.Record(badFrame, nil)
	require.Equal(
		t,
		[]logEntry{
			{Level: "warn", Reason: "bad_frame", SinceLastLog: 1, Error: "invalid frame"},
			{Level: "warn", Reason: "bad_proof", SinceLastLog: 1},
		},
		entries(),
	)

	now++ // The interval boundary allows exactly one more log for this reason.
	stats.Record(badFrame, nil)
	stats.Record(badFrame, nil)
	got := entries()
	require.Len(t, got, 3)
	require.Equal(t, logEntry{Level: "warn", Reason: "bad_frame", SinceLastLog: 11}, got[2])
	require.Equal(t, uint64(13), stats.Count(badFrame))
	require.Equal(t, uint64(1), stats.Count(badProof))

	counts := make(map[string]uint64)
	stats.Each(func(reason string, count uint64) {
		counts[reason] = count
	})
	require.Equal(t, map[string]uint64{"bad_frame": 13, "bad_proof": 1}, counts)
}

func TestRecordConcurrentSampling(t *testing.T) {
	stats, entries := captureStats(t, time.Second)
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 1000 {
				stats.Record(badFrame, nil)
				stats.Record(badProof, nil)
			}
		})
	}
	wg.Wait()
	require.Equal(t, uint64(16_000), stats.Count(badFrame))
	require.Equal(t, uint64(16_000), stats.Count(badProof))

	got := entries()
	require.Len(t, got, 2)
	require.ElementsMatch(
		t,
		[]string{"bad_frame", "bad_proof"},
		[]string{got[0].Reason, got[1].Reason},
	)
}

func TestNewDefaultsAndUnknownReason(t *testing.T) {
	for _, interval := range []time.Duration{0, -time.Second} {
		stats, entries := captureStats(t, interval)
		require.Equal(t, int64(defaultInterval), stats.interval)

		stats.Record(testReason(255), nil)
		stats.Record(badFrame, nil)
		require.Zero(t, stats.Count(badProof))
		require.Len(t, entries(), 1)
	}
}
