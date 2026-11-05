/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package log

import (
	"io"
	"testing"
	"time"

	"go.uber.org/zap"
)

// benchLogger makes a logger. The destination of the logger discards each
// byte. Therefore, the numbers measure the cost that this package adds to
// one log call, and not the speed of the terminal or the disk under it.
func benchLogger(b *testing.B, cfg *Config) *Logger {
	b.Helper()

	logger, closeFn, err := newLogger(
		cfg,
		io.Discard,
		io.Discard,
		time.Now,
	)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = closeFn() })
	return logger
}

// BenchmarkInfo_Unsampled gives the cost of encoding and the queue hand-off
// together. This is the cost of a log call that the sink writes.
func BenchmarkInfo_Unsampled(b *testing.B) {
	logger := benchLogger(
		b,
		&Config{
			QueueSize:       1 << 16,
			DisableSampling: true,
		},
	)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		logger.Info("benchmark entry", zap.Int("i", i))
	}
}

// BenchmarkInfo_Sampled gives the cost of the same call after the sampler
// starts to suppress it. This case is the common case for a message that
// repeats under load.
func BenchmarkInfo_Sampled(b *testing.B) {
	logger := benchLogger(
		b,
		&Config{
			QueueSize:        1 << 16,
			SampleFirst:      1,
			SampleThereafter: 1 << 30,
		},
	)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		logger.Info("benchmark entry", zap.Int("i", i))
	}
}

// BenchmarkInfo_Dropped gives the cost when the queue is full. This cost is
// the price of a stalled stdout. The cost must stay bounded, and the
// benchmark must show few or no allocations.
func BenchmarkInfo_Dropped(b *testing.B) {
	logger := benchLogger(
		b,
		&Config{
			QueueSize:       1,
			DisableSampling: true,
		},
	)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		logger.Info("benchmark entry", zap.Int("i", i))
	}
}

// BenchmarkInfo_Parallel checks that concurrent producers do not contend with
// each other. Each caller encodes its own entry. Only the channel send is
// shared.
func BenchmarkInfo_Parallel(b *testing.B) {
	logger := benchLogger(
		b,
		&Config{
			QueueSize:       1 << 16,
			DisableSampling: true,
		},
	)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			logger.Info("benchmark entry", zap.Int("i", 1))
		}
	})
}
