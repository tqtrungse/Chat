/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package zapcore

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	upstream "go.uber.org/zap/zapcore"
)

func testEncoderConfig() EncoderConfig {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = upstream.ISO8601TimeEncoder
	return cfg
}

func testEntry() Entry {
	return Entry{
		Level:      upstream.ErrorLevel,
		Time:       time.Date(2026, 11, 1, 10, 30, 0, 0, time.UTC),
		LoggerName: "hub.connection",
		Message:    "nats reconnect failed",
		Caller: EntryCaller{
			Defined:  true,
			File:     "internal/hub/infrastructure/nats/conn.go",
			Line:     142,
			Function: "xxx/internal/hub/infrastructure/nats.Connect",
		},
		Stack: "goroutine 1 [running]:\nmain.main()",
	}
}

func testFields() []Field {
	type payload struct {
		Shard int    `json:"shard"`
		Node  string `json:"node"`
	}
	return []Field{
		zap.String("hub_id", "hub-7"),
		zap.Int("attempt", 7),
		zap.Int64("seq", 1<<40),
		zap.Uint16("port", 4222),
		zap.Float64("ratio", 0.125),
		zap.Bool("shutting_down", false),
		zap.Duration("backoff", 1500*time.Millisecond),
		zap.Time("deadline", time.Date(2026, 11, 1, 10, 31, 0, 0, time.UTC)),
		zap.Error(errors.New("connection refused")),
		zap.Strings("urls", []string{"nats://a:4222", "nats://b:4222"}),
		zap.Binary("token", []byte{0x00, 0x01, 0xff}),
		zap.ByteString("raw", []byte(`{"a":1}`)),
		zap.Any("payload", payload{Shard: 3, Node: "n-3"}),
		zap.Namespace("meta"),
		zap.String("region", "ap-southeast-1"),
	}
}

// TestEncodeEntry_MatchesUpstream is the guarantee that the fork changed the
// buffer and nothing else. The forked encoder and the encoder of upstream
// must give the same bytes for the same entry.
func TestEncodeEntry_MatchesUpstream(t *testing.T) {
	var (
		forked   = NewJSONEncoder(testEncoderConfig())
		original = upstream.NewJSONEncoder(testEncoderConfig())
		entry    = testEntry()
	)

	forkedBuf, err := forked.EncodeEntry(entry, testFields())
	require.NoError(t, err)
	defer forkedBuf.Free()

	originalBuf, err := original.EncodeEntry(entry, testFields())
	require.NoError(t, err)
	defer originalBuf.Free()

	assert.Equal(t, string(originalBuf.Bytes()), string(forkedBuf.Bytes()))
}

// TestEncodeEntry_MatchesUpstreamWithContext covers the path of Clone, which
// holds the fields of Logger.With.
func TestEncodeEntry_MatchesUpstreamWithContext(t *testing.T) {
	var (
		forked   = NewJSONEncoder(testEncoderConfig())
		original = upstream.NewJSONEncoder(testEncoderConfig())
		context  = []Field{zap.String("component", "nats"), zap.Int("shard", 3)}
		entry    = testEntry()
	)

	addFields(forked, context)
	for i := range context {
		context[i].AddTo(original)
	}

	forkedBuf, err := forked.Clone().EncodeEntry(entry, testFields())
	require.NoError(t, err)
	defer forkedBuf.Free()

	originalBuf, err := original.Clone().EncodeEntry(entry, testFields())
	require.NoError(t, err)
	defer originalBuf.Free()

	assert.Equal(t, string(originalBuf.Bytes()), string(forkedBuf.Bytes()))
}

func TestEncodeEntry_MatchesUpstreamForEachLevel(t *testing.T) {
	levels := []Level{
		upstream.DebugLevel,
		upstream.InfoLevel,
		upstream.WarnLevel,
		upstream.ErrorLevel,
		upstream.DPanicLevel,
		upstream.PanicLevel,
		upstream.FatalLevel,
	}

	for _, level := range levels {
		t.Run(level.String(), func(t *testing.T) {
			entry := testEntry()
			entry.Level = level

			forkedBuf, err := NewJSONEncoder(testEncoderConfig()).
				EncodeEntry(entry, nil)
			require.NoError(t, err)
			defer forkedBuf.Free()

			originalBuf, err := upstream.NewJSONEncoder(testEncoderConfig()).
				EncodeEntry(entry, nil)
			require.NoError(t, err)
			defer originalBuf.Free()

			assert.Equal(t, string(originalBuf.Bytes()), string(forkedBuf.Bytes()))
		})
	}
}

// TestNewCore_SatisfiesUpstreamCore is a compile-time and run-time check that
// the alias of the types works. zap.New accepts the core of this fork, so the
// fork needs no copy of the logger, the sampler or the tee.
func TestNewCore_SatisfiesUpstreamCore(t *testing.T) {
	var (
		sink = &countingSyncer{}
		core = NewCore(NewJSONEncoder(testEncoderConfig()), sink, upstream.InfoLevel)
	)

	// Each wrapper of upstream must accept the forked core.
	sampled := upstream.NewSamplerWithOptions(core, time.Second, 1, 0)
	tee := upstream.NewTee(sampled, core)
	logger := zap.New(tee)

	logger.Info("entry", zap.Int("i", 1))
	require.NoError(t, logger.Sync())
	assert.Positive(t, sink.writes)
}

// countingSyncer is a plain WriteSyncer. It does not implement
// BufferWriteSyncer. Therefore, it covers the path of the core that writes
// the bytes and frees the buffer.
type countingSyncer struct {
	writes int
	bytes  int
}

func (c *countingSyncer) Write(p []byte) (int, error) {
	c.writes++
	c.bytes += len(p)
	return len(p), nil
}

func (*countingSyncer) Sync() error { return nil }
