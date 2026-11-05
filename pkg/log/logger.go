/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package log

import (
	"io"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	// logzapcore is a partial fork of zapcore. It gives the encoder and the
	// core that move the buffer of the encoder to the sink, with no copy.
	// Each other part of zap and zapcore comes from upstream. See
	// zap/FORK.md.
	logzapcore "xxx/pkg/log/zap/zapcore"
)

// shutdown holds the state that a root Logger and each Logger from that root
// share. Therefore, each derived Logger writes to the same sink, and one
// shutdown stops all of them.
type shutdown struct {
	once sync.Once
	err  error
}

type Logger struct {
	logger      *zap.Logger
	sink        *asyncSink
	atomicLevel zap.AtomicLevel
	shutdown    *shutdown
}

// NewLogger makes a Logger that writes JSON entries to stdout through an
// asynchronous sink. A zero Config gives each default value.
//
// The logger encodes an entry on the goroutine of the caller. Then it sends
// the entry to one writer goroutine through a queue of a fixed size. A log
// call costs one copy and one channel send, and the call does not wait for
// stdout. If the queue is full, the sink discards the entries and counts
// them, and the caller does not wait. The sink writes the count in the
// stream, and Dropped gives the same count. In a container, stdout is a pipe
// to the log collector. Without this design, a slow collector stops the
// goroutine that wrote the log.
//
// The sampler applies only to the levels below Error. It keeps
// Config.SampleFirst entries with the same message in each second. After
// that, it keeps one entry in Config.SampleThereafter entries. The sampler
// uses the message as its key. Therefore, each message must be constant, and
// each variable part must be a field.
//
// The sampler does not apply to Error, but the queue does. If the queue is
// full, the sink discards each level, and this includes Error. A reserve of
// space for each level is not possible. With such a reserve, the sink could
// write an Error before an Info from an earlier time. The sampler is the
// protection that keeps the queue from a full condition.
//
// NewLogger copies cfg and puts the default value in each field that is not
// set. Therefore, a call to LoadConfig before NewLogger is not necessary,
// and NewLogger does not change the Config of the caller.
//
// If the process exits before closeFn, the sink loses each entry in the
// buffer. A SIGKILL, an out-of-memory condition and a fatal error of the
// runtime cause such an exit. Config.FlushInterval limits this loss. Sync
// empties the sink, and zap calls Sync for DPanic and the levels above
// DPanic. Therefore, stdout shows the entry that explains a Fatal before the
// process exits.
//
// The sink reports each failure one time on stderr. It does not use the
// logger, because the logger cannot write a report about its own stream. A
// later call to Sync or closeFn returns that failure.
//
// Call closeFn at a normal shutdown, after each goroutine that writes logs
// stops. closeFn empties the queue, flushes, and stops the writer goroutine.
// A second call gives the result of the first call, and concurrent calls are
// safe. Do not write a log after closeFn. Write accepts the entry, but the
// sink does not write it.
func NewLogger(cfg Config) (l *Logger, closeFn func() error, err error) {
	return newLogger(&cfg, os.Stdout, os.Stderr, time.Now)
}

// newLogger is NewLogger with the destination, the diagnostics writer and
// the clock as parameters. A test can then read the output and control the
// drop notices.
func newLogger(
	cfg *Config,
	dst io.Writer,
	diagnostics io.Writer,
	now func() time.Time,
) (l *Logger, closeFn func() error, err error) {
	cfg.applyDefault()

	err = cfg.validate()
	if err != nil {
		return nil, nil, err
	}

	l = &Logger{
		// AtomicLevel allows a change of the minimum level at run time. The
		// logger needs no rebuild, and the change makes no data race.
		atomicLevel: zap.NewAtomicLevelAt(zap.InfoLevel),
		shutdown:    &shutdown{},
	}
	l.sink = newAsyncSink(cfg, dst, diagnostics, now)

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoder := logzapcore.NewJSONEncoder(encoderConfig)

	// lowCore applies for Debug (-1), Info (0), Warn (1).
	// lowCore passes through a sampling if configured.
	lowCore := logzapcore.NewCore(
		encoder,
		l.sink,
		zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
			return l.atomicLevel.Enabled(lvl) && lvl < zapcore.ErrorLevel
		}),
	)
	if !cfg.DisableSampling {
		lowCore = zapcore.NewSamplerWithOptions(
			lowCore,
			time.Second,
			cfg.SampleFirst,
			cfg.SampleThereafter,
		)
	}

	// highCore applies for Error (2), DPanic (3), Panic (4), Fatal (5).
	// highCore has no sampler. Therefore, it writes every entry, and it
	// ignores none.
	highCore := logzapcore.NewCore(
		encoder,
		l.sink,
		zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
			return l.atomicLevel.Enabled(lvl) && lvl >= zapcore.ErrorLevel
		}),
	)

	l.logger = zap.New(
		zapcore.NewTee(lowCore, highCore),

		// Add the file and the line number of each log call.
		zap.AddCaller(),

		// The log shows the file name and line number of the code that calls the wrapper function.
		// The log does not show the file name and line number of the wrapper function itself.
		zap.AddCallerSkip(1),

		// Keep the diagnostics of zap out of the log stream and out of the
		// sink.
		zap.ErrorOutput(zapcore.Lock(zapcore.AddSync(diagnostics))),
	)
	return l, l.close, nil
}

// Component gives a Logger that adds the field component=name to each entry
// and removes the caller field.
//
// Use Component to send the logs of a library to this logger. The caller of
// such an entry is in that library, some frames deep, and the depth is not
// constant. Therefore, the caller field would show the bridge and nothing
// useful.
//
// The new Logger shares the sink and the shutdown of this Logger. A close of
// one Logger closes both. Do not close a component Logger separately.
func (l *Logger) Component(name string) *Logger {
	return &Logger{
		logger:      l.logger.WithOptions(zap.WithCaller(false)).With(zap.String("component", name)),
		sink:        l.sink,
		atomicLevel: l.atomicLevel,
		shutdown:    l.shutdown,
	}
}

// With gives a Logger that adds fields to each entry. The new Logger shares
// the sink and the shutdown of this Logger.
func (l *Logger) With(fields ...zap.Field) *Logger {
	return &Logger{
		logger:      l.logger.With(fields...),
		sink:        l.sink,
		atomicLevel: l.atomicLevel,
		shutdown:    l.shutdown,
	}
}

// SetLevel changes the minimum log level at run time.
//
// AtomicLevel is safe for concurrent use. Therefore, a call to SetLevel is
// possible while other goroutines write logs.
func (l *Logger) SetLevel(level zapcore.Level) {
	l.atomicLevel.SetLevel(level)
}

// GetLevel gives the minimum log level of this Logger.
func (l *Logger) GetLevel() zapcore.Level {
	return l.atomicLevel.Level()
}

// Dropped gives the number of entries that the sink discarded because its
// queue was full. Export this number as a metric. A value that increases
// shows that the log stream loses data.
func (l *Logger) Dropped() uint64 {
	return l.sink.Dropped()
}

func (l *Logger) Debug(msg string, fields ...zap.Field) {
	l.logger.Debug(msg, fields...)
}

func (l *Logger) Error(msg string, fields ...zap.Field) {
	l.logger.Error(msg, fields...)
}

func (l *Logger) Info(msg string, fields ...zap.Field) {
	l.logger.Info(msg, fields...)
}

func (l *Logger) Warn(msg string, fields ...zap.Field) {
	l.logger.Warn(msg, fields...)
}

func (l *Logger) DPanic(msg string, fields ...zap.Field) {
	l.logger.DPanic(msg, fields...)
}

func (l *Logger) Panic(msg string, fields ...zap.Field) {
	l.logger.Panic(msg, fields...)
}

func (l *Logger) Fatal(msg string, fields ...zap.Field) {
	l.logger.Fatal(msg, fields...)
}

// Sync writes each entry that is in the queue and flushes it to stdout.
//
// Sync does not stop the writer goroutine, but closeFn stops it. Therefore,
// the Logger stays usable after Sync. Sync returns the first write failure
// of the sink. It returns ErrSyncTimeout if the writer goroutine did not
// empty the queue before Config.SyncTimeout.
func (l *Logger) Sync() error {
	return l.sink.Sync()
}

func (l *Logger) close() error {
	l.shutdown.once.Do(func() {
		l.shutdown.err = l.sink.Stop()
	})
	return l.shutdown.err
}
