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
	"errors"
	"os"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/term"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Logger struct {
	logger         *zap.Logger
	bufferedSyncer *zapcore.BufferedWriteSyncer
	lumberjack     *lumberjack.Logger
	atomicLevel    zap.AtomicLevel

	// closeOnce makes Close idempotent and safe to call concurrently.
	//
	// This is important because shutting down BufferedWriteSyncer stops its
	// background flushing goroutine and closes the logger's underlying
	// resources. Those operations should happen exactly once.
	closeOnce sync.Once
	closeErr  error
}

// NewLogger constructs a Logger.
//
// Log entries are buffered in memory (see FileConfig.BufferSize and
// FileConfig.FlushInterval) and are only guaranteed to reach disk
// after a successful call to Sync or closeFn. If the process exits without
// calling one of them — e.g. it is killed, panics outside of this logger,
// or the machine loses power — any buffered-but-not-yet-flushed log entries
// are lost. Callers should call closeFn (or Sync, for a periodic/graceful
// flush without shutting the logger down) during normal shutdown, and
// ideally also from a signal handler (SIGTERM/SIGINT) so logs are not lost
// when the process is asked to stop.
//
// closeFn func flushes pending log entries, stops the buffered writer's background
// goroutine, and closes the underlying rotating file when file logging is
// enabled.
//
// Because log entries are buffered in memory until Sync or closeFn runs clos,
// callers MUST call closeFn during shutdown — or logs written shortly before
// exit can be silently lost. Prefer calling it via defer closeFn() right after
// construction, and also from a signal handler for processes that may be stopped
// externally.
//
// closeFn is idempotent and concurrency-safe. Subsequent calls return the same
// result produced by the first shutdown attempt.
func NewLogger(cfg *FileConfig) (l *Logger, closeFn func() error) {
	l = &Logger{
		// AtomicLevel allows the minimum log level to be changed dynamically
		// without rebuilding the logger or introducing data races.
		atomicLevel: zap.NewAtomicLevelAt(zap.InfoLevel),
	}

	var (
		sink          zapcore.WriteSyncer
		bufferSize    int
		flushInterval time.Duration
	)

	if cfg != nil {
		if cfg.BufferSize > 0 {
			bufferSize = cfg.BufferSize
		}
		if cfg.FlushInterval > 0 {
			flushInterval = cfg.FlushInterval
		}
		// Lumberjack owns log rotation and retention. BufferedWriteSyncer is
		// layered on top of it to reduce the number of writes to the file.
		l.lumberjack = &lumberjack.Logger{
			Filename:   cfg.Filename,
			MaxSize:    cfg.MaxSize,
			MaxBackups: cfg.MaxBackups,
			MaxAge:     cfg.MaxAge,
			Compress:   cfg.Compress,
		}
		sink = zapcore.AddSync(l.lumberjack)
	} else {
		ws := zapcore.AddSync(os.Stdout)
		if term.IsTerminal(int(os.Stdout.Fd())) {
			ws = noSyncWriteSyncer{ws}
		}
		sink = ws
	}

	// Always keep a reference to BufferedWriteSyncer, regardless of whether
	// the destination is stdout or a file. This gives both modes the same
	// shutdown lifecycle: Stop() can flush pending data and stop the internal
	// background flushing goroutine.
	l.bufferedSyncer = &zapcore.BufferedWriteSyncer{
		WS:            sink,
		Size:          bufferSize,
		FlushInterval: flushInterval,
	}

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		l.bufferedSyncer,
		l.atomicLevel,
	)

	l.logger = zap.New(
		core,

		// Include the source file and line number of each log call.
		zap.AddCaller(),

		// Skip Logger.Info/Debug/etc. so the reported caller is the code that
		// invoked this Logger wrapper rather than the wrapper method itself.
		zap.AddCallerSkip(1),
	)
	return l, l.close
}

// SetLevel dynamically changes the minimum log level.
//
// AtomicLevel is concurrency-safe, so this method may be called while other
// goroutines are writing logs.
func (l *Logger) SetLevel(level zapcore.Level) {
	l.atomicLevel.SetLevel(level)
}

// GetLevel returns the currently configured minimum log level.
func (l *Logger) GetLevel() zapcore.Level {
	return l.atomicLevel.Level()
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

// Sync flushes any buffered log entries to the underlying writer (the log
// file, or stdout when file logging is not configured).
//
// Unlike Close, Sync does not stop the buffered writer's background
// flushing goroutine or release any resources, so the Logger remains fully
// usable afterward. Call Sync when you want a flush point without ending
// the logger's lifecycle — e.g. periodically, or on a non-fatal signal —
// and reserve Close for final shutdown.
//
// Errors returned when syncing a non-seekable descriptor such as stdout
// (e.g. ENOTTY/EINVAL) are not treated as failures; see isUnsyncable.
func (l *Logger) Sync() error {
	if err := l.bufferedSyncer.Sync(); err != nil && !isUnsyncable(err) {
		return err
	}
	return nil
}

func (l *Logger) close() error {
	l.closeOnce.Do(func() {
		var errs []error

		// Stop is preferred for shutdown over Sync alone.
		//
		// Sync flushes the current buffer, but Stop also terminates the
		// BufferedWriteSyncer's background flushing goroutine. Since this
		// is the final lifecycle operation for the buffered writer, Stop
		// ensures that both responsibilities are completed.
		if l.bufferedSyncer != nil {
			if err := l.bufferedSyncer.Stop(); err != nil &&
				!isUnsyncable(err) {
				errs = append(errs, err)
			}
		}

		// Stop has already flushed buffered data before the underlying
		// lumberjack writer is closed, so no buffered writes should remain.
		if l.lumberjack != nil {
			if err := l.lumberjack.Close(); err != nil {
				errs = append(errs, err)
			}
		}

		// Preserve all shutdown errors instead of returning only the first one.
		l.closeErr = errors.Join(errs...)
	})
	return l.closeErr
}

// noSyncWriteSyncer wraps a WriteSyncer whose Sync should be a no-op —
// typically because the underlying descriptor is a terminal, where
// fsync-style semantics don't apply and the OS's error code for a failed
// flush attempt is platform-specific (EINVAL/ENOTTY on Unix,
// ERROR_INVALID_HANDLE on Windows, etc.). Skipping the syscall entirely
// avoids depending on an exhaustive, per-platform errno list.
type noSyncWriteSyncer struct {
	zapcore.WriteSyncer
}

func (noSyncWriteSyncer) Sync() error { return nil }

// isUnsyncable reports whether err is one of the well-known errors returned
// when syncing a non-seekable file descriptor such as stdout or stderr.
//
// On some platforms, calling fsync-like operations on streams can return
// ENOTTY or EINVAL. Those errors do not indicate that buffered log data was
// lost, so they are intentionally ignored during shutdown.
func isUnsyncable(err error) bool {
	return errors.Is(err, syscall.EINVAL) ||
		errors.Is(err, syscall.ENOTTY) ||
		errors.Is(err, syscall.EBADF)
}
