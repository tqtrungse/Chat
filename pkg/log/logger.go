/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package log

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Logger struct {
	logger         *zap.Logger
	syncer         zapcore.WriteSyncer
	bufferedSyncer *zapcore.BufferedWriteSyncer
	lumberjack     *lumberjack.Logger
	atomicLevel    zap.AtomicLevel

	// closeOnce makes closeFn idempotent and safe to call concurrently.
	//
	// This is important because shutting down BufferedWriteSyncer stops its
	// background flushing goroutine and closes the logger's underlying
	// resources. Those operations should happen exactly once.
	closeOnce sync.Once
	closeErr  error
}

// NewLogger constructs a Logger. With nil cfg, entries are written directly
// to stdout without an application buffer. The returned closeFn never closes
// stdout.
//
// A non-nil cfg is copied and any unset (zero or negative) MaxSize, BufferSize
// or FlushInterval is replaced by its default, so calling FileConfig.Load
// beforehand is optional and cfg itself is never modified. The log directory
// is created and the file is checked for writing before NewLogger returns.
// Invalid file destinations and buffers exceeding the rotation size return
// an error. File entries are buffered according to BufferSize and FlushInterval;
// DPanic and higher levels flush immediately through zap's core. Sync and
// closeFn also flush pending entries. An abrupt exit can lose buffered entries.
//
// A file buffer retains its first write error: later Sync and closeFn calls
// return it, and logging to that file requires constructing a new logger after
// the underlying problem is fixed. The buffer is not reset automatically
// because this could discard pending data.
//
// Sink failures, and zap's own "write error" diagnostics, are written to stderr
// through a single shared limiter: the first message is written immediately and
// later ones at most once per minute, preceded by a count of the messages that
// were suppressed. Without it, a failed buffer would produce one stderr line
// per log call.
//
// Call closeFn during normal shutdown, after all goroutines that use the logger
// have stopped. It flushes pending entries, stops the file buffer's background
// goroutine, and closes the rotating file. It is idempotent and concurrency-safe;
// subsequent calls return the result of the first shutdown attempt. Logging
// after closeFn is unsupported: entries are no longer flushed periodically and
// the rotating file may be reopened.
func NewLogger(cfg *FileConfig) (l *Logger, closeFn func() error, err error) {
	return newLogger(cfg, os.Stderr, time.Now)
}

// newLogger is NewLogger with the diagnostics destination and clock injected
// so tests can observe and control error reporting.
func newLogger(
	cfg *FileConfig,
	diagnostics io.Writer,
	now func() time.Time,
) (l *Logger, closeFn func() error, err error) {
	l = &Logger{
		// AtomicLevel allows the minimum log level to be changed dynamically
		// without rebuilding the logger or introducing data races.
		atomicLevel: zap.NewAtomicLevelAt(zap.InfoLevel),
	}

	var (
		// One limiter for every diagnostic of this logger: sink failures reported
		// by reportingWriteSyncer and zap's per-entry "write error" messages.
		errOut = newRateLimitedWriter(diagnostics, sinkErrorReportInterval, now)

		sink zapcore.WriteSyncer
	)

	if cfg != nil {
		cfgc := *cfg
		cfgc.applyDefault()

		// BufferSize is in bytes; lumberjack's MaxSize is in megabytes.
		if int64(cfgc.BufferSize) > int64(cfgc.MaxSize)*1024*1024 {
			return nil, nil, ErrBufferSizeOverMaxSize
		}

		filename := cfg.Filename
		if filename == "" {
			filename = filepath.Join(os.TempDir(), filepath.Base(os.Args[0])+"-lumberjack.log")
		}

		err = checkLogFile(filename)
		if err != nil {
			return nil, nil, err
		}
		// Lumberjack owns log rotation and retention. BufferedWriteSyncer is
		// layered on top of it to reduce the number of writes to the file.
		l.lumberjack = &lumberjack.Logger{
			Filename:   filename,
			MaxSize:    cfgc.MaxSize,
			MaxBackups: cfgc.MaxBackups,
			MaxAge:     cfgc.MaxAge,
			Compress:   cfgc.Compress,
		}
		l.bufferedSyncer = &zapcore.BufferedWriteSyncer{
			WS: &reportingWriteSyncer{
				WriteSyncer: zapcore.AddSync(l.lumberjack),
				errorOutput: errOut,
			},
			Size:          cfgc.BufferSize,
			FlushInterval: cfgc.FlushInterval,
		}
		sink = l.bufferedSyncer
	} else {
		var (
			info, statErr = os.Stdout.Stat()
			ws            = zapcore.AddSync(os.Stdout)
		)

		// Terminals and other character devices (including /dev/null), pipes and
		// sockets do not support fsync. Detecting them from the file mode avoids
		// os.Stdout.Fd(), which can switch the descriptor to blocking mode.
		//
		// Pipes can return EBADF from fsync even while writes work
		// (e.g. on macOS). Detect streams instead of ignoring EBADF,
		// which would also hide genuinely invalid descriptors.
		isStream := statErr == nil &&
			info.Mode()&(os.ModeCharDevice|os.ModeNamedPipe|os.ModeSocket) != 0
		if isStream {
			ws = noSyncWriteSyncer{ws}
		} else {
			ws = stdoutWriteSyncer{ws}
		}
		sink = zapcore.Lock(ws)
	}

	l.syncer = sink
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		sink,
		l.atomicLevel,
	)

	l.logger = zap.New(
		core,

		// Include the source file and line number of each log call.
		zap.AddCaller(),

		// Skip Logger.Info/Debug/etc. so the reported caller is the code that
		// invoked this Logger wrapper rather than the wrapper method itself.
		zap.AddCallerSkip(1),

		// Rate-limit zap's own diagnostics for failed writes (see above).
		zap.ErrorOutput(errOut),
	)
	return l, l.close, nil
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
// Sync does not stop the file buffer's background flushing goroutine or
// release resources. The logger remains usable afterward unless its file
// buffer has failed. Use closeFn for final shutdown.
//
// Errors returned when syncing a non-seekable descriptor such as stdout
// (e.g. ENOTTY/EINVAL) are filtered by the stdout sink before they can be
// combined with errors flushing buffered entries. Write errors are preserved.
func (l *Logger) Sync() error {
	return l.syncer.Sync()
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
			err := l.bufferedSyncer.Stop()
			if err != nil {
				errs = append(errs, err)
			}
		} else if err := l.syncer.Sync(); err != nil {
			errs = append(errs, err)
		}

		// Stop has already flushed buffered data before the underlying
		// lumberjack writer is closed, so no buffered writes should remain.
		if l.lumberjack != nil {
			err := l.lumberjack.Close()
			if err != nil {
				errs = append(errs, err)
			}
		}

		// Preserve all shutdown errors instead of returning only the first one.
		l.closeErr = errors.Join(errs...)
	})
	return l.closeErr
}

// checkLogFile probes the destination without starting lumberjack's maintenance
// goroutine or retaining a descriptor on constructor failure. This only checks
// startup access; later permission changes and disk failures remain runtime errors.
//
// The probe may create the file, and lumberjack reuses an existing file as is,
// so the mode used here becomes the log file's mode. It is 0600 to match what
// lumberjack itself uses for new files: logs often contain sensitive data.
func checkLogFile(filename string) error {
	err := os.MkdirAll(filepath.Dir(filename), 0o755)
	if err != nil {
		return fmt.Errorf("create log directory: %w", err)
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open log file %q: %w", filename, err)
	}

	err = file.Close()
	if err != nil {
		return fmt.Errorf("close log file probe %q: %w", filename, err)
	}
	return nil
}

// noSyncWriteSyncer wraps a WriteSyncer whose Sync should be a no-op —
// typically because the underlying descriptor is a terminal, pipe, or socket, where
// fsync-style semantics don't apply and the OS's error code for a failed
// flush attempt is platform-specific (EINVAL/ENOTTY on Unix,
// ERROR_INVALID_HANDLE on Windows, etc.). Skipping the syscall entirely
// avoids depending on an exhaustive, per-platform errno list.
type noSyncWriteSyncer struct {
	zapcore.WriteSyncer
}

func (noSyncWriteSyncer) Sync() error { return nil }

// stdoutWriteSyncer filters unsupported fsync errors at the sink, before
// they are combined with potentially fatal write errors.
type stdoutWriteSyncer struct {
	zapcore.WriteSyncer
}

func (s stdoutWriteSyncer) Sync() error {
	err := s.WriteSyncer.Sync()
	if isUnsyncable(err) {
		return nil
	}
	return err
}

// isUnsyncable reports whether err is one of the well-known errors returned
// when syncing a non-seekable file descriptor such as stdout or stderr.
//
// On some platforms, calling fsync-like operations on streams can return
// ENOTTY or EINVAL. Those errors do not indicate that buffered log data was
// lost. Only an error consisting entirely of these codes is ignored by the
// stdout sink; a bad descriptor or a mixed error must still be reported.
func isUnsyncable(err error) bool {
	switch e := err.(type) {
	case interface{ Unwrap() []error }:
		errs := e.Unwrap()
		if len(errs) == 0 {
			return false
		}
		for _, child := range errs {
			if !isUnsyncable(child) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return isUnsyncable(e.Unwrap())
	}
	return errors.Is(err, syscall.EINVAL) ||
		errors.Is(err, syscall.ENOTTY)
}
