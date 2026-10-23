/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package log

import (
	"fmt"
	"io"
	"sync"
	"time"

	"go.uber.org/zap/zapcore"
)

const sinkErrorReportInterval = time.Minute

// rateLimitedWriter forwards diagnostics to w, at most one message per
// interval. Messages arriving inside the interval are dropped and counted; the
// count is reported just before the next forwarded message.
//
// It is used both as zap's ErrorOutput and as the destination of
// reportingWriteSyncer, so that every diagnostic of one logger shares a single
// limit. This matters because once the file buffer has failed, zap reports a
// "write error" for every subsequent log call, which would otherwise flood
// stderr.
//
// Write never fails: diagnostics are best effort and must not feed back into
// the logger. Sync is a no-op so zap does not fsync stderr for each report.
type rateLimitedWriter struct {
	w        io.Writer
	interval time.Duration
	now      func() time.Time

	mu         sync.Mutex
	last       time.Time
	suppressed int
}

func newRateLimitedWriter(w io.Writer, interval time.Duration, now func() time.Time) *rateLimitedWriter {
	return &rateLimitedWriter{w: w, interval: interval, now: now}
}

func (r *rateLimitedWriter) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	t := r.now()
	if !r.last.IsZero() && t.Sub(r.last) < r.interval {
		r.suppressed++
		return len(p), nil
	}

	r.last = t
	if r.suppressed > 0 {
		_, _ = fmt.Fprintf(r.w, "logger: %d diagnostic message(s) suppressed\n", r.suppressed)
		r.suppressed = 0
	}
	_, _ = r.w.Write(p)
	return len(p), nil
}

func (*rateLimitedWriter) Sync() error { return nil }

// reportingWriteSyncer observes failures below the file buffer, including
// writes from its background flush loop, which zap itself does not surface.
// Diagnostics bypass the logger so a failing log file cannot recursively log
// its own errors.
//
// The buffer above keeps its first write error and never calls the underlying
// writer again, so this reports essentially the first failure; the limiting of
// repeated reports is done by errorOutput (see rateLimitedWriter).
type reportingWriteSyncer struct {
	zapcore.WriteSyncer
	errorOutput io.Writer
}

func (s *reportingWriteSyncer) Write(p []byte) (int, error) {
	n, err := s.WriteSyncer.Write(p)
	if err == nil && n < len(p) {
		err = io.ErrShortWrite
	}
	s.report(err)
	return n, err
}

func (s *reportingWriteSyncer) Sync() error {
	err := s.WriteSyncer.Sync()
	s.report(err)
	return err
}

func (s *reportingWriteSyncer) report(err error) {
	if err != nil {
		_, _ = fmt.Fprintf(s.errorOutput, "logger: file sink error: %v\n", err)
	}
}
