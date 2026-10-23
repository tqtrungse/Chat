/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package http

import (
	"net/http"
	"time"

	"xxx/pkg/log"

	"github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

type requestLogFormatter struct {
	logger *log.Logger
}

// NewLogEntry captures the request method and path for completion and panic logs.
func (f requestLogFormatter) NewLogEntry(r *http.Request) middleware.LogEntry {
	return &requestLogEntry{
		logger: f.logger,
		fields: []zap.Field{
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
		},
	}
}

type requestLogEntry struct {
	logger *log.Logger
	fields []zap.Field
}

// Write logs the completed request status, response size, and elapsed time at Info level.
func (e *requestLogEntry) Write(
	status int,
	bytes int,
	_ http.Header,
	elapsed time.Duration,
	_ any,
) {
	if status == 0 {
		status = http.StatusOK
	}
	fields := append(
		e.fields,
		zap.Int("status", status),
		zap.Int("bytes", bytes),
		zap.Duration("duration", elapsed),
	)
	e.logger.Info("HTTP request", fields...)
}

// Panic logs the recovered panic value and stack together with the request fields.
func (e *requestLogEntry) Panic(v any, stack []byte) {
	fields := append(e.fields, zap.Any("panic", v), zap.ByteString("stack", stack))
	e.logger.Error("HTTP request panicked", fields...)
}
