/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package sql

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"xxx/pkg/log"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	gormlogger "gorm.io/gorm/logger"
)

// Messages of the bridge. The sampler of pkg/log keys on the message, so each
// message is a constant and each variable part is a field.
const (
	msgStatement       = "sql"
	msgSlowStatement   = "slow sql"
	msgFailedStatement = "sql failed"
	msgNotice          = "gorm notice"
)

// gormLogger sends the logs of GORM to the logger of this project.
//
// Without this bridge GORM takes logger.Default, which writes plain text with
// ANSI colors to os.Stdout and bypasses the sink and the sampler of pkg/log.
type gormLogger struct {
	logger        *log.Logger
	slowThreshold time.Duration
	// level is the level of GORM, not of pkg/log. GORM replaces it through
	// LogMode, and DB.Debug and Session depend on that replacement. The level
	// of pkg/log stays the real gate of a statement.
	level     gormlogger.LogLevel
	logParams bool
}

// newGormLogger builds the bridge over a component logger named gorm.
//
// The level of GORM starts at Info so that Trace reaches the branch of a
// normal statement. That branch writes at Debug, and pkg/log runs at Info.
// Therefore, a normal process writes no statement. log.Logger.SetLevel turns
// the full trace on at run time.
func newGormLogger(
	logger *log.Logger,
	slowThreshold time.Duration,
	logParams bool,
) gormlogger.Interface {
	return &gormLogger{
		logger:        logger.Component("gorm"),
		slowThreshold: slowThreshold,
		level:         gormlogger.Info,
		logParams:     logParams,
	}
}

// LogMode gives a copy at the level that GORM asks for. The receiver must not
// change, because GORM keeps it for the other sessions.
func (l *gormLogger) LogMode(level gormlogger.LogLevel) gormlogger.Interface {
	next := *l
	next.level = level
	return &next
}

func (l *gormLogger) Info(_ context.Context, msg string, data ...any) {
	if l.level >= gormlogger.Info {
		l.logger.Info(msgNotice, zap.String("detail", fmt.Sprintf(msg, data...)))
	}
}

func (l *gormLogger) Warn(_ context.Context, msg string, data ...any) {
	if l.level >= gormlogger.Warn {
		l.logger.Warn(msgNotice, zap.String("detail", fmt.Sprintf(msg, data...)))
	}
}

func (l *gormLogger) Error(_ context.Context, msg string, data ...any) {
	if l.level >= gormlogger.Error {
		l.logger.Error(msgNotice, zap.String("detail", fmt.Sprintf(msg, data...)))
	}
}

// Trace reports the statement that GORM just ran. A failure goes to Error, a
// statement over slowThreshold goes to Warn, and each other statement goes to
// Debug.
//
// fc is a callback of GORM. A call to fc() gives the full SQL statement. It
// also gives the number of rows that the statement changed.
//
// fc renders the statement through Dialector.Explain, which costs a pass over
// the statement and its parameters. Therefore, Trace calls fc only after the
// branch that writes knows that the level accepts the entry.
func (l *gormLogger) Trace(
	_ context.Context,
	begin time.Time,
	fc func() (string, int64),
	err error,
) {
	if l.level <= gormlogger.Silent {
		return
	}

	elapsed := time.Since(begin)

	switch {
	// ErrRecordNotFound is the normal control flow of a repository, and not
	// a failure of the database.
	case err != nil && !errors.Is(err, gormlogger.ErrRecordNotFound):
		if l.level < gormlogger.Error {
			return
		}
		sql, rows := fc()
		l.logger.Error(
			msgFailedStatement,
			append(statementFields(sql, rows, elapsed), zap.Error(err))...,
		)

	case l.slowThreshold > 0 && elapsed > l.slowThreshold:
		if l.level < gormlogger.Warn {
			return
		}
		sql, rows := fc()
		l.logger.Warn(msgSlowStatement, statementFields(sql, rows, elapsed)...)

	default:
		if l.level < gormlogger.Info || l.logger.GetLevel() > zapcore.DebugLevel {
			return
		}
		sql, rows := fc()
		l.logger.Debug(msgStatement, statementFields(sql, rows, elapsed)...)
	}
}

// ParamsFilter keeps the parameters of a statement out of the log unless the
// configuration asks for them. GORM inlines each parameter into the statement
// that Trace reports, and those parameters hold the data of the user.
//
// GORM asks the logger of the statement for this filter. DB.Scan is the one
// finisher that replaces that logger with logger.Recorder, which reads the
// package-level logger.RecorderParamsFilter of GORM instead. Therefore a
// statement of DB.Scan reports each parameter. No repository uses DB.Scan;
// the one that starts to use it must set that filter of GORM as well.
func (l *gormLogger) ParamsFilter(
	_ context.Context,
	sql string,
	params ...any,
) (string, []any) {
	if l.logParams {
		return sql, params
	}
	return sql, nil
}

func statementFields(sql string, rows int64, elapsed time.Duration) []zap.Field {
	return []zap.Field{
		zap.String("sql", sql),
		zap.Int64("rows", rows),
		zap.Duration("elapsed", elapsed),
		zap.String("caller", statementCaller()),
	}
}

// statementCaller gives the line of the project that started the statement.
//
// The component logger removes the caller field, because the caller of a GORM
// entry sits a variable number of frames deep in that library.
// utils.FileWithLineNum of GORM is no help here: it removes the frames of its
// own source directory, and this bridge is outside that directory, so it would
// report this file. The walk below starts above Trace and removes what is left
// of GORM by the name of the package.
func statementCaller() string {
	// 0 is runtime.Callers, 1 is statementCaller, 2 is statementFields and 3
	// is Trace. The walk starts at the frame that called Trace.
	var pcs [16]uintptr
	frames := runtime.CallersFrames(pcs[:runtime.Callers(4, pcs[:])])
	for {
		frame, more := frames.Next()
		if frame.Function == "" {
			return ""
		}
		if !strings.HasPrefix(frame.Function, "gorm.io/") {
			return frame.File + ":" + strconv.Itoa(frame.Line)
		}
		if !more {
			return ""
		}
	}
}
