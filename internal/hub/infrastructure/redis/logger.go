/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package redis

import (
	"context"
	"fmt"

	"xxx/pkg/log"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/logging"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// goredisLogger sends the log of go-redis to the logger of this project.
//
// go-redis takes its own default logger if no code calls redis.SetLogger.
// That default logger writes plain text to os.Stderr through the standard
// log package. Such a line never reaches the sink in pkg/log. See
// InstallLogger for the call that replaces it.
//
// go-redis gives each line to this bridge with no level. The pool reports a
// failed dial and a lost connection through this method. The PubSub client
// reports a reconnect through the same method.
//
// Warn reports each line. The default level in pkg/log is Info, so each line
// stays visible. Error is not correct here, because go-redis retries such a
// condition without help.
type goredisLogger struct {
	logger *log.Logger
}

// newGoredisLogger builds the bridge over a component logger named redis.
//
// logger must not be nil. InstallLogger is the only caller of
// newGoredisLogger. InstallLogger gives a VoidLogger to redis.SetLogger
// instead of a nil logger.
func newGoredisLogger(logger *log.Logger) *goredisLogger {
	return &goredisLogger{
		logger: logger.Component("redis"),
	}
}

// Printf writes one line of the log of go-redis.
//
// format is a string constant at the call site in go-redis. Printf keeps
// format as the message. The sampler of pkg/log can then use the message as
// a stable key. params holds the complete line, with each value in place of
// its verb.
//
// go-redis gives this method a context. pkg/log takes no context. Printf
// drops the context.
func (l *goredisLogger) Printf(_ context.Context, format string, v ...any) {
	if l.logger.GetLevel() > zapcore.WarnLevel {
		return
	}
	l.logger.Warn(format, zap.String("params", fmt.Sprintf(format, v...)))
}

// InstallLogger sends the log of go-redis to logger.
//
// redis.SetLogger writes a variable of the go-redis package. Every client of
// the process shares that variable. Call InstallLogger one time, before the
// first client of go-redis. NewCache opens such a client. A call after that
// point can race with code that already reads the variable.
//
// A nil logger disables the log of go-redis.
//
// The component logger shares the sink of logger. A close of logger also
// closes the component logger. Do not close the component logger
// separately.
func InstallLogger(logger *log.Logger) {
	if logger == nil {
		redis.SetLogger(&logging.VoidLogger{})
		return
	}
	redis.SetLogger(newGoredisLogger(logger))
}
