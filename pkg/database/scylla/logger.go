/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package scylla

import (
	"fmt"
	"strings"

	"xxx/pkg/log"

	"github.com/gocql/gocql"
	"go.uber.org/zap"
)

// msgNotice is the message of each log entry that this bridge writes.
//
// gocql.StdLogger has no level and no field. Its methods Print, Printf, and
// Println only format one line of text.
//
// The sampler in pkg/log uses the message as its key. Therefore, the bridge
// puts the formatted line in a field. The message always stays this constant.
const msgNotice = "gocql notice"

// gocqlLogger sends the log of the gocql driver to the logger of this project.
//
// gocql takes its own defaultLogger if no code sets gocql.ClusterConfig.Logger.
// That defaultLogger writes plain text to os.Stdout through the standard log
// package. Such a line never reaches the sink in pkg/log.
//
// gocql gives each line to this bridge with no level. The driver reports a
// failure to dial a node, a fallback to the initial contact points, and a
// deprecated option. The driver also writes one line for each frame. Its own
// debug flag controls that line, and the flag is off by default. Therefore, a
// normal process gets no line for each frame.
//
// Warn reports each line. The default level in pkg/log is Info, so each line
// stays visible. Error is not correct here, because gocql retries such a
// condition without help.
type gocqlLogger struct {
	logger *log.Logger
}

// newGocqlLogger makes the bridge over a component logger with the name gocql.
//
// A nil logger gives a StdLogger that discards each line. pkg/database/sql
// does the same with gormlogger.Discard.
func newGocqlLogger(logger *log.Logger) gocql.StdLogger {
	if logger == nil {
		return nopLogger{}
	}
	return &gocqlLogger{
		logger: logger.Component("gocql"),
	}
}

func (l *gocqlLogger) Print(v ...any) {
	l.logger.Warn(msgNotice, zap.String("detail", fmt.Sprint(v...)))
}

func (l *gocqlLogger) Printf(format string, v ...any) {
	l.logger.Warn(msgNotice, zap.String("detail", fmt.Sprintf(format, v...)))
}

func (l *gocqlLogger) Println(v ...any) {
	// Sprintln adds a newline at the end of the text. A field of a structured
	// entry needs no such terminator, so TrimSuffix removes it.
	l.logger.Warn(msgNotice, zap.String("detail", strings.TrimSuffix(fmt.Sprintln(v...), "\n")))
}

// nopLogger discards every line. gocql exports no equivalent of its own
// nopLogger, so this bridge carries one for the nil-logger case.
type nopLogger struct{}

func (nopLogger) Print(...any)          {}
func (nopLogger) Printf(string, ...any) {}
func (nopLogger) Println(...any)        {}
