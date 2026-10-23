/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package log

import (
	"time"

	"xxx/pkg/config"
)

const (
	defaultMaxSize       = 100 // megabytes, as required by lumberjack.Logger.MaxSize
	defaultBufferSize    = 256 * 1024
	defaultFlushInterval = 30 * time.Second
)

type FileConfig struct {
	// Filename is the file to write logs to. Backup log files will be retained
	// in the same directory. It uses <process name>-lumberjack.log in
	// os.TempDir() if empty.
	Filename string

	// MaxSize is the maximum size in megabytes of the log file before it gets
	// rotated.
	//
	// Default: 100MB
	MaxSize int

	// MaxBackups is the maximum number of old log files to retain. The default
	// is to retain all old log files (though MaxAge may still cause them to get
	// deleted.)
	MaxBackups int

	// MaxAge is the maximum number of days to retain old log files based on the
	// timestamp encoded in their filename.  Note that a day is defined as 24
	// hours and may not exactly correspond to calendar days due to daylight
	// savings, leap seconds, etc. The default is not to remove old log files
	// based on age.
	MaxAge int

	// Compress determines if the rotated log files should be compressed
	// using gzip.
	//
	// Default: false
	Compress bool

	// BufferSize specifies the maximum amount of data the writer will buffer
	// before flushing. It must not exceed MaxSize * 1024 * 1024 bytes.
	//
	// Default: 256 KB
	BufferSize int

	// FlushInterval specifies how often the file writer flushes buffered data.
	// Stdout logging does not use this buffer or interval.
	//
	// Default: 30s
	FlushInterval time.Duration
}

func (f *FileConfig) Load(loader config.Loader) {
	if loader != nil {
		f.Filename = loader.GetString("LOGGER_FILENAME")
		f.MaxSize = loader.GetInt("LOGGER_MAX_SIZE")
		f.MaxBackups = loader.GetInt("LOGGER_MAX_BACKUPS")
		f.MaxAge = loader.GetInt("LOGGER_MAX_AGE")
		f.Compress = loader.GetBool("LOGGER_COMPRESS")
		f.BufferSize = loader.GetInt("LOGGER_BUFFER_SIZE")
		f.FlushInterval = loader.GetDuration("LOGGER_FLUSH_INTERVAL")
	}
	f.applyDefault()
}

func (f *FileConfig) applyDefault() {
	if f.MaxSize <= 0 {
		f.MaxSize = defaultMaxSize
	}
	if f.BufferSize <= 0 {
		f.BufferSize = defaultBufferSize
	}
	// FlushInterval is only used in zapcore.BufferedWriteSyncer.
	// zapcore.BufferedWriteSyncer will set default 30s for FlushInterval if it is zero.
	// If it is negative, a panic occurs during the first `Write` call (due to lazy initialization)
	// rather than when the logger is created; consequently, configuration errors surface late—specifically,
	// at the moment a log is written.
	//
	// So we will set default in here to prevent lazy panic.
	if f.FlushInterval <= 0 {
		f.FlushInterval = defaultFlushInterval
	}
}
