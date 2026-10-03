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
	"time"

	"xxx/pkg"
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
	// before flushing.
	//
	// Default: 256 KB
	BufferSize int

	// FlushInterval specifies how often the writer should flush data if
	// there have been no writes.
	//
	// Default: 30s
	FlushInterval time.Duration
}

func (f *FileConfig) Load(loader pkg.ConfigLoader) {
	if loader == nil {
		return
	}
	f.Filename = loader.GetString("LOGGER_FILENAME")
	f.MaxSize = loader.GetInt("LOGGER_MAX_SIZE")
	f.MaxBackups = loader.GetInt("LOGGER_MAX_BACKUPS")
	f.MaxAge = loader.GetInt("LOGGER_MAX_AGE")
	f.Compress = loader.GetBool("LOGGER_COMPRESS")
	f.BufferSize = loader.GetInt("LOGGER_BUFFER_SIZE")
	f.FlushInterval = loader.GetDuration("LOGGER_FLUSH_INTERVAL")
}
