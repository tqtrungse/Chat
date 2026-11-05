/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package log

import (
	"time"

	"xxx/pkg/config"
)

const (
	defaultQueueSize        = 1 << 14 // 16384 entries
	defaultBufferSize       = 256 * 1024
	defaultFlushInterval    = 200 * time.Millisecond
	defaultSyncTimeout      = 5 * time.Second
	defaultSampleFirst      = 100
	defaultSampleThereafter = 1000

	// minBufferSize is the smallest buffer that bufio.NewWriterSize keeps.
	// bufio makes a smaller value larger and gives no message. This package
	// rejects such a value, because it cannot apply the value of the caller.
	minBufferSize = 16
)

func DefaultConfig() Config {
	cfg := Config{}
	cfg.applyDefault()
	return cfg
}

// Config sets the behavior of the asynchronous stdout sink.
//
// The logger encodes each entry as JSON. Then it sends the entry to one
// writer goroutine through a queue of a fixed size. A producer never waits.
// If the queue is full, the sink discards the entry and counts it. Therefore,
// a slow log collector or a full pipe cannot stop the goroutine that wrote
// the log.
type Config struct {
	// QueueSize is the number of encoded entries that can wait for the
	// writer goroutine. If the queue is full, the sink discards each new
	// entry and counts it. The sink does this until the writer goroutine is
	// ready again.
	//
	// Default: 16384
	QueueSize int

	// BufferSize is the size in bytes of the buffer between the writer
	// goroutine and stdout. This size sets the maximum quantity of data in
	// one write syscall. It also sets the maximum quantity of data that an
	// unexpected exit can lose.
	//
	// Default: 256 KB
	BufferSize int

	// FlushInterval is the time between two flushes of a buffer that is not
	// empty and not full. It is the maximum time that an entry stays in
	// memory before stdout shows the entry.
	//
	// Default: 200ms
	FlushInterval time.Duration

	// SyncTimeout is the maximum time that Sync and the shutdown function
	// wait for the writer goroutine to empty the queue. If stdout is
	// blocked, the shutdown gives a late error and does not stop.
	//
	// Default: 5s
	SyncTimeout time.Duration

	// SampleFirst is the number of entries with the same message that the
	// sampler keeps in each second. The sampler applies only to the levels
	// below Error. It does not apply to Error and the levels above Error.
	//
	// Default: 100
	SampleFirst int

	// SampleThereafter is the sample rate after SampleFirst entries. The
	// sampler keeps one entry in SampleThereafter entries. It discards the
	// other entries.
	//
	// Default: 1000
	SampleThereafter int

	// DisableSampling stops the sampler. Then the sink queues each entry
	// that the level filter permits. Use this value in development and in a
	// test that counts entries. In production, it removes the only protection
	// against one message that repeats and fills the log stream.
	//
	// Default: false
	DisableSampling bool
}

// LoadConfig reads the sink settings from loader and applies defaults. A nil loader
// is an empty source.
func LoadConfig(loader config.Loader) Config {
	var c Config
	if loader != nil {
		c.QueueSize = loader.GetInt("LOGGER_QUEUE_SIZE")
		c.BufferSize = loader.GetInt("LOGGER_BUFFER_SIZE")
		c.FlushInterval = loader.GetDuration("LOGGER_FLUSH_INTERVAL")
		c.SyncTimeout = loader.GetDuration("LOGGER_SYNC_TIMEOUT")
		c.SampleFirst = loader.GetInt("LOGGER_SAMPLE_FIRST")
		c.SampleThereafter = loader.GetInt("LOGGER_SAMPLE_THEREAFTER")
		c.DisableSampling = loader.GetBool("LOGGER_DISABLE_SAMPLING")
	}
	c.applyDefault()
	return c
}

// applyDefault puts the default value in each field that is zero or
// negative. NewLogger calls applyDefault on a copy of the Config of the
// caller. Therefore, a call to LoadConfig before NewLogger is not necessary, and
// NewLogger does not change the Config of the caller.
func (c *Config) applyDefault() {
	if c.QueueSize <= 0 {
		c.QueueSize = defaultQueueSize
	}
	if c.BufferSize <= 0 {
		c.BufferSize = defaultBufferSize
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = defaultFlushInterval
	}
	if c.SyncTimeout <= 0 {
		c.SyncTimeout = defaultSyncTimeout
	}
	if c.SampleFirst <= 0 {
		c.SampleFirst = defaultSampleFirst
	}
	if c.SampleThereafter <= 0 {
		c.SampleThereafter = defaultSampleThereafter
	}
}

// validate finds a configuration value that this package cannot apply.
// validate runs after applyDefault. Therefore, it rejects only a value that
// the caller set.
func (c *Config) validate() error {
	if c.BufferSize < minBufferSize {
		return ErrBufferSizeTooSmall
	}
	return nil
}
