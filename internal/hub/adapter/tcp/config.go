/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package tcp

import (
	"strings"
	"time"

	"xxx/pkg/config"
)

const (
	defaultTcpKeepAlive     = 30 * time.Second
	defaultReadBufferCap    = 4 * 1024
	defaultWriteBufferCap   = 4 * 1024
	defaultMaxFrameBytes    = 4096
	defaultMaxFramesPerTick = 64
	defaultMaxBytesPerTick  = 256 * 1024
)

type Config struct {
	// MaxFrameBytes limits each frame body, excluding the 2-byte length prefix.
	// Bootstrap also applies this limit to the handshake and transport factory.
	//
	// Default: 4096 bytes.
	//
	// Bootstrap accepts: 512..65535.
	MaxFrameBytes int

	// MaxFramesPerTick limits complete frames processed per OnTraffic callback,
	// not per timer tick. On reaching either processing budget, the connection
	// schedules a Wake to resume buffered data after yielding the event loop.
	//
	// Default: 64.
	MaxFramesPerTick int

	// MaxBytesPerTick is the processing budget per OnTraffic callback, including
	// frame length prefixes. A complete frame is always processed, so the last
	// frame may exceed this budget before the callback yields.
	//
	// Default: 256 KiB.
	MaxBytesPerTick int

	Addr string

	// TCPKeepAlive enables the TCP keep-alive mechanism (SO_KEEPALIVE) and set its value
	// on TCP_KEEPIDLE.
	//
	// 1/5 of TCPKeepAlive will be set on TCP_KEEPINTVL,  and 5 will be set on TCP_KEEPCNT.
	//
	// Default: 30s
	TCPKeepAlive time.Duration

	// TCPKeepInterval is the value for TCP_KEEPINTVL, it's the interval between
	// TCP keep-alive probes.
	TCPKeepInterval time.Duration

	// TCPKeepCount is the number of keep-alive probes that will be sent before
	// the connection is considered dead and dropped.
	TCPKeepCount int

	// ReadBufferCap is the maximum number of bytes that can be read from the remote when the readable event comes.
	//
	// Note that ReadBufferCap will always be converted to the least power of two integer value greater than
	// or equal to its real amount.
	//
	// Default: 4KB
	ReadBufferCap int

	// WriteBufferCap is the maximum number of bytes that a static outbound buffer can hold,
	// if the data exceeds this value.
	//
	// Note that WriteBufferCap will always be converted to the least power of two integer value greater than
	// or equal to its real amount.
	//
	// Default: 4KB
	WriteBufferCap int
}

func (c *Config) Load(loader config.Loader) {
	if loader != nil {
		c.MaxFrameBytes = loader.GetInt("TCP_SERVER_MAX_FRAME_BYTES")
		c.MaxFramesPerTick = loader.GetInt("TCP_SERVER_MAX_FRAMES_PER_TICK")
		c.MaxBytesPerTick = loader.GetInt("TCP_SERVER_MAX_BYTES_PER_TICK")
		c.Addr = strings.TrimSpace(loader.GetString("TCP_SERVER_ADDRESS"))
		c.TCPKeepAlive = loader.GetDuration("TCP_SERVER_KEEP_ALIVE")
		c.TCPKeepInterval = loader.GetDuration("TCP_SERVER_KEEP_INTERVAL")
		c.TCPKeepCount = loader.GetInt("TCP_SERVER_KEEP_COUNT")
		c.ReadBufferCap = loader.GetInt("TCP_SERVER_READ_BUFFER")
		c.WriteBufferCap = loader.GetInt("TCP_SERVER_WRITE_BUFFER")
	}
	if c.TCPKeepAlive <= 0 {
		c.TCPKeepAlive = defaultTcpKeepAlive
	}
	if c.ReadBufferCap <= 0 {
		c.ReadBufferCap = defaultReadBufferCap
	}
	if c.WriteBufferCap <= 0 {
		c.WriteBufferCap = defaultWriteBufferCap
	}
	if c.MaxFrameBytes <= 0 {
		c.MaxFrameBytes = defaultMaxFrameBytes
	}
	if c.MaxFramesPerTick <= 0 {
		c.MaxFramesPerTick = defaultMaxFramesPerTick
	}
	if c.MaxBytesPerTick <= 0 {
		c.MaxBytesPerTick = defaultMaxBytesPerTick
	}
}
