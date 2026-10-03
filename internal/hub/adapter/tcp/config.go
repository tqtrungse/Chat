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

package tcp

import (
	"time"

	"xxx/pkg"
)

type Config struct {
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
	ReadBufferCap uint16

	// WriteBufferCap is the maximum number of bytes that a static outbound buffer can hold,
	// if the data exceeds this value.
	//
	// Note that WriteBufferCap will always be converted to the least power of two integer value greater than
	// or equal to its real amount.
	//
	// Default: 4KB
	WriteBufferCap uint16
}

func (c *Config) Load(loader pkg.ConfigLoader) {
	if loader == nil {
		return
	}
	c.Addr = loader.GetString("TCP_SERVER_ADDRESS")
	c.TCPKeepAlive = loader.GetDuration("TCP_SERVER_KEEP_ALIVE")
	c.TCPKeepInterval = loader.GetDuration("TCP_SERVER_KEEP_INTERVAL")
	c.TCPKeepCount = loader.GetInt("TCP_SERVER_KEEP_COUNT")
	c.ReadBufferCap = loader.GetUint16("TCP_SERVER_READ_BUFFER")
	c.WriteBufferCap = loader.GetUint16("TCP_SERVER_WRITE_BUFFER")
}
