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

package cb

import (
	"time"

	"xxx/pkg"
)

type Config struct {
	// Name is the name of the circuit breaker.
	Name string

	// MaxRequests is the maximum number of requests allowed to pass through
	// when the circuit breaker is half-open.
	//
	// Default: 1
	MaxRequests uint32

	// Interval is the cyclic period of the closed state
	// for the circuit breaker to clear the internal Counts.
	//
	// If Interval is less than or equal to 0, the circuit breaker
	// does not clear internal Counts during the closed state.
	Interval time.Duration

	// Timeout is the period of the open state, after which the state
	// of the circuit breaker becomes half-open.
	//
	// Default: 60s
	Timeout time.Duration

	// ConsecutiveFailures represents the number of consecutive failed calls to a downstream service
	// without any intervening successful calls.
	// As soon as there is a successful call, the consecutive error counter will be reset to 0.
	// When the number of consecutive errors reaches the configured threshold, circuit breaker
	// will switch from CLOSED to OPEN state to stop sending further requests to protect the system.
	ConsecutiveFailures uint32
}

func (c *Config) Load(loader pkg.ConfigLoader) {
	if loader == nil {
		return
	}
	c.Name = loader.GetString("CIRCUIT_BREAKER_NAME")
	c.MaxRequests = loader.GetUint32("CIRCUIT_BREAKER_MAX_REQUESTS")
	c.Interval = loader.GetDuration("CIRCUIT_BREAKER_INTERVAL")
	c.Timeout = loader.GetDuration("CIRCUIT_BREAKER_TIMEOUT")
	c.ConsecutiveFailures = loader.GetUint32("CIRCUIT_BREAKER_CONSECUTIVE_FAILURES")

}
