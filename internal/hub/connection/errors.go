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

package connection

import "errors"

var (
	// errSuperseded is returned to a batched request that lost to a newer
	// operation on the same device (see dedupeDeviceOps).
	errSuperseded = errors.New("device operation superseded")

	// ErrCbOpen is returned when the circuit is open, or half-open with a probe
	// already in flight, so the attempt did not run.
	ErrCbOpen = errors.New("cb: circuit breaker open")

	ErrRouterClosed     = errors.New("router closed")
	ErrConnReachMax     = errors.New("connection over")
	ErrSessionDuplicate = errors.New("session duplicate")
)
