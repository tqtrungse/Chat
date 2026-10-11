/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package activate_conn

import (
	"time"

	"xxx/pkg"
)

type Config struct {
	// PreAuthTimeout bounds the whole pre-auth window: accept -> proof verified.
	//
	// Default: 10s
	PreAuthTimeout time.Duration

	// MaxPreAuthConns caps connections that have not completed activation.
	//
	// Default: Router.MaxConns()/10 (at least 1)
	MaxPreAuthConns uint32
}

func (c *Config) Load(loader pkg.ConfigLoader) {
	if loader == nil {
		return
	}
	c.PreAuthTimeout = loader.GetDuration("ACTIVATE_PREAUTH_TIMEOUT")
	c.MaxPreAuthConns = loader.GetUint32("ACTIVATE_MAX_PREAUTH_CONNS")
}