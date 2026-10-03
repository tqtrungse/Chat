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

package nats

import (
	"time"

	"xxx/pkg"
)

type Config struct {
	// Based on the environment (e.g., "prod." vs. "staging.").
	HubSubjectPrefix string

	// MaxHops is the maximum number of inter-hub forwards.
	//
	// Default: 5, maximum is 120
	MaxHops uint8

	// PublishTimeout bounds waiting for the NATS connection to flush the
	// published message to the server.
	//
	// It does not guarantee that the destination hub received or processed
	// the message.
	//
	// Default: 30s
	PublishTimeout time.Duration

	// DeliveryRetryBackoff defines delays between local delivery retries.
	// The number of entries is the maximum number of retry attempts.
	//
	// Default: [100ms, 500ms, 1s, 2s, 5s]
	DeliveryBackOff []time.Duration

	// InboxWorkers bounds concurrent handleInboxMessage goroutines.
	//
	// Default: 128
	InboxWorkers int
}

func (c *Config) Load(loader pkg.ConfigLoader) {
	if loader == nil {
		return
	}

	var (
		idx             = 0
		strs            = loader.GetStringSlice("BROKER_DELIVERY_BACKOFF")
		deliveryBackOff = make([]time.Duration, len(strs))
	)
	for idx = range strs {
		deliveryBackOff[idx], _ = time.ParseDuration(strs[idx])
	}

	c.HubSubjectPrefix = loader.GetString("BROKER_HUB_SUBJECT_PREFIX")
	c.MaxHops = loader.GetUint8("BROKER_MAX_HOPS")
	c.PublishTimeout = loader.GetDuration("BROKER_PUBLISH_TIMEOUT")
	c.InboxWorkers = loader.GetInt("BROKER_INBOX_WORKERS")
	c.DeliveryBackOff = deliveryBackOff
}
