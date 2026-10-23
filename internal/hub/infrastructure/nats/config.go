/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package nats

import (
	"fmt"
	"time"

	"xxx/pkg/config"
)

const (
	defaultMaxHops        = uint8(5)
	defaultPublishTimeout = 30 * time.Second
	defaultInboxWorkers   = 128
)

type Config struct {
	// Based on the environment (e.g., "prod." vs. "staging.").
	HubSubjectPrefix string

	// MaxHops is the maximum number of inter-hub forwards.
	//
	// Default: 5, maximum is 120
	MaxHops uint8

	// PublishTimeout is retained for configuration compatibility. Twice this
	// duration bounds waiting for inbox handlers during shutdown. Route uses
	// buffered publishing and does not wait for server confirmation.
	//
	// Default: 30s
	PublishTimeout time.Duration

	// DeliveryBackOff defines delays between local delivery retries.
	// The number of entries is the maximum number of retry attempts.
	//
	// Default: [100ms, 500ms, 1s, 2s, 5s]
	DeliveryBackOff []time.Duration

	// InboxWorkers bounds concurrent handleInboxMessage goroutines.
	//
	// Default: 128
	InboxWorkers int
}

func (c *Config) Load(loader config.Loader) error {
	if loader != nil {
		c.HubSubjectPrefix = loader.GetString("BROKER_HUB_SUBJECT_PREFIX")
		c.MaxHops = loader.GetUint8("BROKER_MAX_HOPS")
		c.PublishTimeout = loader.GetDuration("BROKER_PUBLISH_TIMEOUT")
		c.InboxWorkers = loader.GetInt("BROKER_INBOX_WORKERS")
		c.DeliveryBackOff = nil
		for _, str := range config.SplitList(loader.GetString("BROKER_DELIVERY_BACKOFF")) {
			duration, err := time.ParseDuration(str)
			if err != nil || duration <= 0 {
				return fmt.Errorf("BROKER_DELIVERY_BACKOFF: invalid duration %q", str)
			}
			c.DeliveryBackOff = append(c.DeliveryBackOff, duration)
		}
	}
	if c.MaxHops == 0 {
		c.MaxHops = defaultMaxHops
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = defaultPublishTimeout
	}
	if c.InboxWorkers <= 0 {
		c.InboxWorkers = defaultInboxWorkers
	}
	if len(c.DeliveryBackOff) == 0 {
		c.DeliveryBackOff = append([]time.Duration(nil), defaultDeliveryBackoff...)
	}
	return nil
}
