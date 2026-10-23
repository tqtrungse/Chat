/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package bootstrap

import (
	"xxx/pkg/breaker"
	"xxx/pkg/config"
)

func loadDistCacheCircuitBreakerConfig(loader config.Loader) breaker.Config {
	var cfg breaker.Config
	if loader == nil {
		return cfg
	}

	cfg.Name = loader.GetString("DISTRIBUTED_CACHE_CIRCUIT_BREAKER_NAME")
	cfg.MaxRequests = loader.GetUint32("DISTRIBUTED_CACHE_CIRCUIT_BREAKER_MAX_REQUESTS")
	cfg.Interval = loader.GetDuration("DISTRIBUTED_CACHE_CIRCUIT_BREAKER_INTERVAL")
	cfg.Timeout = loader.GetDuration("DISTRIBUTED_CACHE_CIRCUIT_BREAKER_TIMEOUT")
	cfg.ConsecutiveFailures = loader.GetUint32("DISTRIBUTED_CACHE_CIRCUIT_BREAKER_CONSECUTIVE_FAILURE")
	return cfg
}
