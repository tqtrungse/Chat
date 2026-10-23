/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package worker

import (
	"runtime"
	"testing"

	"xxx/pkg/config"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestConfigLoadDefaults(t *testing.T) {
	for _, loader := range []config.Loader{nil, viper.New()} {
		var cfg Config
		cfg.Load(loader)
		require.Equal(
			t,
			Config{
				NumWorkers:          runtime.GOMAXPROCS(0),
				GlobalCheckInterval: 64,
				GlobalBatchSize:     32,
			},
			cfg,
		)
	}
}

func TestConfigLoadPreservesExplicitValues(t *testing.T) {
	l := viper.New()
	l.Set("WORKER_POOL_NUMBER_WORKERS", 3)
	l.Set("WORKER_GLOBAL_CHECK_INTERVAL", 8)
	l.Set("WORKER_GLOBAL_BATCH_SIZE", 4)

	var cfg Config
	cfg.Load(l)
	require.Equal(
		t,
		Config{
			NumWorkers:          3,
			GlobalCheckInterval: 8,
			GlobalBatchSize:     4,
		},
		cfg,
	)
}
