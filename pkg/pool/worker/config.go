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

package worker

import "xxx/pkg"

type Config struct {
	// NumWorkers sets the maximum workers in pool.
	//
	// Defaults to runtime.GOMAXPROCS(0)
	NumWorkers int

	// GlobalCheckInterval sets the number of tasks a worker runs
	// between priority checks of the global queue.
	//
	// Every this-many tasks a worker runs, it checks the shared global
	// queue FIRST, ahead of its own local queue. Without this, a worker
	// that keeps finding work locally (via steals or overflow refills)
	// could in principle starve a task sitting in the global queue indefinitely.
	//
	// Default: 64
	GlobalCheckInterval uint32

	// GlobalBatchSize caps how many tasks a worker pulls from
	// the global queue in one go: one to run immediately, the rest
	// seeded into its own local queue so subsequent pops for that
	// batch are cheap, single-owner, uncontended operations instead of
	// repeated trips to the shared (MPMC, more contended) global
	// queue.
	//
	// Default: 32
	GlobalBatchSize int
}

func (c *Config) Load(loader pkg.ConfigLoader) {
	if loader == nil {
		return
	}
	c.NumWorkers = loader.GetInt("WORKER_POOL_NUMBER_WORKERS")
	c.GlobalCheckInterval = loader.GetUint32("WORKER_GLOBAL_CHECK_INTERVAL")
	c.GlobalBatchSize = loader.GetInt("WORKER_GLOBAL_BATCH_SIZE")
}
