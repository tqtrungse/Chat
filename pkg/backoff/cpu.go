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

package backoff

import (
	"runtime"
	"time"
	_ "unsafe"

	// Automatically adjust GOMAXPROCS (the number of OS threads executing Go code concurrently)
	// to match the CPU quota in a containerized environment such as Docker or Kubernetes.
	_ "go.uber.org/automaxprocs"
)

const (
	spinLimit  = 6
	yieldLimit = 10

	sleepAfter = yieldLimit + 20
	sleepBase  = 50 * time.Microsecond
	sleepMax   = 5 * time.Millisecond
)

var multiOSThreads = runtime.GOMAXPROCS(0) > 1

// Cpu
// Makes the current thread to wait in the short time.
//
// It should be used to implement wait-retry in high contention multithreading environment
// because of improving performance significantly.
//
// It should not be used to replace other blocking mechanism.
type Cpu struct {
	step int
}

func (b *Cpu) Reset() {
	b.step = 0
}

// Spin backs off in a lock-free loop.
//
// This method should be used when we need to retry an operation because another thread made
// progress.
func (b *Cpu) Spin() {
	procYield(1 << min(b.step, spinLimit))
	if b.step <= spinLimit {
		b.step++
	}
}

// Snooze backs off in a blocking loop.
//
// This method should be used when we need to wait for another thread to make progress.
//
// If possible, use IsDone to check when it is advised to stop using backoff and
// block the current thread using a different synchronization mechanism instead.
func (b *Cpu) Snooze() {
	switch {
	case b.step <= spinLimit && multiOSThreads:
		procYield(1 << b.step)
	case b.step <= sleepAfter:
		runtime.Gosched()
	default:
		// Bounded real sleep: releases the OS thread entirely so a
		// starved goroutine can't be held off the CPU forever by
		// neighbors that only Gosched (the actual root cause of the
		// intermittent block under GOMAXPROCS > real CPU quota).
		shift := min(b.step-sleepAfter, 6)
		d := min(sleepBase<<uint(shift), sleepMax)
		time.Sleep(d)
	}
	// Unlike before, step keeps growing past yieldLimit -- needed so
	// Snooze() can reach the sleep tier. IsDone() below still trips at
	// the same yieldLimit threshold as before, so callers relying on it
	// (e.g. Mpsc.Pop's decision to stop spinning and park) see no change
	// in timing.
	b.step++
}

// IsDone returns `true` if exponential backoff has completed and blocking the thread is advised.
func (b *Cpu) IsDone() bool {
	return b.step > yieldLimit
}
