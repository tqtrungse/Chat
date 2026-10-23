/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package cpu

import (
	"runtime"
	"time"
	_ "unsafe"
)

const (
	spinLimit  = 6
	yieldLimit = 10

	sleepAfter = yieldLimit + 20
	sleepBase  = 50 * time.Microsecond
	sleepMax   = 5 * time.Millisecond
)

// Backoff
// Makes the current thread to wait in the short time.
//
// It should be used to implement wait-retry in high contention multithreading environment
// because of improving performance significantly.
//
// It should not be used to replace other blocking mechanism.
type Backoff struct {
	step int
}

func (b *Backoff) Reset() {
	b.step = 0
}

// Spin backs off in a lock-free loop.
//
// This method should be used when we need to retry an operation because another thread made
// progress.
func (b *Backoff) Spin() {
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
func (b *Backoff) Snooze() {
	switch {
	case b.step <= spinLimit && runtime.GOMAXPROCS(0) > 1:
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
func (b *Backoff) IsDone() bool {
	return b.step > yieldLimit
}
