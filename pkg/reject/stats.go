/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package reject

import (
	"sync/atomic"
	"time"

	"xxx/pkg/cpu"
	"xxx/pkg/log"

	"go.uber.org/zap"
)

const defaultInterval = 5 * time.Second

// slot pad to align with cache lines: multiple event
// loops incrementing different counters must not interfere
// with one another (false sharing).
type slot struct {
	count    atomic.Uint64
	lastLog  atomic.Int64  // Unix nanoseconds of the last log entry
	lastSeen atomic.Uint64 // count value at the most recent log line
	_        [cpu.CacheLineSize - 24]byte
}

// Stats tracks event counts by reason, logging at most one 'Warn' entry
// per reason within each interval. Concurrency-safe, lock-free, and
// allocation-free on the fast path.
type Stats[R ~uint8 | ~uint16] struct {
	logger   *log.Logger
	interval int64
	names    []string
	slots    []slot
	now      func() int64 // can be injected for testing purposes
}

func New[R ~uint8 | ~uint16](
	logger *log.Logger,
	interval time.Duration,
	names []string,
) *Stats[R] {
	if interval <= 0 {
		interval = defaultInterval
	}

	return &Stats[R]{
		logger:   logger,
		interval: int64(interval),
		names:    names,
		slots:    make([]slot, len(names)),
		now:      func() int64 { return time.Now().UnixNano() },
	}
}

func (s *Stats[R]) Record(r R, err error) {
	if int(r) >= len(s.slots) {
		return
	}

	sl := &s.slots[r]
	n := sl.count.Add(1)

	now := s.now()
	last := sl.lastLog.Load()
	if now-last < s.interval || !sl.lastLog.CompareAndSwap(last, now) {
		return
	}

	prev := sl.lastSeen.Swap(n)
	s.logger.Warn(
		"client rejected",
		zap.String("reason", s.names[r]),
		zap.Uint64("since_last_log", n-prev),
		zap.Error(err), // If err == nil, zap skips the field
	)
}

func (s *Stats[R]) Count(r R) uint64 { return s.slots[r].count.Load() }

// Each iterates through all counters; used for the metric exporter.
func (s *Stats[R]) Each(fn func(reason string, count uint64)) {
	for i := range s.slots {
		fn(s.names[i], s.slots[i].count.Load())
	}
}
