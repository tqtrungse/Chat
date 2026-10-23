/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package ticketlimit

import (
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	core "xxx/internal/iam/core/ticketlimit"

	"github.com/stretchr/testify/require"
)

// fakeClock is a manually advanced clock. Times have no monotonic reading, so
// the limiter falls back to wall-clock subtraction, which is what we want.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// testConfig: subject 1/s burst 3 (window 3s); device 1/5s burst 2 (window 10s).
func testConfig() Config {
	return Config{
		SubjectInterval:      time.Second,
		SubjectBurst:         3,
		DeviceInterval:       5 * time.Second,
		DeviceBurst:          2,
		MaxSubjectEntries:    4,
		MaxDeviceEntries:     8,
		MaxDevicesPerSubject: 2,
	}
}

func newTestLimiter(cfg Config) (*Limiter, *fakeClock) {
	clk := newFakeClock()
	return newWithClock(cfg, clk.Now), clk
}

func requireAllowed(t *testing.T, d core.Decision) {
	t.Helper()
	require.Equal(t, core.Decision{}, d, "want allowed")
}

func requireDenied(t *testing.T, d core.Decision, reason core.Reason, retry int) {
	t.Helper()
	require.Equal(t, core.Decision{Reason: reason, RetryAfter: retry}, d)
}

func requireCapacityDenied(t *testing.T, d core.Decision, reason core.Reason) {
	t.Helper()
	require.Equal(t, reason, d.Reason)
	require.GreaterOrEqual(t, d.RetryAfter, 5)
	require.LessOrEqual(t, d.RetryAfter, 10)
}

// checkInvariants verifies heap/map consistency and devicesPerSubject
// bookkeeping. It must only run while no other goroutine is calling allow.
func checkInvariants(t testing.TB, l *Limiter) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()

	perSubject := make(map[string]int)
	for name, table := range map[string]*ticketRateTable{
		"subjects": l.subjects,
		"devices":  l.devices,
	} {
		require.LessOrEqualf(
			t,
			len(table.entries),
			table.maxEntries,
			"%s: entries exceed capacity", name,
		)
		require.Lenf(
			t,
			table.heap,
			len(table.entries),
			"%s: heap/map size mismatch", name,
		)
		for i, b := range table.heap {
			require.Equalf(
				t,
				i,
				b.index,
				"%s: bucket index mismatch", name,
			)
			require.Samef(
				t,
				b,
				table.entries[b.key],
				"%s: heap bucket %+v missing from map", name, b.key,
			)

			if i > 0 {
				require.LessOrEqualf(
					t,
					table.heap[(i-1)/2].tat,
					b.tat,
					"%s: heap order violated at %d", name, i,
				)
			}
			require.Equalf(
				t,
				name == "devices",
				b.key.device != 0,
				"%s: bucket with device=%d in wrong table", name, b.key.device,
			)
			if b.key.device != 0 {
				perSubject[b.key.subject]++
			}
		}
	}

	require.Len(t, l.devicesPerSubject, len(perSubject))
	for s, n := range perSubject {
		require.Equalf(
			t,
			n,
			l.devicesPerSubject[s],
			"devicesPerSubject[%q]", s,
		)
		require.LessOrEqualf(
			t,
			n,
			l.maxDevicesPerSubject,
			"subject %q exceeds device capacity", s,
		)
	}
}

func TestBurstRefillAndRetryAfter(t *testing.T) {
	l, clk := newTestLimiter(testConfig())

	for range 3 {
		requireAllowed(t, l.AllowSubject("a"))
	}
	requireDenied(t, l.AllowSubject("a"), core.SubjectLimited, 1)

	clk.Advance(400 * time.Millisecond) // 600ms to the next token -> rounds up to 1s
	requireDenied(t, l.AllowSubject("a"), core.SubjectLimited, 1)

	clk.Advance(600 * time.Millisecond)
	requireAllowed(t, l.AllowSubject("a"))
	requireDenied(t, l.AllowSubject("a"), core.SubjectLimited, 1)

	// Device: interval 5s, burst 2.
	requireAllowed(t, l.AllowDevice("a", 1))
	requireAllowed(t, l.AllowDevice("a", 1))
	requireDenied(t, l.AllowDevice("a", 1), core.DeviceLimited, 5)

	clk.Advance(time.Second)
	requireDenied(t, l.AllowDevice("a", 1), core.DeviceLimited, 4)

	clk.Advance(500 * time.Millisecond) // 3.5s left -> 4
	requireDenied(t, l.AllowDevice("a", 1), core.DeviceLimited, 4)

	clk.Advance(3500 * time.Millisecond)
	requireAllowed(t, l.AllowDevice("a", 1))
	checkInvariants(t, l)
}

func TestRejectedRequestsAreFree(t *testing.T) {
	l, clk := newTestLimiter(testConfig())

	for range 3 {
		requireAllowed(t, l.AllowSubject("a"))
	}

	for range 1000 {
		requireDenied(t, l.AllowSubject("a"), core.SubjectLimited, 1)
	}

	clk.Advance(time.Second) // exactly one token, despite the spam
	requireAllowed(t, l.AllowSubject("a"))
	requireDenied(t, l.AllowSubject("a"), core.SubjectLimited, 1)
}

func TestRefillGranularity(t *testing.T) {
	l, clk := newTestLimiter(testConfig())

	for range 3 {
		requireAllowed(t, l.AllowSubject("a"))
	}

	clk.Advance(3*time.Second - time.Millisecond) // 2.999 tokens
	requireAllowed(t, l.AllowSubject("a"))
	requireAllowed(t, l.AllowSubject("a"))
	requireDenied(t, l.AllowSubject("a"), core.SubjectLimited, 1)

	clk.Advance(time.Millisecond)
	requireAllowed(t, l.AllowSubject("a"))
}

func TestPruneBoundary(t *testing.T) {
	cfg := testConfig()
	cfg.SubjectBurst = 1 // a bucket admitted at t0 is full again at t0+1s
	l, clk := newTestLimiter(cfg)

	requireAllowed(t, l.AllowSubject("a"))
	clk.Advance(time.Second - time.Nanosecond)
	requireAllowed(t, l.AllowSubject("b"))
	require.Len(
		t,
		l.subjects.entries,
		2,
		"a must survive until tat <= now",
	)

	clk.Advance(time.Nanosecond)
	requireAllowed(t, l.AllowSubject("c"))
	require.NotContains(
		t,
		l.subjects.entries,
		ticketRateKey{subject: "a"},
		"a should be pruned once tat == now",
	)
	require.Len(
		t,
		l.subjects.entries,
		2,
		"want b and c",
	)
	checkInvariants(t, l)
}

func TestCapacityNeverEvictsActiveBuckets(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubjectEntries = 2
	l, clk := newTestLimiter(cfg)

	requireAllowed(t, l.AllowSubject("a"))
	requireAllowed(t, l.AllowSubject("b"))

	for range 50 {
		d := l.AllowSubject("c")
		requireCapacityDenied(t, d, core.SubjectCapacity)
		require.True(
			t,
			d.Overloaded(),
			"subject capacity must be reported as shared capacity exhaustion",
		)
	}
	// Existing buckets keep their own quota; churn did not reset anything.
	requireAllowed(t, l.AllowSubject("a"))
	requireAllowed(t, l.AllowSubject("a"))
	requireDenied(t, l.AllowSubject("a"), core.SubjectLimited, 1)

	clk.Advance(3 * time.Second) // window elapsed: a and b are expendable
	requireAllowed(t, l.AllowSubject("c"))
	checkInvariants(t, l)
}

func TestTablesAreIndependent(t *testing.T) {
	cfg := testConfig()
	cfg.MaxDeviceEntries = 1
	l, _ := newTestLimiter(cfg)

	requireAllowed(t, l.AllowDevice("a", 1))
	d := l.AllowDevice("a", 2)
	requireCapacityDenied(t, d, core.DeviceCapacity)
	require.True(
		t,
		d.Overloaded(),
		"device capacity must be reported as shared capacity exhaustion",
	)
	// A full device table does not block new subject buckets.
	requireAllowed(t, l.AllowSubject("zzz"))
}

func TestPerSubjectDeviceCap(t *testing.T) {
	l, clk := newTestLimiter(testConfig())

	requireAllowed(t, l.AllowDevice("a", 1))
	requireAllowed(t, l.AllowDevice("a", 2))
	d := l.AllowDevice("a", 3)
	requireCapacityDenied(t, d, core.SubjectDeviceCapacity)
	require.False(
		t,
		d.Overloaded(),
		"per-subject cap must be reported as caller quota, not shared overload",
	)

	requireAllowed(t, l.AllowDevice("b", 3)) // other subjects unaffected
	requireAllowed(t, l.AllowDevice("a", 1)) // existing buckets still served
	checkInvariants(t, l)

	clk.Advance(10 * time.Second) // device window: everything expired
	requireAllowed(t, l.AllowDevice("a", 3))
	require.Equal(t, 1, l.devicesPerSubject["a"])
	require.NotContains(
		t,
		l.devicesPerSubject,
		"b",
		"zero counters must be deleted",
	)
	checkInvariants(t, l)
}

// Active buckets must not prevent the heap from reclaiming replenished buckets
// with smaller tat values, otherwise storage capacity is falsely exhausted.
func TestActiveBucketDoesNotBlockPrune(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubjectEntries = 3
	l, clk := newTestLimiter(cfg)

	requireAllowed(t, l.AllowSubject("a"))
	requireAllowed(t, l.AllowSubject("b"))
	requireAllowed(t, l.AllowSubject("c"))
	// Two requests per interval keep "a" non-idle (tat stays ahead of now)
	// while "b" and "c" go idle and expire.
	for range 4 {
		clk.Advance(500 * time.Millisecond)
		requireAllowed(t, l.AllowSubject("a"))
	}

	requireAllowed(t, l.AllowSubject("d"))
	require.Len(
		t,
		l.subjects.entries,
		2,
		"want only a and d to remain",
	)
	checkInvariants(t, l)
}

// Regression: list order (last admission) differs from expiry order (tat). An
// expired bucket sitting behind a still-active one must be reclaimed at once,
// otherwise it holds a slot and a table-full 503 happens with free capacity.
func TestExpiredBucketBehindActiveOneIsReclaimed(t *testing.T) {
	cfg := testConfig()
	cfg.MaxSubjectEntries = 2
	l, clk := newTestLimiter(cfg)

	for range 3 {
		requireAllowed(t, l.AllowSubject("A")) // tat = 3s
	}
	requireAllowed(t, l.AllowSubject("B")) // tat = 1s, admitted after A

	clk.Advance(1100 * time.Millisecond) // B is full again, A is not
	requireAllowed(t, l.AllowSubject("C"))
	require.NotContains(
		t,
		l.subjects.entries,
		ticketRateKey{subject: "B"},
		"B is full again and must have been reclaimed",
	)
	require.Contains(
		t,
		l.subjects.entries,
		ticketRateKey{subject: "A"},
		"A is still active and must be kept",
	)
	checkInvariants(t, l)
}

func TestDeviceSlotIsReclaimedWhenBucketRefills(t *testing.T) {
	l, clk := newTestLimiter(testConfig()) // device 1/5s burst 2, 2 devices per subject

	requireAllowed(t, l.AllowDevice("s", 1))
	requireAllowed(t, l.AllowDevice("s", 1)) // tat = 10s
	requireAllowed(t, l.AllowDevice("s", 2)) // tat = 5s
	requireCapacityDenied(t, l.AllowDevice("s", 3), core.SubjectDeviceCapacity)

	clk.Advance(5100 * time.Millisecond) // device 2 refilled, device 1 not
	requireAllowed(t, l.AllowDevice("s", 3))
	require.NotContains(
		t,
		l.devices.entries,
		ticketRateKey{subject: "s", device: 2},
		"device 2 should have been reclaimed",
	)
	require.Equal(t, 2, l.devicesPerSubject["s"])
	checkInvariants(t, l)
}

func TestClockStepBackIsNotAnExploit(t *testing.T) {
	l, clk := newTestLimiter(testConfig())
	start := clk.Now()
	for range 3 {
		requireAllowed(t, l.AllowSubject("a"))
	}

	clk.Set(start.Add(-time.Hour))
	requireDenied(t, l.AllowSubject("a"), core.SubjectLimited, 1)
	requireAllowed(t, l.AllowSubject("new")) // must not panic or corrupt order
	checkInvariants(t, l)

	clk.Set(start.Add(time.Second))
	requireAllowed(t, l.AllowSubject("a"))
	checkInvariants(t, l)
}

func TestNormalizationAndExtremeConfig(t *testing.T) {
	l := newWithClock(Config{}, newFakeClock().Now)
	for range 20 { // default subject burst
		requireAllowed(t, l.AllowSubject("a"))
	}
	requireDenied(t, l.AllowSubject("a"), core.SubjectLimited, 1)

	huge := newWithClock(Config{
		SubjectInterval: time.Duration(1 << 62),
		SubjectBurst:    1 << 40,
		DeviceInterval:  time.Hour,
		DeviceBurst:     1 << 40,
	}, newFakeClock().Now)
	requireAllowed(t, huge.AllowSubject("a"))
	requireAllowed(t, huge.AllowDevice("a", 1))
}

// TestMatchesTokenBucketModel compares every decision, including Retry-After,
// against an independent integer token-bucket model on a random workload that
// is long enough to exercise pruning and re-creation of buckets.
func TestMatchesTokenBucketModel(t *testing.T) {
	cfg := Config{
		SubjectInterval:      time.Second,
		SubjectBurst:         4,
		DeviceInterval:       3 * time.Second,
		DeviceBurst:          3,
		MaxSubjectEntries:    1 << 20,
		MaxDeviceEntries:     1 << 20,
		MaxDevicesPerSubject: 1 << 20,
	}
	l, clk := newTestLimiter(cfg)
	origin := clk.Now()

	type state struct {
		credit int64
		seen   int64
	}

	model := make(map[ticketRateKey]*state)
	step := func(key ticketRateKey, interval time.Duration, burst int, now int64) (ok bool, wait int64) {
		iv, window := int64(interval), int64(interval)*int64(burst)
		s := model[key]
		if s == nil {
			s = &state{credit: window, seen: now}
			model[key] = s
		}

		s.credit = min(window, s.credit+now-s.seen)
		s.seen = now
		if s.credit >= iv {
			s.credit -= iv
			return true, 0
		}
		return false, iv - s.credit
	}

	rng := rand.New(rand.NewPCG(1, 2))
	steps := []time.Duration{
		0,
		0,
		0,
		100 * time.Millisecond,
		700 * time.Millisecond,
		time.Second,
		4 * time.Second,
		12 * time.Second,
	}

	for i := range 60_000 {
		clk.Advance(steps[rng.IntN(len(steps))])
		now := int64(clk.Now().Sub(origin))
		subject := "s" + strconv.Itoa(rng.IntN(6))

		var (
			got     core.Decision
			used    *ticketRateTable
			ok      bool
			wait    int64
			limited core.Reason
		)

		if rng.IntN(2) == 0 {
			got = l.AllowSubject(subject)
			used = l.subjects
			ok, wait = step(
				ticketRateKey{subject: subject},
				cfg.SubjectInterval,
				cfg.SubjectBurst,
				now,
			)
			limited = core.SubjectLimited
		} else {
			dev := uint64(1 + rng.IntN(5))
			got = l.AllowDevice(subject, dev)
			used = l.devices
			ok, wait = step(
				ticketRateKey{
					subject: subject,
					device:  dev,
				},
				cfg.DeviceInterval,
				cfg.DeviceBurst,
				now,
			)
			limited = core.DeviceLimited
		}

		want := core.Decision{}
		if !ok {
			want = core.Decision{Reason: limited, RetryAfter: ceilSeconds(wait)}
		}
		require.Equal(t, want, got, "op %d", i)

		// Expiry is exact: nothing that is full again may survive an allow.
		if len(used.heap) > 0 {
			require.Greater(t, used.heap[0].tat, l.last, "op %d: expired bucket survived prune", i)
		}
		if i%1000 == 0 {
			checkInvariants(t, l)
		}
	}
	checkInvariants(t, l)
}

func TestConcurrentAdmissionIsExact(t *testing.T) {
	// One token per hour: nothing refills during the test, so exactly burst
	// requests may pass no matter how the goroutines interleave.
	l := New(Config{
		SubjectInterval: time.Hour,
		SubjectBurst:    100,
	})

	var admitted atomic.Int64
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 50 {
				if l.AllowSubject("a").Reason == core.Allowed {
					admitted.Add(1)
				}
			}
		})
	}
	wg.Wait()
	require.Equal(t, int64(100), admitted.Load(), "want exactly 100 admitted requests")
}

func TestConcurrentMixedKeys(t *testing.T) {
	// Tiny tables and a short interval so capacity rejection, pruning and
	// re-creation all race with each other (run with -race).
	l := New(Config{
		SubjectInterval:      time.Millisecond,
		SubjectBurst:         2,
		DeviceInterval:       time.Millisecond,
		DeviceBurst:          2,
		MaxSubjectEntries:    8,
		MaxDeviceEntries:     16,
		MaxDevicesPerSubject: 3,
	})

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			rng := rand.New(rand.NewPCG(uint64(g), 7))
			for range 20_000 {
				s := "s" + strconv.Itoa(rng.IntN(12))
				if rng.IntN(2) == 0 {
					l.AllowSubject(s)
				} else {
					l.AllowDevice(s, uint64(1+rng.IntN(6)))
				}
			}
		})
	}
	wg.Wait()
	checkInvariants(t, l)
}

func BenchmarkAllowExistingKey(b *testing.B) {
	// Buckets stay alive (tat far in the future) and are never rejected.
	l := New(Config{
		SubjectInterval: time.Millisecond,
		SubjectBurst:    1 << 30,
	})
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		l.AllowSubject("subject")
	}
}

// BenchmarkAllowChurn creates a new bucket on every call; with a 1ns interval
// each one is pruned by the next call, so this is steady-state insert+prune.
func BenchmarkAllowChurn(b *testing.B) {
	keys := make([]string, 1<<16)
	for i := range keys {
		keys[i] = "subject-" + strconv.Itoa(i)
	}

	l := New(Config{
		SubjectInterval: time.Nanosecond,
		SubjectBurst:    1,
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.AllowSubject(keys[i&(len(keys)-1)])
	}
}

func BenchmarkAllowParallelManyKeys(b *testing.B) {
	keys := make([]string, 4096)
	for i := range keys {
		keys[i] = "subject-" + strconv.Itoa(i)
	}

	l := New(Config{
		SubjectInterval: time.Millisecond,
		SubjectBurst:    1 << 30,
	})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewPCG(rand.Uint64(), 1))
		for pb.Next() {
			l.AllowSubject(keys[rng.IntN(len(keys))])
		}
	})
}

// BenchmarkAllowLargeTable keeps 65k live buckets (a 1h interval keeps every
// tat far ahead of now) so the heap is at realistic depth (~16 levels) and
// every call takes the "existing bucket" path.
func BenchmarkAllowLargeTable(b *testing.B) {
	keys := make([]string, 1<<16)
	for i := range keys {
		keys[i] = "subject-" + strconv.Itoa(i)
	}

	l := New(Config{
		SubjectInterval:   time.Hour,
		SubjectBurst:      1 << 30,
		MaxSubjectEntries: 1 << 17,
	})

	for _, k := range keys {
		l.AllowSubject(k)
	}

	rng := rand.New(rand.NewPCG(3, 4))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		l.AllowSubject(keys[rng.IntN(len(keys))])
	}
}
