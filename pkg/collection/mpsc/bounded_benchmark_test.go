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

package mpsc

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"testing"

	"xxx/pkg/backoff"
)

func mpscProducerCounts() []int {
	procs := runtime.GOMAXPROCS(0)
	seen := map[int]bool{}
	var out []int
	for _, n := range []int{2, 4, 8, procs} {
		if n > 0 && !seen[n] {
			seen[n] = true
			out = append(out, n)
		}
	}
	return out
}

// ---- Push-only ----

func BenchmarkBounded_Push(b *testing.B) {
	for _, p := range mpscProducerCounts() {
		b.Run(fmt.Sprintf("mpsc/p=%d", p), func(b *testing.B) { boundedPush(b, p) })
		b.Run(fmt.Sprintf("chan/p=%d", p), func(b *testing.B) { chanPush(b, p) })
	}
}

func boundedPush(b *testing.B, producers int) {
	q, _ := NewBounded[int](benchCapacity)

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			_, closed := q.Pop()
			if closed {
				return
			}
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	share, rem := b.N/producers, b.N%producers
	for p := range producers {
		n := share
		if p == 0 {
			n += rem
		}
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := range n {
				q.Push(i)
			}
		}(n)
	}
	wg.Wait()

	b.StopTimer()
	q.Close()
	<-consumerDone
}

func chanPush(b *testing.B, producers int) {
	ch := make(chan int, benchCapacity)

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for range ch {
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	share, rem := b.N/producers, b.N%producers
	for p := range producers {
		n := share
		if p == 0 {
			n += rem
		}
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := range n {
				ch <- i
			}
		}(n)
	}
	wg.Wait()

	b.StopTimer()
	close(ch)
	<-consumerDone
}

// ---- TryPush-only ----

func BenchmarkBounded_TryPush(b *testing.B) {
	for _, p := range mpscProducerCounts() {
		b.Run(fmt.Sprintf("mpsc/p=%d", p), func(b *testing.B) { boundedTryPush(b, p) })
		b.Run(fmt.Sprintf("chan/p=%d", p), func(b *testing.B) { chanTryPush(b, p) })
	}
}

func boundedTryPush(b *testing.B, producers int) {
	q, _ := NewBounded[int](benchCapacity)

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for {
			_, closed := q.Pop()
			if closed {
				return
			}
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	share, rem := b.N/producers, b.N%producers
	for p := range producers {
		n := share
		if p == 0 {
			n += rem
		}
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			var bkc backoff.Cpu
			for i := 0; i < n; {
				if q.TryPush(i) {
					i++
					bkc.Reset()
				} else {
					bkc.Snooze()
				}
			}
		}(n)
	}
	wg.Wait()

	b.StopTimer()
	q.Close()
	<-consumerDone
}

func chanTryPush(b *testing.B, producers int) {
	ch := make(chan int, benchCapacity)

	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		for range ch {
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	share, rem := b.N/producers, b.N%producers
	for p := range producers {
		n := share
		if p == 0 {
			n += rem
		}
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			var bkc backoff.Cpu
			for i := 0; i < n; {
				select {
				case ch <- i:
					i++
					bkc.Reset()
				default:
					bkc.Snooze()
				}
			}
		}(n)
	}
	wg.Wait()

	b.StopTimer()
	close(ch)
	<-consumerDone
}

// ---- Pop-only ----

func BenchmarkBounded_Pop(b *testing.B) {
	b.Run("mpsc", boundedPop)
	b.Run("chan", chanPop)
}

func boundedPop(b *testing.B) {
	q, _ := NewBounded[int](math.MaxUint16)
	for i := range math.MaxUint16 {
		q.Push(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range math.MaxUint16 {
		q.Pop()
	}
}

func chanPop(b *testing.B) {
	ch := make(chan int, math.MaxUint16)
	for i := range math.MaxUint16 {
		ch <- i
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range math.MaxUint16 {
		<-ch
	}
}

// ---- Push + Pop (pipeline) ----

func BenchmarkBounded_PushPop(b *testing.B) {
	for _, p := range mpscProducerCounts() {
		b.Run(fmt.Sprintf("mpsc/p=%d", p), func(b *testing.B) { boundedPushPop(b, p) })
		b.Run(fmt.Sprintf("chan/p=%d", p), func(b *testing.B) { chanPushPop(b, p) })
	}
}

func boundedPushPop(b *testing.B, producers int) {
	q, _ := NewBounded[int](benchCapacity)

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup

	wg.Go(func() {
		for consumed := 0; consumed < b.N; consumed++ {
			_, closed := q.Pop()
			if closed {
				return
			}
		}
	})

	share, rem := b.N/producers, b.N%producers
	for p := range producers {
		n := share
		if p == 0 {
			n += rem
		}
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := range n {
				q.Push(i)
			}
		}(n)
	}
	wg.Wait()
}

func chanPushPop(b *testing.B, producers int) {
	ch := make(chan int, benchCapacity)

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup

	wg.Go(func() {
		for consumed := 0; consumed < b.N; consumed++ {
			<-ch
		}
	})

	share, rem := b.N/producers, b.N%producers
	for p := range producers {
		n := share
		if p == 0 {
			n += rem
		}
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for i := range n {
				ch <- i
			}
		}(n)
	}
	wg.Wait()
}
