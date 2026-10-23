// Copyright (c) 2016 Uber Technologies, Inc.
// SPDX-License-Identifier: MIT
//
// Forked from go.uber.org/zap/buffer/buffer.go at v1.28.0.

// Package pool provides internal pool utilities.
// Forked from go.uber.org/zap/internal/pool/pool.go at v1.28.0, unchanged.
// The forked encoder needs it, and the original is an internal package.

package pool

import (
	"sync"
)

// A Pool is a generic wrapper around [sync.Pool] to provide strongly-typed
// object pooling.
//
// Note that SA6002 (ref: https://staticcheck.io/docs/checks/#SA6002) will
// not be detected, so all internal pool use must take care to only store
// pointer types.
type Pool[T any] struct {
	pool sync.Pool
}

// New returns a new [Pool] for T, and will use fn to construct new Ts when
// the pool is empty.
func New[T any](fn func() T) *Pool[T] {
	return &Pool[T]{
		pool: sync.Pool{
			New: func() any {
				return fn()
			},
		},
	}
}

// Get gets a T from the pool, or creates a new one if the pool is empty.
func (p *Pool[T]) Get() T {
	return p.pool.Get().(T)
}

// Put returns x into the pool.
func (p *Pool[T]) Put(x T) {
	p.pool.Put(x)
}
