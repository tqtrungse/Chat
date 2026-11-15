// Copyright (c) 2016 Uber Technologies, Inc.
// SPDX-License-Identifier: MIT
//
// Forked from go.uber.org/zap/internal/pool/pool.go at v1.28.0.

// The code of this file has no change from upstream. The forked encoder needs
// this package, and the original is an internal package. Therefore, the fork
// cannot import it.

// Package pool gives internal pool utilities.
package pool

import (
	"sync"
)

// A Pool is a generic wrapper around [sync.Pool]. It gives an object pool with
// strong types.
//
// Staticcheck does not find SA6002 (ref:
// https://staticcheck.io/docs/checks/#SA6002) through this wrapper. Therefore,
// each internal use of the pool must store only pointer types.
type Pool[T any] struct {
	pool sync.Pool
}

// New returns a new [Pool] for T. When the pool is empty, the Pool calls fn to
// make a new T.
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

// Put returns x to the pool.
func (p *Pool[T]) Put(x T) {
	p.pool.Put(x)
}
