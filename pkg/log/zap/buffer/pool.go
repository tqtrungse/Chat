// Copyright (c) 2016 Uber Technologies, Inc.
// SPDX-License-Identifier: MIT
//
// Forked from go.uber.org/zap/buffer/buffer.go at v1.28.0.

// Modified from go.uber.org/zap/buffer/pool.go.
//
// The original keeps each byte slice in one sync.Pool of Buffer objects. This
// version keeps the Buffer objects in a sync.Pool and the byte slices in
// xxx/pkg/pool/slice. The reasons are below.
//
// The slice pool puts each slice in a bucket for its size. The buckets hold
// the sizes 2^0 to 2^30. Therefore, one large entry cannot hold a large
// allocation for the life of the process. The original pool has no limit of
// size, so it gives a large slice to a small entry and keeps that allocation.
//
// The slice pool is also common to the complete program. A slice from a log
// entry can serve a read buffer of the network layer, and the opposite is
// also true.
//
// This version also replaces the Pool type of the original with one pool of
// this package. The original gives each Buffer a copy of its Pool, so that
// Free finds the pool of that Buffer. One pool needs no such field, and the
// question of the pool of a Buffer cannot have a wrong answer.
//
// The original holds the pool of the encoder in internal/bufferpool, so that
// the files of zapcore share one pool without an export of it. Get of this
// package serves that purpose, and the fork therefore drops that package.

package buffer

import (
	"sync"

	slicepool "xxx/pkg/pool/slice"
)

// objectPool holds the Buffer objects of the complete program. The bytes of
// each Buffer come from slicepool, so this pool holds no allocation of a size
// that one large entry decided.
var objectPool = &sync.Pool{
	New: func() any { return new(Buffer) },
}

// Get retrieves a Buffer from the pool, creating one if necessary.
//
// The Buffer object and its byte slice come from two different pools. The
// object pool gives a Put that costs no allocation, because the value is
// already a pointer. The slice pool gives the bytes.
func Get() *Buffer {
	buf := objectPool.Get().(*Buffer)
	if buf.bs == nil {
		buf.bs = slicepool.Get(size)
	}
	buf.bs = buf.bs[:0]
	return buf
}

// put gives the byte slice back to the slice pool and the Buffer object back
// to the object pool.
//
// A nil slice shows that a previous call already gave this Buffer back. Such a
// second call is an error of the caller. put then drops the Buffer object
// instead of a second Put. A second Put would give one Buffer object to two
// callers, and each caller would write in the same bytes.
func put(buf *Buffer) {
	if buf.bs == nil {
		return
	}
	slicepool.Put(buf.bs)
	buf.bs = nil
	objectPool.Put(buf)
}
