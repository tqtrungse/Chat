//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package nio

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnContextSupportsNilAndMixedTypes(t *testing.T) {
	c := new(conn)
	require.Nil(t, c.Context())

	var typedNil *int
	for _, value := range []any{42, "device", &struct{ ID int }{42}, typedNil, nil} {
		c.SetContext(value)
		require.Equal(t, value, c.Context())
	}
	c.SetContext("session")
	c.release()
	require.Nil(t, c.Context(), "release must discard the attached context")
}

func TestConnContextConcurrentReadAndReplace(t *testing.T) {
	type state struct {
		id      uint64
		inverse uint64
	}

	var (
		c     = new(conn)
		start = make(chan struct{})
		wg    sync.WaitGroup
	)

	for worker := range 8 {
		wg.Go(func() {
			<-start
			for i := range 2000 {
				if worker%2 == 0 {
					id := uint64(worker*2000 + i)
					c.SetContext(&state{id: id, inverse: ^id})
					if i%2 == 0 {
						c.SetContext(nil)
					}
				} else if value := c.Context(); value != nil {
					s := value.(*state)
					if s.inverse != ^s.id {
						t.Error("context did not publish a fully initialized object")
						return
					}
				}
			}
		})
	}
	close(start)
	wg.Wait()

	c.SetContext("last")
	require.Equal(t, "last", c.Context())

	c.release()
	require.Nil(t, c.Context())
}
