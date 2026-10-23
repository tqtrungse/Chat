/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package batcher

import (
	"testing"
	"unsafe"
	"xxx/pkg/cpu"

	"github.com/stretchr/testify/require"
)

func TestPartitionSizeIsCacheLineMultiple(t *testing.T) {
	size := unsafe.Sizeof(partition[int]{})
	require.Truef(
		t,
		size%cpu.CacheLineSize == 0,
		"sizeof(partition) = %d, not a multiple of CacheLineSize (%d)", size, cpu.CacheLineSize,
	)
}
