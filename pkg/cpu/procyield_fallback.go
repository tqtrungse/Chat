//go:build !amd64 && !arm64

/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package cpu

// procYield is a fallback for architectures that don't have
// dedicated assembly implementations.
func procYield(cycles int) {
	// A simple, cheap volatile-like operation or a simple loop
	// to waste a few cycles without eating the entire pipeline.
	for i := 0; i < cycles; i++ {
		// On generic architectures, we just let the loop run.
		// It's not as power-efficient as a hardware PAUSE,
		// but it ensures your code still compiles and runs anywhere.
	}
}
