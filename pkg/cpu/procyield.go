//go:build amd64 || arm64

/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package cpu

// procYield loops and executes a hardware pause instruction to back off the CPU.
func procYield(cycles int)
