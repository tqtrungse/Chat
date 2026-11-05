//go:build darwin || dragonfly || freebsd || openbsd

/*
 * Copyright (c) 2024 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package netpoll

// IOFlags represents the flags of IO events.
type IOFlags = uint16

// IOEvent is the integer type of I/O events on BSD's.
type IOEvent = int16
