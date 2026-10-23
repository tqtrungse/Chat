//go:build arm && linux

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package netpoll

// poll_opt is default and only mode for Linux.
type epollevent struct {
	events uint32
	_pad   uint32
	data   [8]byte // to match amd64
}
