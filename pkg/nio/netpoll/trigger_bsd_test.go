//go:build darwin || dragonfly || freebsd || netbsd || openbsd

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package netpoll

func breakWakeupFD(p *Poller) func() {
	if len(p.pipe) == 2 {
		fd := p.pipe[1]
		p.pipe[1] = -1
		return func() { p.pipe[1] = fd }
	}
	fd := p.fd
	p.fd = -1
	return func() { p.fd = fd }
}
