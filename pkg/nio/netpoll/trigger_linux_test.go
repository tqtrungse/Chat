//go:build linux

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package netpoll

func breakWakeupFD(p *Poller) func() {
	fd := p.epa.FD
	p.epa.FD = -1
	return func() { p.epa.FD = fd }
}
