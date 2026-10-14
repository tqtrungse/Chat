//go:build darwin || dragonfly || freebsd || netbsd || openbsd

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
