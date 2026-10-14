//go:build linux

package netpoll

func breakWakeupFD(p *Poller) func() {
	fd := p.epa.FD
	p.epa.FD = -1
	return func() { p.epa.FD = fd }
}
