package session

import (
	"sync/atomic"

	shareddevice "xxx/internal/shared/device"
	sharedsession "xxx/internal/shared/session"
)

type State uint32

const (
	StateUnactive = State(0)
	StateActive   = State(1)
	StateClosed   = State(2)
)

type Data struct {
	Keys          sharedsession.Keys
	DeviceID      shareddevice.ID
	LastHeartBeat atomic.Int64
	State         atomic.Uint32
}
