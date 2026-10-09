package session

import (
	"sync/atomic"

	"xxx/internal/hub/domain/device"
)

type State uint32

const (
	StateUnactive = State(0)
	StateActive   = State(1)
	StateClosed   = State(2)
)

// Keys are directional, named from the server's point of view:
// Recv* protects client->server, Send* protects server->client.
type Keys struct {
	RecvEnc [32]byte
	SendEnc [32]byte
	RecvMac [32]byte
	SendMac [32]byte
}

type Data struct {
	Keys          Keys
	DeviceID      device.ID
	LastHeartBeat atomic.Int64
	State         atomic.Uint32
}
