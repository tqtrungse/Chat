package session

import (
	"errors"
	"sync"
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

var ErrSendUnavailable = errors.New("session send unavailable")

type Data struct {
	Keys          sharedsession.Keys
	DeviceID      shareddevice.ID
	LastHeartBeat atomic.Int64
	State         atomic.Uint32

	// recvSeq is the sequence number expected on the next client->server
	// frame. Only the event-loop goroutine that owns the connection may touch
	// it, so it needs no synchronization.
	recvSeq uint64

	sendMu     sync.Mutex
	sendSeq    uint64

	// sendFailed marks the outbound stream unusable after an AsyncWrite error;
	// the frame may have been partially written, so later frames must not queue.
	// It is atomic because the async write callback can set it concurrently.
	sendFailed atomic.Bool
}

// RecvSeq returns the sequence number the next client->server frame must carry.
// Event-loop goroutine only.
func (d *Data) RecvSeq() uint64 {
	return d.recvSeq
}

// AdvanceRecv must be called exactly once per successfully verified frame, and
// never after a failure (the connection must be closed instead).
// Event-loop goroutine only.
func (d *Data) AdvanceRecv() {
	d.recvSeq++
}

// WithSendSeq serializes sequence allocation with the caller's enqueue
// operation, preserving the sequence order on the TCP stream. The callback
// returns enqueued=true once it has handed the frame to AsyncWrite. If that
// enqueue reports an error, the stream is marked unusable because the write
// may still have been queued.
func (d *Data) WithSendSeq(enqueue func(seq uint64) (enqueued bool, err error)) error {
	d.sendMu.Lock()
	defer d.sendMu.Unlock()

	if d.sendFailed.Load() {
		return ErrSendUnavailable
	}

	enqueued, err := enqueue(d.sendSeq)
	if enqueued {
		if err == nil {
			d.sendSeq++
		} else {
			d.sendFailed.Store(true)
		}
	}
	return err
}

// MarkSendFailed prevents any more frames from being queued after an
// asynchronous socket write fails.
func (d *Data) MarkSendFailed() {
	d.sendFailed.Store(true)
}

func (d *Data) SendFailed() bool {
	return d.sendFailed.Load()
}
