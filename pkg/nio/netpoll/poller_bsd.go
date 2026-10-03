//go:build darwin || dragonfly || freebsd || netbsd || openbsd

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package netpoll

import (
	"errors"
	"os"
	"runtime"
	"sync/atomic"
	"unsafe"

	"xxx/pkg/log"
	errorx "xxx/pkg/nio/error"
	"xxx/pkg/nio/queue"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const (
	// InitPollEventsCap represents the initial capacity of poller event-list.
	InitPollEventsCap = 64
	// MaxPollEventsCap is the maximum limitation of events that the poller can process.
	MaxPollEventsCap = 512
	// MinPollEventsCap is the minimum limitation of events that the poller can process.
	MinPollEventsCap = 16
	// MaxAsyncTasksAtOneTime is the maximum amount of asynchronous tasks that the event-loop will process at one time.
	MaxAsyncTasksAtOneTime = 128
	// ReadEvents represents readable events that are polled by kqueue.
	ReadEvents = unix.EVFILT_READ
	// WriteEvents represents writeable events that are polled by kqueue.
	WriteEvents = unix.EVFILT_WRITE
	// ReadWriteEvents represents both readable and writeable events.
	ReadWriteEvents = ReadEvents | WriteEvents
	// ErrEvents represents exceptional events that occurred.
	ErrEvents = unix.EV_EOF | unix.EV_ERROR
)

// IsReadEvent checks if the event is a read event.
func IsReadEvent(event IOEvent) bool {
	return event == ReadEvents
}

// IsWriteEvent checks if the event is a write event.
func IsWriteEvent(event IOEvent) bool {
	return event == WriteEvents
}

// IsErrorEvent checks if the event is an error event.
func IsErrorEvent(_ IOEvent, flags IOFlags) bool {
	return flags&ErrEvents != 0
}

type eventList struct {
	size   int
	events []unix.Kevent_t
}

func newEventList(size int) *eventList {
	return &eventList{size, make([]unix.Kevent_t, size)}
}

func (el *eventList) expand() {
	if newSize := el.size << 1; newSize <= MaxPollEventsCap {
		el.size = newSize
		el.events = make([]unix.Kevent_t, newSize)
	}
}

func (el *eventList) shrink() {
	if newSize := el.size >> 1; newSize >= MinPollEventsCap {
		el.size = newSize
		el.events = make([]unix.Kevent_t, newSize)
	}
}

// Poller represents a poller which is in charge of monitoring file-descriptors.
//
// poll_opt is default and only mode.
type Poller struct {
	fd                          int
	pipe                        []int
	wakeupCall                  atomic.Int32
	asyncTaskQueue              queue.AsyncTaskQueue // queue with low priority
	urgentAsyncTaskQueue        queue.AsyncTaskQueue // queue with high priority
	logger                      *log.Logger
	highPriorityEventsThreshold int32 // threshold of high-priority events
}

// OpenPoller instantiates a poller.
func OpenPoller(logger *log.Logger) (poller *Poller, err error) {
	poller = new(Poller)
	if poller.fd, err = unix.Kqueue(); err != nil {
		poller = nil
		err = os.NewSyscallError("kqueue", err)
		return
	}
	if err = poller.addWakeupEvent(); err != nil {
		_ = poller.Close()
		poller = nil
		err = os.NewSyscallError("kevent | pipe2", err)
		return
	}
	poller.logger = logger
	poller.asyncTaskQueue = queue.NewAsyncTaskQueue()
	poller.urgentAsyncTaskQueue = queue.NewAsyncTaskQueue()
	poller.highPriorityEventsThreshold = MaxPollEventsCap
	return
}

// Close closes the poller.
func (p *Poller) Close() error {
	if len(p.pipe) == 2 {
		_ = unix.Close(p.pipe[0])
		_ = unix.Close(p.pipe[1])
	}
	return os.NewSyscallError("close", unix.Close(p.fd))
}

// Trigger enqueues task and wakes up the poller to process pending tasks.
// By default, any incoming task will enqueued into urgentAsyncTaskQueue
// before the threshold of high-priority events is reached. When it happens,
// any asks other than high-priority tasks will be shunted to asyncTaskQueue.
//
// Note that asyncTaskQueue is a queue of low-priority whose size may grow large and tasks in it may backlog.
func (p *Poller) Trigger(priority queue.EventPriority, fn queue.Func, param any) (err error) {
	if priority > queue.HighPriority && p.urgentAsyncTaskQueue.Length() >= p.highPriorityEventsThreshold {
		p.asyncTaskQueue.Enqueue(queue.Task{
			Exec:  fn,
			Param: param,
		})
	} else {
		// There might be some low-priority tasks overflowing into urgentAsyncTaskQueue in a flash,
		// but that's tolerable because it ought to be a rare case.
		p.urgentAsyncTaskQueue.Enqueue(queue.Task{
			Exec:  fn,
			Param: param,
		})
	}
	if p.wakeupCall.CompareAndSwap(0, 1) {
		err = p.wakePoller()
	}
	return os.NewSyscallError("kevent | write", err)
}

// Polling blocks the current goroutine, monitoring the registered file descriptors and waiting for network I/O.
// When I/O occurs on any of the file descriptors, the provided callback function is invoked.
func (p *Poller) Polling() error {
	var (
		ts       unix.Timespec
		tsp      *unix.Timespec
		doChores bool
		el       = newEventList(InitPollEventsCap)
	)

	for {
		n, err := unix.Kevent(p.fd, nil, el.events, tsp)
		if n == 0 || (n < 0 && errors.Is(err, unix.EINTR)) {
			tsp = nil
			runtime.Gosched()
			continue
		} else if err != nil {
			p.logger.Error("error occurs in kqueue", zap.Error(os.NewSyscallError("kevent wait", err)))
			return err
		}
		tsp = &ts

		for i := range n {
			ev := &el.events[i]
			if ev.Ident == 0 { // poller is awakened to run tasks in queues
				doChores = true
				p.drainWakeupEvent()
			} else {
				pollAttachment := restorePollAttachment(unsafe.Pointer(&ev.Udata))
				err = pollAttachment.Callback(int(ev.Ident), ev.Filter, ev.Flags)
				if errors.Is(err, errorx.ErrAcceptSocket) || errors.Is(err, errorx.ErrEngineShutdown) {
					return err
				}
			}
		}

		if doChores {
			doChores = false
			task, ok := p.urgentAsyncTaskQueue.Dequeue()
			for ok {
				err = task.Exec(task.Param)
				if errors.Is(err, errorx.ErrEngineShutdown) {
					return err
				}
				task, ok = p.urgentAsyncTaskQueue.Dequeue()
			}
			for range MaxAsyncTasksAtOneTime {
				if task, ok = p.asyncTaskQueue.Dequeue(); !ok {
					break
				}

				err = task.Exec(task.Param)
				if errors.Is(err, errorx.ErrEngineShutdown) {
					return err
				}
			}

			p.wakeupCall.Store(0)
			if (!p.asyncTaskQueue.IsEmpty() || !p.urgentAsyncTaskQueue.IsEmpty()) && p.wakeupCall.CompareAndSwap(0, 1) {
				if err = p.wakePoller(); err != nil {
					doChores = true
				}
			}
		}

		if n == el.size {
			el.expand()
		} else if n < el.size>>1 {
			el.shrink()
		}
	}
}

// AddReadWrite registers the given file descriptor with readable and writable events to the poller.
func (p *Poller) AddReadWrite(pa *PollAttachment, edgeTriggered bool) error {
	var evs [2]unix.Kevent_t
	evs[0].Ident = keventIdent(pa.FD)
	evs[0].Filter = unix.EVFILT_READ
	evs[0].Flags = unix.EV_ADD
	if edgeTriggered {
		evs[0].Flags |= unix.EV_CLEAR
	}
	convertPollAttachment(unsafe.Pointer(&evs[0].Udata), pa)
	evs[1] = evs[0]
	evs[1].Filter = unix.EVFILT_WRITE
	_, err := unix.Kevent(p.fd, evs[:], nil, nil)
	return os.NewSyscallError("kevent add", err)
}

// AddRead registers the given file descriptor with readable event to the poller.
func (p *Poller) AddRead(pa *PollAttachment, edgeTriggered bool) error {
	var evs [1]unix.Kevent_t
	evs[0].Ident = keventIdent(pa.FD)
	evs[0].Filter = unix.EVFILT_READ
	evs[0].Flags = unix.EV_ADD
	if edgeTriggered {
		evs[0].Flags |= unix.EV_CLEAR
	}
	convertPollAttachment(unsafe.Pointer(&evs[0].Udata), pa)
	_, err := unix.Kevent(p.fd, evs[:], nil, nil)
	return os.NewSyscallError("kevent add", err)
}

// AddWrite registers the given file descriptor with writable event to the poller.
func (p *Poller) AddWrite(pa *PollAttachment, edgeTriggered bool) error {
	var evs [1]unix.Kevent_t
	evs[0].Ident = keventIdent(pa.FD)
	evs[0].Filter = unix.EVFILT_WRITE
	evs[0].Flags = unix.EV_ADD
	if edgeTriggered {
		evs[0].Flags |= unix.EV_CLEAR
	}
	convertPollAttachment(unsafe.Pointer(&evs[0].Udata), pa)
	_, err := unix.Kevent(p.fd, evs[:], nil, nil)
	return os.NewSyscallError("kevent add", err)
}

// ModRead modifies the given file descriptor with readable event in the poller.
func (p *Poller) ModRead(pa *PollAttachment, _ bool) error {
	var evs [1]unix.Kevent_t
	evs[0].Ident = keventIdent(pa.FD)
	evs[0].Filter = unix.EVFILT_WRITE
	evs[0].Flags = unix.EV_DELETE
	_, err := unix.Kevent(p.fd, evs[:], nil, nil)
	return os.NewSyscallError("kevent delete", err)
}

// ModReadWrite modifies the given file descriptor with readable and writable events in the poller.
func (p *Poller) ModReadWrite(pa *PollAttachment, edgeTriggered bool) error {
	var evs [1]unix.Kevent_t
	evs[0].Ident = keventIdent(pa.FD)
	evs[0].Filter = unix.EVFILT_WRITE
	evs[0].Flags = unix.EV_ADD
	if edgeTriggered {
		evs[0].Flags |= unix.EV_CLEAR
	}
	convertPollAttachment(unsafe.Pointer(&evs[0].Udata), pa)
	_, err := unix.Kevent(p.fd, evs[:], nil, nil)
	return os.NewSyscallError("kevent add", err)
}

// Delete removes the given file descriptor from the poller.
func (p *Poller) Delete(_ int) error {
	return nil
}
