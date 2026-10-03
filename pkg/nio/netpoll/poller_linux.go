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

// IOFlags represents the flags of IO events.
type IOFlags = uint16

// IOEvent is the integer type of I/O events on Linux.
type IOEvent = uint32

const (
	// InitPollEventsCap represents the initial capacity of poller event-list.
	InitPollEventsCap = 128
	// MaxPollEventsCap is the maximum limitation of events that the poller can process.
	MaxPollEventsCap = 1024
	// MinPollEventsCap is the minimum limitation of events that the poller can process.
	MinPollEventsCap = 32
	// MaxAsyncTasksAtOneTime is the maximum amount of asynchronous tasks that the event-loop will process at one time.
	MaxAsyncTasksAtOneTime = 256
	// ReadEvents represents readable events that are polled by epoll.
	ReadEvents = unix.EPOLLIN | unix.EPOLLPRI
	// WriteEvents represents writeable events that are polled by epoll.
	WriteEvents = unix.EPOLLOUT
	// ReadWriteEvents represents both readable and writeable events.
	ReadWriteEvents = ReadEvents | WriteEvents
	// ErrEvents represents exceptional events that occurred.
	ErrEvents = unix.EPOLLERR | unix.EPOLLHUP
)

// IsReadEvent checks if the event is a read event.
func IsReadEvent(event IOEvent) bool {
	return event&ReadEvents != 0
}

// IsWriteEvent checks if the event is a write event.
func IsWriteEvent(event IOEvent) bool {
	return event&WriteEvents != 0
}

// IsErrorEvent checks if the event is an error event.
func IsErrorEvent(event IOEvent, _ IOFlags) bool {
	return event&ErrEvents != 0
}

type eventList struct {
	size   int
	events []epollevent
}

func newEventList(size int) *eventList {
	return &eventList{size, make([]epollevent, size)}
}

func (el *eventList) expand() {
	if newSize := el.size << 1; newSize <= MaxPollEventsCap {
		el.size = newSize
		el.events = make([]epollevent, newSize)
	}
}

func (el *eventList) shrink() {
	if newSize := el.size >> 1; newSize >= MinPollEventsCap {
		el.size = newSize
		el.events = make([]epollevent, newSize)
	}
}

// Poller represents a poller which is in charge of monitoring file-descriptors.
//
// poll_opt is default and only mode.
type Poller struct {
	fd                          int             // epoll fd
	epa                         *PollAttachment // PollAttachment for waking events
	efdBuf                      []byte          // efd buffer to read an 8-byte integer
	wakeupCall                  atomic.Int32
	asyncTaskQueue              queue.AsyncTaskQueue // queue with low priority
	urgentAsyncTaskQueue        queue.AsyncTaskQueue // queue with high priority
	logger                      *log.Logger
	highPriorityEventsThreshold int32 // threshold of high-priority events
}

// OpenPoller instantiates a poller.
func OpenPoller(logger *log.Logger) (poller *Poller, err error) {
	poller = new(Poller)
	if poller.fd, err = unix.EpollCreate1(unix.EPOLL_CLOEXEC); err != nil {
		poller = nil
		err = os.NewSyscallError("epoll_create1", err)
		return
	}
	var efd int
	if efd, err = unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC); err != nil {
		_ = poller.Close()
		poller = nil
		err = os.NewSyscallError("eventfd", err)
		return
	}
	poller.efdBuf = make([]byte, 8)
	poller.epa = &PollAttachment{FD: efd}
	if err = poller.AddRead(poller.epa, true); err != nil {
		_ = poller.Close()
		poller = nil
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
	_ = unix.Close(p.epa.FD)
	return os.NewSyscallError("close", unix.Close(p.fd))
}

// Make the endianness of bytes compatible with more linux OSs under different processor-architectures,
// according to http://man7.org/linux/man-pages/man2/eventfd.2.html.
var (
	u uint64 = 1
	b        = (*(*[8]byte)(unsafe.Pointer(&u)))[:]
)

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
		for {
			_, err = unix.Write(p.epa.FD, b)
			if errors.Is(err, unix.EAGAIN) {
				_, _ = unix.Read(p.epa.FD, p.efdBuf)
				continue
			}
			break
		}
	}
	return os.NewSyscallError("write", err)
}

// Polling blocks the current goroutine, monitoring the registered file descriptors and waiting for network I/O.
// When I/O occurs on any of the file descriptors, the provided callback function is invoked.
func (p *Poller) Polling() error {
	var (
		doChores bool
		el       = newEventList(InitPollEventsCap)
	)

	msec := -1
	for {
		n, err := epollWait(p.fd, el.events, msec)
		if n == 0 || (n < 0 && errors.Is(err, unix.EINTR)) {
			msec = -1
			runtime.Gosched()
			continue
		} else if err != nil {
			//logging.Errorf("error occurs in epoll: %v", os.NewSyscallError("epoll_wait", err))
			return err
		}
		msec = 0

		for i := 0; i < n; i++ {
			ev := &el.events[i]
			pollAttachment := restorePollAttachment(unsafe.Pointer(&ev.data))
			if pollAttachment.FD == p.epa.FD { // poller is awakened to run tasks in queues.
				doChores = true
			} else {
				err = pollAttachment.Callback(pollAttachment.FD, ev.events, 0)
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
			for i := 0; i < MaxAsyncTasksAtOneTime; i++ {
				if task, ok = p.asyncTaskQueue.Dequeue(); !ok {
					break
				}
				err = task.Exec(task.Param)
				if errors.Is(err, errorx.ErrEngineShutdown) {
					return err
				}
			}

			p.wakeupCall.Store(0)
			if (!p.asyncTaskQueue.IsEmpty() || !p.urgentAsyncTaskQueue.IsEmpty()) &&
				p.wakeupCall.CompareAndSwap(0, 1) {
				for {
					_, err = unix.Write(p.epa.FD, b)
					if errors.Is(err, unix.EAGAIN) {
						_, _ = unix.Read(p.epa.FD, p.efdBuf)
						continue
					}
					if err != nil {
						p.logger.Error("failed to notify next round of event-loop for leftover tasks", zap.Error(os.NewSyscallError("write", err)))
					}
					break
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
	var ev epollevent
	ev.events = ReadWriteEvents
	if edgeTriggered {
		ev.events |= unix.EPOLLET | unix.EPOLLRDHUP
	}
	convertPollAttachment(unsafe.Pointer(&ev.data), pa)
	return os.NewSyscallError("epoll_ctl add", epollCtl(p.fd, unix.EPOLL_CTL_ADD, pa.FD, &ev))
}

// AddRead registers the given file descriptor with readable event to the poller.
func (p *Poller) AddRead(pa *PollAttachment, edgeTriggered bool) error {
	var ev epollevent
	ev.events = ReadEvents
	if edgeTriggered {
		ev.events |= unix.EPOLLET | unix.EPOLLRDHUP
	}
	convertPollAttachment(unsafe.Pointer(&ev.data), pa)
	return os.NewSyscallError("epoll_ctl add", epollCtl(p.fd, unix.EPOLL_CTL_ADD, pa.FD, &ev))
}

// AddWrite registers the given file descriptor with writable event to the poller.
func (p *Poller) AddWrite(pa *PollAttachment, edgeTriggered bool) error {
	var ev epollevent
	ev.events = WriteEvents
	if edgeTriggered {
		ev.events |= unix.EPOLLET | unix.EPOLLRDHUP
	}
	convertPollAttachment(unsafe.Pointer(&ev.data), pa)
	return os.NewSyscallError("epoll_ctl add", epollCtl(p.fd, unix.EPOLL_CTL_ADD, pa.FD, &ev))
}

// ModRead modifies the given file descriptor with readable event in the poller.
func (p *Poller) ModRead(pa *PollAttachment, edgeTriggered bool) error {
	var ev epollevent
	ev.events = ReadEvents
	if edgeTriggered {
		ev.events |= unix.EPOLLET | unix.EPOLLRDHUP
	}
	convertPollAttachment(unsafe.Pointer(&ev.data), pa)
	return os.NewSyscallError("epoll_ctl mod", epollCtl(p.fd, unix.EPOLL_CTL_MOD, pa.FD, &ev))
}

// ModReadWrite modifies the given file descriptor with readable and writable events in the poller.
func (p *Poller) ModReadWrite(pa *PollAttachment, edgeTriggered bool) error {
	var ev epollevent
	ev.events = ReadWriteEvents
	if edgeTriggered {
		ev.events |= unix.EPOLLET | unix.EPOLLRDHUP
	}
	convertPollAttachment(unsafe.Pointer(&ev.data), pa)
	return os.NewSyscallError("epoll_ctl mod", epollCtl(p.fd, unix.EPOLL_CTL_MOD, pa.FD, &ev))
}

// Delete removes the given file descriptor from the poller.
func (p *Poller) Delete(fd int) error {
	return os.NewSyscallError("epoll_ctl del", epollCtl(p.fd, unix.EPOLL_CTL_DEL, fd, nil))
}
