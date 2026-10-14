//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

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

import "unsafe"

// PollEventHandler is the callback for I/O events notified by the poller.
type PollEventHandler func(int, IOEvent, IOFlags) error

// PollAttachment is the user data which is about to be stored in "void *ptr" of epoll_data or "void *udata" of kevent.
type PollAttachment struct {
	FD       int
	Callback PollEventHandler
}

func convertPollAttachment(ptr unsafe.Pointer, attachment *PollAttachment) {
	// epoll_event stores pointer bytes at an address that need not be aligned
	// for a Go pointer (notably the packed 12-byte event on Linux/amd64).
	// Copy bytes rather than dereferencing **PollAttachment at that address.
	copy((*[unsafe.Sizeof(attachment)]byte)(ptr)[:],
		(*[unsafe.Sizeof(attachment)]byte)(unsafe.Pointer(&attachment))[:])
}

func restorePollAttachment(ptr unsafe.Pointer) *PollAttachment {
	var attachment *PollAttachment
	copy((*[unsafe.Sizeof(attachment)]byte)(unsafe.Pointer(&attachment))[:],
		(*[unsafe.Sizeof(attachment)]byte)(ptr)[:])
	return attachment
}
