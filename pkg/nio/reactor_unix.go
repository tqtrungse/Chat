//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 *
 * Modified from the original gnet source by tqtrungse@gmail.com in 2026.
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

package nio

import (
	"errors"
	"runtime"

	errorx "xxx/pkg/nio/error"

	"go.uber.org/zap"
)

func (el *eventloop) rotate() error {
	if el.engine.opts.LockOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	err := el.poller.Polling()
	if errors.Is(err, errorx.ErrEngineShutdown) {
		el.getLogger().Debug("main reactor is exiting in terms of the demand from user", zap.Error(err))
		err = nil
	} else if err != nil {
		el.getLogger().Error("main reactor is exiting due to error", zap.Error(err))
	}

	el.engine.shutdown(err)

	return err
}

func (el *eventloop) orbit() error {
	if el.engine.opts.LockOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	err := el.poller.Polling()
	if errors.Is(err, errorx.ErrEngineShutdown) {
		el.getLogger().Debug("event-loop is exiting in terms of the demand from user", zap.Int("event-loop-idx", el.idx), zap.Error(err))
		err = nil
	} else if err != nil {
		el.getLogger().Error("event-loop is exiting due to error", zap.Int("event-loop-idx", el.idx), zap.Error(err))
	}

	el.closeConns()
	el.engine.shutdown(err)

	return err
}

func (el *eventloop) run() error {
	if el.engine.opts.LockOSThread {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	err := el.poller.Polling()
	if errors.Is(err, errorx.ErrEngineShutdown) {
		el.getLogger().Debug("event-loop is exiting in terms of the demand from user", zap.Int("event-loop-idx", el.idx), zap.Error(err))
		err = nil
	} else if err != nil {
		el.getLogger().Error("event-loop is exiting due to error", zap.Int("event-loop-idx", el.idx), zap.Error(err))
	}

	el.closeConns()
	el.engine.shutdown(err)

	return err
}
