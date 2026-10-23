//go:build darwin || dragonfly || freebsd || netbsd || openbsd || linux

/*
 * Copyright (c) 2019 The Gnet Authors. All rights reserved.
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
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
