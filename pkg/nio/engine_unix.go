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
	"context"
	"errors"
	"sync/atomic"

	"xxx/pkg/collection/swiss"
	"xxx/pkg/hash"
	errorx "xxx/pkg/nio/error"
	"xxx/pkg/nio/netpoll"
	"xxx/pkg/nio/queue"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type engine struct {
	listeners    *swiss.Table[int, *listener] // listeners for accepting incoming connections
	opts         *Options                     // options with engine
	ingress      *eventloop                   // main event-loop that monitors all listeners
	eventLoops   loadBalancer                 // event-loops for handling events
	inShutdown   atomic.Bool                  // whether the engine is in shutdown
	turnOff      context.CancelFunc
	eventHandler EventHandler // user eventHandler
	concurrency  struct {
		*errgroup.Group

		ctx context.Context
	}
}

func (eng *engine) isShutdown() bool {
	return eng.inShutdown.Load()
}

// shutdown signals the engine to shut down.
func (eng *engine) shutdown(err error) {
	if err != nil && !errors.Is(err, errorx.ErrEngineShutdown) {
		eng.opts.Logger.Error("engine is being shutdown with error", zap.Error(err))
	}
	// Cancel the context to stop the engine.
	eng.turnOff()
}

func (eng *engine) closeEventLoops() {
	eng.eventLoops.iterate(func(_ int, el *eventloop) bool {
		el.listeners.Range(func(_ int, ln *listener) (goOn bool) {
			ln.close()
			return true
		})
		_ = el.poller.Close()
		return true
	})
	if eng.ingress != nil {
		eng.listeners.Range(func(_ int, ln *listener) (goOn bool) {
			ln.close()
			return true
		})
		err := eng.ingress.poller.Close()
		if err != nil {
			eng.opts.Logger.Error("failed to close poller when stopping engine", zap.Error(err))
		}
	}
}

func (eng *engine) runEventLoops(ctx context.Context, numEventLoop int, maxConns int) error {
	var el0 *eventloop
	lns := eng.listeners
	// Create loops locally and bind the listeners.
	for i := range numEventLoop {
		if i > 0 {
			lns = swiss.NewTableWithCap[int, *listener](eng.listeners.Len(), hash.Int)
			var (
				err error
				ln  *listener
			)
			eng.listeners.Range(func(_ int, l *listener) (goOn bool) {
				ln, err = initListener(l.network, l.address, eng.opts)
				if err != nil {
					return false
				}
				lns.Set(ln.fd, ln)
				return true
			})
			if err != nil {
				lns.Range(func(_ int, ln *listener) bool {
					ln.close()
					return true
				})
				return err
			}
		}
		p, err := netpoll.OpenPoller(eng.opts.Logger)
		if err != nil {
			return err
		}
		el := new(eventloop)
		el.listeners = lns
		el.engine = eng
		el.poller = p
		el.buffer = make([]byte, eng.opts.ReadBufferCap)
		el.connections.init(maxConns / numEventLoop)
		el.eventHandler = eng.eventHandler
		lns.Range(func(_ int, ln *listener) (goOn bool) {
			if err = el.poller.AddRead(ln.packPollAttachment(el.accept), false); err != nil {
				return false
			}
			return true
		})
		if err != nil {
			return err
		}

		eng.eventLoops.register(el)

		// Start the ticker.
		if eng.opts.Ticker && el.idx == 0 {
			el0 = el
		}
	}

	// Start event-loops in the background.
	eng.eventLoops.iterate(func(_ int, el *eventloop) bool {
		eng.concurrency.Go(el.run)
		return true
	})

	if el0 != nil {
		eng.concurrency.Go(func() error {
			el0.ticker(ctx)
			return nil
		})
	}

	return nil
}

func (eng *engine) activateReactors(ctx context.Context, numEventLoop int, maxConns int) error {
	for range numEventLoop {
		p, err := netpoll.OpenPoller(eng.opts.Logger)
		if err != nil {
			return err
		}
		el := new(eventloop)
		el.listeners = eng.listeners
		el.engine = eng
		el.poller = p
		el.buffer = make([]byte, eng.opts.ReadBufferCap)
		el.connections.init(maxConns / numEventLoop)
		el.eventHandler = eng.eventHandler
		eng.eventLoops.register(el)
	}

	// Start sub reactors in the background.
	eng.eventLoops.iterate(func(_ int, el *eventloop) bool {
		eng.concurrency.Go(el.orbit)
		return true
	})

	p, err := netpoll.OpenPoller(eng.opts.Logger)
	if err != nil {
		return err
	}
	el := new(eventloop)
	el.listeners = eng.listeners
	el.idx = -1
	el.engine = eng
	el.poller = p
	el.eventHandler = eng.eventHandler
	eng.listeners.Range(func(_ int, ln *listener) (goOn bool) {
		if err = el.poller.AddRead(ln.packPollAttachment(el.accept0), true); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	eng.ingress = el

	// Start the main reactor in the background.
	eng.concurrency.Go(el.rotate)

	// Start the ticker.
	if eng.opts.Ticker {
		eng.concurrency.Go(func() error {
			eng.ingress.ticker(ctx)
			return nil
		})
	}

	return nil
}

func (eng *engine) start(ctx context.Context, numEventLoop int, maxConns int) error {
	if eng.opts.ReusePort {
		return eng.runEventLoops(ctx, numEventLoop, maxConns)
	}
	return eng.activateReactors(ctx, numEventLoop, maxConns)
}

func (eng *engine) stop(ctx context.Context, s Engine) {
	// Wait on a signal for shutdown
	<-ctx.Done()

	eng.eventHandler.OnShutdown(s)

	// Notify all event-loops to exit.
	eng.eventLoops.iterate(func(i int, el *eventloop) bool {
		err := el.poller.Trigger(queue.HighPriority, func(_ any) error { return errorx.ErrEngineShutdown }, nil)
		if err != nil {
			eng.opts.Logger.Error("failed to enqueue shutdown signal of high-priority", zap.Int("event-loop", i), zap.Error(err))
		}
		return true
	})
	if eng.ingress != nil {
		err := eng.ingress.poller.Trigger(queue.HighPriority, func(_ any) error { return errorx.ErrEngineShutdown }, nil)
		if err != nil {
			eng.opts.Logger.Error("failed to enqueue shutdown signal of high-priority for main event-loop", zap.Error(err))
		}
	}

	if err := eng.concurrency.Wait(); err != nil {
		eng.opts.Logger.Error("engine shutdown error", zap.Error(err))
	}

	// Close all listeners and pollers of event-loops.
	eng.closeEventLoops()

	// Put the engine into the shutdown state.
	eng.inShutdown.Store(true)
}
