/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package nats

import (
	"errors"
	"os"
	"sync/atomic"
	"time"

	"xxx/pkg/log"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

var (
	ErrLoggerNil       = errors.New("logger is nil")
	ErrShuttingDownNil = errors.New("shutting down is nil")
	ErrOnFatalCbNil    = errors.New("onFatal callback is nil")
)

type ConnConfig struct {
	// Name sets the client name
	Name string

	// MaxReconnects sets the maximum number of reconnect attempts.
	// If negative, it will never stop trying to reconnect.
	//
	// Defaults to 60.
	MaxReconnects int

	// ReconnectWait sets the wait time between reconnect attempts.
	//
	// Defaults to 2s.
	ReconnectWait time.Duration
}

func DefaultConnConfig() ConnConfig {
	hostname, _ := os.Hostname()
	return ConnConfig{
		Name:          hostname,
		MaxReconnects: 60,
		ReconnectWait: 2 * time.Second,
	}
}

func Connect(
	url string,
	cfg ConnConfig,
	logger *log.Logger,
	shuttingDown *atomic.Bool,
	onFatalClose func(),
) (*nats.Conn, error) {
	if logger == nil {
		return nil, ErrLoggerNil
	}
	if shuttingDown == nil {
		return nil, ErrShuttingDownNil
	}
	if onFatalClose == nil {
		return nil, ErrOnFatalCbNil
	}

	return nats.Connect(
		url,
		nats.Name(cfg.Name),
		nats.MaxReconnects(cfg.MaxReconnects),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			logger.Warn("NATS disconnected", zap.Error(err))
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			logger.Info("NATS reconnected", zap.String("url", nc.ConnectedUrl()))
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			if shuttingDown.Load() {
				logger.Info("NATS connection closed gracefully")
				return
			}
			logger.Error("NATS connection closed unexpectedly — unrecoverable", zap.Error(nc.LastError()))
			// page/alert + fail the readiness probe, or os.Exit(1) so the orchestrator restarts the process
			// metrics.NatsAsyncErrors.Inc() // if Prometheus/OTel is already set up
			onFatalClose()
		}),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			logger.Error("NATS async error", zap.Error(err))
			// metrics.NatsAsyncErrors.Inc() // if Prometheus/OTel is already set up
		}),
	)
}
