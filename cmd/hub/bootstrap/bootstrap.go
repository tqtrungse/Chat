/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"xxx/internal/hub/adapter/tcp"
	"xxx/internal/hub/application/activateconn"
	"xxx/internal/hub/core/activation"
	"xxx/internal/hub/core/connection"
	"xxx/internal/hub/core/presence"
	"xxx/internal/hub/core/protocol"
	"xxx/internal/hub/core/rejection"
	"xxx/internal/hub/infrastructure/authorization"
	"xxx/internal/hub/infrastructure/crypto"
	"xxx/internal/hub/infrastructure/noise"
	"xxx/internal/hub/infrastructure/redis"
	"xxx/internal/shared/device"

	"xxx/pkg/breaker"
	"xxx/pkg/config"
	"xxx/pkg/k8s"
	"xxx/pkg/log"
	workerpool "xxx/pkg/pool/worker"
	"xxx/pkg/reject"

	"go.uber.org/zap"
)

// Run constructs hub's dependencies, starts its TCP API, and shuts it down
// gracefully when ctx is canceled.
func Run(ctx context.Context) (retErr error) {
	logger, closeLogger, err := log.NewLogger(nil)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}

	defer func() {
		closeErr := closeLogger()
		if closeErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("failed close logger: %w", closeErr))
		}
	}()

	loader, err := config.Load("hub")
	if err != nil {
		return fmt.Errorf("failed to load hub config: %w", err)
	}

	err = ctx.Err()
	if err != nil {
		return err
	}

	hubID := loader.GetUint64("HUB_ID")
	if hubID == 0 {
		serviceID := loader.GetUint64("SERVICE_ID")
		if serviceID == 0 {
			return fmt.Errorf("service ID is required")
		}

		hubID, err = k8s.MachineIDFromHostname(serviceID)
		if err != nil {
			return fmt.Errorf("failed to get machine ID from hostname %w", err)
		}
	}

	cfgTcp := tcp.Config{}
	cfgTcp.Load(loader)
	if len(cfgTcp.Addr) == 0 {
		return errors.New("TCP_SERVER_ADDRESS is required")
	}
	if !strings.Contains(cfgTcp.Addr, "://") {
		cfgTcp.Addr = "tcp://" + cfgTcp.Addr
	}

	cfgCache := redis.Config{}
	cfgCache.Load(loader)
	if len(cfgCache.Addr) == 0 {
		return errors.New("DISTRIBUTED_CACHE_ADDRESS is required")
	}

	var (
		cfgPool     workerpool.Config
		cfgManager  connection.Config
		cfgSyncer   presence.Config
		cfgActivate activateconn.Config
		cfgDCacheCB breaker.Config
	)
	cfgPool.Load(loader)
	cfgManager.Load(loader)
	cfgSyncer.Load(loader)
	cfgActivate.Load(loader)
	cfgDCacheCB = loadDistCacheCircuitBreakerConfig(loader)

	ticketVerifier, err := loadTicketVerifier(loader)
	if err != nil {
		return err
	}

	var cfgNoise noise.Config
	err = cfgNoise.Load(loader)
	if err != nil {
		return err
	}

	cfgActivate, err = resolveActivationConfig(cfgActivate, cfgNoise, cfgTcp, cfgManager)
	if err != nil {
		return err
	}

	keyring, err := noise.LoadKeyring(cfgNoise.KeyringFile, cfgNoise.CurrentKeyID)
	if err != nil {
		return err
	}
	defer keyring.Close()

	factory, err := noise.NewFactory(keyring, cfgNoise.Audience, cfgTcp.MaxFrameBytes)
	if err != nil {
		return err
	}

	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(nil)

	pool, closePool := workerpool.New(
		cfgPool,
		func(workerID int, recovered any, stack []byte) {
			logger.Error(
				"hub worker panicked",
				zap.Int("workerID", workerID),
				zap.Any("panic", recovered),
				zap.ByteString("stack", stack),
			)
		},
	)
	defer closePool()

	dCache := redis.NewCache(hubID, cfgCache, logger, pool)
	cacheStarted := false
	defer func() {
		// Drain queued cache operations while Redis is still available, then
		// remove this hub's presence using a fresh shutdown context.
		cancelRun(nil)
		closePool()

		if cacheStarted {
			shutdownCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx),
				10*time.Second,
			)

			err2 := dCache.ReapHub(shutdownCtx)
			cancel()

			if err2 != nil {
				retErr = errors.Join(retErr, fmt.Errorf("failed to reap hub presence: %w", err2))
			}
		}

		err2 := dCache.Close()
		if err2 != nil {
			retErr = errors.Join(retErr, fmt.Errorf("failed to close cache: %w", err2))
		}
	}()

	err = dCache.SetCallbacks(
		redis.Callbacks{
			OnHubDeleted:    func(uint64) {},
			OnDeviceDeleted: func(uint64, device.ID) {},
			OnSelfReaped: func() {
				logger.Error("hub registration was lost; shutting down", zap.Uint64("hubID", hubID))
				cancelRun(errors.New("hub cache registration was lost"))
			},
		},
	)
	if err != nil {
		return fmt.Errorf("failed to configure cache callbacks: %w", err)
	}

	err = dCache.Start(runCtx)
	if err != nil {
		return fmt.Errorf("failed to start cache: %w", err)
	}
	cacheStarted = true

	encoder := protocol.NewEncoder()
	decoder := protocol.NewDecoder()
	syncer := presence.NewSyncer(
		runCtx,
		cfgSyncer,
		dCache,
		breaker.New(
			cfgDCacheCB,
			dCache.IsInfraError,
			nil,
		),
		logger,
		pool,
	)
	manager := connection.NewManager(
		cfgManager,
		syncer,
		logger,
		encoder,
	)
	defer manager.Close()

	activationSvc := activation.NewService(
		crypto.NewEd25519Signer(),
		ticketVerifier,
	)

	verifyExecutor, closeVerifier := authorization.New(
		cfgActivate.VerifyWorkers,
		cfgActivate.MaxVerifyJobs,
	)
	defer closeVerifier()

	rejections := reject.New[rejection.Reason](
		logger,
		5*time.Second,
		rejection.Names(),
	)

	activator := activateconn.New(
		cfgActivate,
		logger,
		rejections,
		decoder,
		manager,
		activationSvc,
		factory,
		verifyExecutor,
	)

	server := tcp.NewServer(
		runCtx,
		cfgTcp,
		logger,
		rejections,
		activator,
		nil,
		manager.Close,
	)

	return runTCPServer(
		runCtx,
		server,
		logger,
		cfgTcp.Addr,
		hubID,
	)
}

func runTCPServer(
	ctx context.Context,
	server *tcp.Server,
	logger *log.Logger,
	address string,
	hubID uint64,
) error {
	serverResult := make(chan error, 1)
	go func() { serverResult <- server.Run() }()

	// OnBoot publishes the engine before Shutdown can read it. Run may
	// return without OnBoot when binding or initialization fails.
	select {
	case err := <-serverResult:
		return tcpServerError(err)
	case <-server.Ready():
	case <-ctx.Done():
		select {
		case err := <-serverResult:
			return tcpServerError(err)
		case <-server.Ready():
		}
	}

	if ctx.Err() == nil {
		logger.Info(
			"hub TCP server started",
			zap.String("address", address),
			zap.Uint64("hubID", hubID),
		)

		select {
		case err := <-serverResult:
			return tcpServerError(err)
		case <-ctx.Done():
		}
	}

	logger.Info("shutting down hub TCP server")
	err := server.Shutdown()
	if err != nil {
		return fmt.Errorf("failed to shutdown hub TCP server: %w", err)
	}

	serverErr := tcpServerError(<-serverResult)
	cause := context.Cause(ctx)
	if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
		cause = nil
	}
	return errors.Join(cause, serverErr)
}

func tcpServerError(err error) error {
	if err != nil {
		return fmt.Errorf("failed to run hub TCP server: %w", err)
	}
	return nil
}
