/*
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

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"xxx/internal/hub/adapter/tcp"
	"xxx/internal/hub/application/activate_conn"
	"xxx/internal/hub/connection"
	"xxx/internal/hub/infrastructure/cb"
	"xxx/internal/hub/infrastructure/crypto"
	"xxx/internal/hub/infrastructure/redis"
	"xxx/internal/hub/protocol"
	shareddevice "xxx/internal/shared/device"

	"xxx/pkg/config"
	"xxx/pkg/k8s"
	"xxx/pkg/log"
	workerpool "xxx/pkg/pool/worker"

	"go.uber.org/zap"
)

// Run constructs hub's dependencies, starts its TCP API, and shuts it down
// gracefully when ctx is canceled.
func Run(ctx context.Context) (retErr error) {
	logger, closeLogger := log.NewLogger(nil)
	defer func() {
		if err := closeLogger(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("failed close logger: %w", err))
		}
	}()

	loader, err := config.Load("hub")
	if err != nil {
		return fmt.Errorf("failed to load hub config: %w", err)
	}
	if err = ctx.Err(); err != nil {
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
		cfgCb       cb.Config
		cfgPool     workerpool.Config
		cfgRouter   connection.Config
		cfgActivate activate_conn.Config
	)
	cfgCb.Load(loader)
	cfgPool.Load(loader)
	cfgRouter.Load(loader)
	cfgActivate.Load(loader)

	ticketVerifier, err := loadTicketVerifier(loader)
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

	dCache, startCache, closeCache, setCallbacks := redis.NewCache(hubID, cfgCache, logger, pool)
	cacheStarted := false
	defer func() {
		// Drain queued cache operations while Redis is still available, then
		// remove this hub's presence using a fresh shutdown context.
		cancelRun(nil)
		closePool()
		if cacheStarted {
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			err2 := dCache.ReapHub(shutdownCtx)
			cancel()
			if err2 != nil {
				retErr = errors.Join(retErr, fmt.Errorf("failed to reap hub presence: %w", err2))
			}
		}
		if err2 := closeCache(); err2 != nil {
			retErr = errors.Join(retErr, fmt.Errorf("failed to close cache: %w", err2))
		}
	}()
	setCallbacks(
		redis.Callbacks{
			OnHubDeleted:    func(uint64) {},
			OnDeviceDeleted: func(uint64, shareddevice.ID) {},
			OnSelfReaped: func() {
				logger.Error("hub registration was lost; shutting down", zap.Uint64("hubID", hubID))
				cancelRun(errors.New("hub cache registration was lost"))
			},
		},
	)
	if err = startCache(runCtx); err != nil {
		return fmt.Errorf("failed to start cache: %w", err)
	}
	cacheStarted = true

	channel := crypto.NewAesGcmChannel()
	encoder := protocol.NewEncoder(channel)
	decoder := protocol.NewDecoder(channel)
	breaker := cb.New(
		cfgCb,
		dCache.IsInfraError,
		nil,
	)
	router := connection.NewRouter(
		runCtx,
		cfgRouter,
		crypto.NewEd25519Signer(),
		ticketVerifier,
		dCache,
		breaker,
		logger,
		pool,
		encoder,
	)
	defer router.Close()

	activator := activate_conn.New(
		cfgActivate,
		logger,
		encoder,
		decoder,
		router,
	)
	server := tcp.NewServer(
		runCtx,
		cfgTcp,
		logger,
		activator,
		nil,
		router.Close,
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
	if err := server.Shutdown(); err != nil {
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
	if err == nil {
		return nil
	}
	return fmt.Errorf("failed to run hub TCP server: %w", err)
}
