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

	hubtcp "xxx/internal/hub/adapter/tcp"
	"xxx/internal/hub/application/accept_conn"
	"xxx/internal/hub/application/activate_conn"
	"xxx/internal/hub/connection"
	hubcb "xxx/internal/hub/infrastructure/cb"
	hubcrypto "xxx/internal/hub/infrastructure/crypto"
	hubredis "xxx/internal/hub/infrastructure/redis"
	"xxx/internal/hub/protocol"
	shareddevice "xxx/internal/shared/device"
	sharedsession "xxx/internal/shared/session"

	"xxx/pkg"
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
			retErr = errors.Join(retErr, fmt.Errorf("close logger: %w", err))
		}
	}()

	cfgLoader, err := pkg.LoadConfig("hub")
	if err != nil {
		return fmt.Errorf("load hub config: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return err
	}

	var tcpConfig hubtcp.Config
	tcpConfig.Load(cfgLoader)
	tcpConfig.Addr = strings.TrimSpace(tcpConfig.Addr)
	if tcpConfig.Addr == "" {
		return errors.New("TCP_SERVER_ADDRESS is required")
	}
	if !strings.Contains(tcpConfig.Addr, "://") {
		tcpConfig.Addr = "tcp://" + tcpConfig.Addr
	}
	if tcpConfig.ReadBufferCap == 0 {
		tcpConfig.ReadBufferCap = 4096
	}
	if tcpConfig.WriteBufferCap == 0 {
		tcpConfig.WriteBufferCap = 4096
	}

	var (
		routerConfig   connection.Config
		activateConfig activate_conn.Config
		cacheConfig    hubredis.Config
		breakerConfig  hubcb.Config
		workerConfig   workerpool.Config
	)
	routerConfig.Load(cfgLoader)
	activateConfig.Load(cfgLoader)
	cacheConfig.Load(cfgLoader)
	breakerConfig.Load(cfgLoader)
	workerConfig.Load(cfgLoader)
	if strings.TrimSpace(cacheConfig.Addr) == "" {
		return errors.New("DISTRIBUTED_CACHE_ADDRESS is required for activation")
	}

	ticketVerifier, err := loadTicketVerifier(cfgLoader)
	if err != nil {
		return err
	}

	hubID := cfgLoader.GetUint64("HUB_ID")
	if hubID == 0 {
		serviceID := cfgLoader.GetUint64("SERVICE_ID")
		if serviceID == 0 {
			return fmt.Errorf("service ID is required")
		}

		hubID, err = pkg.MachineIDFromHostname(serviceID)
		if err != nil {
			return fmt.Errorf("failed to get machine ID from hostname %w", err)
		}
	}

	runCtx, cancelRun := context.WithCancelCause(ctx)
	defer cancelRun(nil)
	pool, closePool := workerpool.New(workerConfig, func(workerID int, recovered any, stack []byte) {
		logger.Error("hub worker panicked",
			zap.Int("workerID", workerID),
			zap.Any("panic", recovered),
			zap.ByteString("stack", stack),
		)
	})
	defer closePool()

	dCache, startCache, closeCache, setCallbacks := hubredis.NewCache(hubID, cacheConfig, logger, pool)
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
				retErr = errors.Join(retErr, fmt.Errorf("reap hub presence: %w", err2))
			}
		}
		if err2 := closeCache(); err2 != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close Redis: %w", err2))
		}
	}()
	setCallbacks(hubredis.Callbacks{
		OnHubDeleted:    func(uint64) {},
		OnDeviceDeleted: func(uint64, shareddevice.ID) {},
		OnSelfReaped: func() {
			logger.Error("hub registration was lost; shutting down", zap.Uint64("hubID", hubID))
			cancelRun(errors.New("hub Redis registration was lost"))
		},
	})
	if err = startCache(runCtx); err != nil {
		return fmt.Errorf("start Redis presence: %w", err)
	}
	cacheStarted = true

	channel := hubcrypto.NewAesGcmChannel()
	encoder := protocol.NewEncoder(channel)
	decoder := protocol.NewDecoder(channel)
	breaker := hubcb.New(breakerConfig, dCache.IsInfraError, nil)
	router := connection.NewRouter(
		runCtx, routerConfig, hubcrypto.NewEd25519Signer(), ticketVerifier,
		dCache, breaker, logger, pool, encoder,
	)
	defer router.Close()
	acceptor := accept_conn.New(router)
	activator := activate_conn.New(activateConfig, logger, encoder, decoder, router)
	server := hubtcp.NewServer(runCtx, tcpConfig, logger, acceptor, activator, nil, router.Close)
	return runTCPServer(runCtx, server, logger, tcpConfig.Addr, hubID)
}

func runTCPServer(ctx context.Context, server *hubtcp.Server, logger *log.Logger, address string, hubID uint64) error {
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
		logger.Info("hub TCP server started", zap.String("address", address), zap.Uint64("hubID", hubID))
		select {
		case err := <-serverResult:
			return tcpServerError(err)
		case <-ctx.Done():
		}
	}

	logger.Info("shutting down hub TCP server")
	if err := server.Shutdown(); err != nil {
		return fmt.Errorf("shutdown hub TCP server: %w", err)
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
	return fmt.Errorf("run hub TCP server: %w", err)
}

func loadTicketVerifier(cfgLoader pkg.ConfigLoader) (*sharedsession.TicketVerifier, error) {
	keys, err := sharedsession.ParseTicketEncryptKeys(
		strings.Split(cfgLoader.GetString("TICKET_ENCRYPT_KEYS"), ","),
	)
	if err != nil {
		return nil, fmt.Errorf("parse ticket encryption keys: %w", err)
	}
	defer func() {
		for _, key := range keys {
			clear(key)
		}
	}()

	sealer, err := sharedsession.NewTicketSealer(
		cfgLoader.GetUint8("TICKET_ENCRYPT_CURRENT_KEY_ID"), keys,
	)
	if err != nil {
		return nil, fmt.Errorf("initialize ticket decryption: %w", err)
	}

	publicKeys, err := sharedsession.ParseTicketSigningPublicKeys(
		strings.Split(cfgLoader.GetString("TICKET_SIGNNING_PUBLIC_KEYS"), ","),
	)
	if err != nil {
		return nil, fmt.Errorf("parse hub ticket public keys: %w", err)
	}

	clockSkew, err := time.ParseDuration(strings.TrimSpace(cfgLoader.GetString("TICKET_CLOCK_SKEW")))
	if err != nil {
		return nil, fmt.Errorf("parse TICKET_CLOCK_SKEW: %w", err)
	}

	return sharedsession.NewTicketVerifier(
		sealer,
		publicKeys,
		clockSkew,
	)
}
