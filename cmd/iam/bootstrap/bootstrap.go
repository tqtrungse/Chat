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
	"net/http"

	iamhttp "xxx/internal/iam/adapter/http"
	"xxx/internal/iam/application/exchange_key"
	"xxx/internal/iam/infrastructure/crypto"
	"xxx/internal/iam/infrastructure/datastore/mysql"

	"xxx/pkg"
	"xxx/pkg/database/sql"
	"xxx/pkg/log"

	"go.uber.org/zap"
)

// Run constructs IAM's dependencies, starts its HTTPS API, and shuts it down
// gracefully when ctx is canceled.
func Run(ctx context.Context) (retErr error) {
	logger, closeLogger := log.NewLogger(nil)
	defer func() {
		if err := closeLogger(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("failed to close logger: %w", err))
		}
	}()

	cfgLoader, err := pkg.LoadConfig("iam")
	if err != nil {
		return fmt.Errorf("failed to load IAM config: %w", err)
	}

	cfgHttp := iamhttp.Config{}
	cfgHttp.Load(cfgLoader)
	if err = validateHTTPConfig(cfgHttp); err != nil {
		return err
	}

	var cfgDb sql.Config
	cfgDb.Load(cfgLoader)

	ticketSigner, err := loadTicketSigner(cfgLoader)
	if err != nil {
		return err
	}

	db := sql.NewMySql()
	if err = db.Open(ctx, cfgDb); err != nil {
		return fmt.Errorf("failed to open MySQL: %w", err)
	}
	defer func() {
		if err = db.Close(); err != nil {
			logger.Error("failed to close MySQL", zap.Error(err))
			retErr = errors.Join(retErr, fmt.Errorf("failed to close MySQL: %w", err))
		} else {
			logger.Info("closed MySQL")
		}
		_ = logger.Sync()
	}()

	keyExchanger := exchange_key.New(
		db,
		crypto.NewHkdfDeriver(),
		mysql.NewDeviceMetaReader(),
		ticketSigner,
	)
	server, err := iamhttp.NewServer(
		ctx,
		cfgHttp,
		logger,
		keyExchanger,
	)
	if err != nil {
		return fmt.Errorf("failed to initialize IAM HTTP server: %w", err)
	}

	serverResult := make(chan error, 1)
	go func() {
		serverResult <- server.Run()
	}()
	logger.Info("IAM HTTP server started", zap.String("address", cfgHttp.Addr))
	_ = logger.Sync()

	select {
	case err = <-serverResult:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("failed to run IAM HTTP server: %w", err)

	case <-ctx.Done():
		logger.Info("shutting down IAM HTTP server")
		_ = logger.Sync()

		shutdownErr := server.Shutdown()
		serverErr := <-serverResult
		if errors.Is(serverErr, http.ErrServerClosed) {
			serverErr = nil
		} else if serverErr != nil {
			serverErr = fmt.Errorf("failed to run IAM HTTP server: %w", serverErr)
		}
		return errors.Join(shutdownErr, serverErr)
	}
}

func validateHTTPConfig(cfg iamhttp.Config) error {
	if cfg.Addr == "" {
		return errors.New("HTTP_SERVER_ADDR is required")
	}
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return errors.New("HTTP_SERVER_CERT_FILE and HTTP_SERVER_KEY_FILE are required; IAM serves HTTPS")
	}
	return nil
}
