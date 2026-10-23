/*
 * Copyright (c) 2026 tqtrungse@gmail.com. All rights reserved.
 * Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
 */

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	iamhttp "xxx/internal/iam/adapter/http"
	"xxx/internal/iam/application/connticket"
	"xxx/internal/iam/core/rejection"
	"xxx/internal/iam/infrastructure/mysql"
	"xxx/internal/iam/infrastructure/ticketlimit"

	"xxx/pkg/config"
	"xxx/pkg/database/sql"
	"xxx/pkg/log"
	"xxx/pkg/reject"

	"go.uber.org/zap"
)

// Run constructs IAM's dependencies, starts its HTTPS API, and shuts it down
// gracefully when ctx is canceled.
func Run(ctx context.Context) (retErr error) {
	logger, closeLogger, err := log.NewLogger(nil)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}

	defer func() {
		closeErr := closeLogger()
		if closeErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("failed to close logger: %w", closeErr))
		}
	}()

	loader, err := config.Load("iam")
	if err != nil {
		return fmt.Errorf("failed to load IAM config: %w", err)
	}

	cfgHttp := iamhttp.Config{}
	cfgHttp.Load(loader)

	var cfgTicketLimit ticketlimit.Config
	cfgTicketLimit.Load(loader)

	err = validateHTTPConfig(cfgHttp)
	if err != nil {
		return err
	}

	var cfgDb sql.Config
	cfgDb.Load(loader)

	ticketSigner, err := loadTicketSigner(loader)
	if err != nil {
		return err
	}

	db := sql.NewMySql()
	closeDB, err := db.Open(ctx, cfgDb)
	if err != nil {
		return fmt.Errorf("failed to open MySQL: %w", err)
	}

	defer func() {
		closeErr := closeDB()
		if closeErr != nil {
			logger.Error("failed to close MySQL", zap.Error(closeErr))
			retErr = errors.Join(retErr, fmt.Errorf("failed to close MySQL: %w", closeErr))
		} else {
			logger.Info("closed MySQL")
		}
		_ = logger.Sync()
	}()

	hub, err := loadHubEndpoint(loader)
	if err != nil {
		return err
	}

	ticketIssuer, err := connticket.New(
		db,
		mysql.NewDeviceMetaReader(),
		ticketSigner,
		*hub,
	)
	if err != nil {
		return err
	}

	rejections := reject.New[rejection.Reason](
		logger,
		5*time.Second,
		rejection.Names(),
	)

	server, err := iamhttp.NewServer(
		ctx,
		cfgHttp,
		logger,
		rejections,
		ticketIssuer,
		ticketlimit.New(cfgTicketLimit),
	)
	if err != nil {
		return fmt.Errorf("failed to initialize IAM HTTP server: %w", err)
	}

	serverResult := make(chan error, 1)
	go func() { serverResult <- server.Run() }()

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
