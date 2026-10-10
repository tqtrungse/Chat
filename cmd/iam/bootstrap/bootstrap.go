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
	"os"
	"path/filepath"
	"strings"

	iamhttp "xxx/internal/iam/adapter/http"
	"xxx/internal/iam/application/exchange_key"
	iamcrypto "xxx/internal/iam/infrastructure/crypto"
	iammysql "xxx/internal/iam/infrastructure/datastore/mysql"
	sharedsession "xxx/internal/shared/session"

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
			retErr = errors.Join(retErr, fmt.Errorf("close logger: %w", err))
		}
	}()

	loader, err := loadConfig()
	if err != nil {
		return fmt.Errorf("load IAM config: %w", err)
	}

	var httpConfig iamhttp.Config
	httpConfig.Load(loader)
	if err = validateHTTPConfig(httpConfig); err != nil {
		return err
	}

	var dbConfig sql.Config
	dbConfig.Load(loader)

	keys, err := sharedsession.ParseTicketKeys(loader.GetStringSlice("TICKET_KEYS"))
	if err != nil {
		return fmt.Errorf("parse ticket keys: %w", err)
	}

	ticketSealer, err := sharedsession.NewTicketSealer(
		loader.GetUint8("TICKET_CURRENT_KEY_ID"),
		keys,
	)
	if err != nil {
		return fmt.Errorf("initialize ticket sealer: %w", err)
	}

	db := sql.NewMySql()
	if err = db.Open(ctx, dbConfig); err != nil {
		return fmt.Errorf("open MySQL: %w", err)
	}
	defer func() {
		if err = db.Close(); err != nil {
			logger.Error("failed to close MySQL", zap.Error(err))
			retErr = errors.Join(retErr, fmt.Errorf("close MySQL: %w", err))
		} else {
			logger.Info("closed MySQL")
		}
		_ = logger.Sync()
	}()

	keyExchanger := exchange_key.New(
		db,
		iamcrypto.NewHkdfDeriver(),
		iammysql.NewDeviceMetaReader(),
		ticketSealer,
	)
	server, err := iamhttp.NewServer(ctx, httpConfig, logger, keyExchanger)
	if err != nil {
		return fmt.Errorf("initialize IAM HTTP server: %w", err)
	}

	serverResult := make(chan error, 1)
	go func() {
		serverResult <- server.Run()
	}()
	logger.Info("IAM HTTP server started", zap.String("address", httpConfig.Addr))
	_ = logger.Sync()

	select {
	case err = <-serverResult:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("run IAM HTTP server: %w", err)

	case <-ctx.Done():
		logger.Info("shutting down IAM HTTP server")
		_ = logger.Sync()
		shutdownErr := server.Shutdown()
		serverErr := <-serverResult
		if errors.Is(serverErr, http.ErrServerClosed) {
			serverErr = nil
		} else if serverErr != nil {
			serverErr = fmt.Errorf("run IAM HTTP server: %w", serverErr)
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

func loadConfig() (pkg.ConfigLoader, error) {
	path, err := findConfigPath()
	if err != nil {
		return nil, err
	}

	fileName := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	return pkg.DefaultConfigLoader(filepath.Dir(path), fileName)
}

func findConfigPath() (string, error) {
	if path := os.Getenv("IAM_CONFIG_FILE"); path != "" {
		absPath, err := filepath.Abs(path)
		if err != nil {
			return "", err
		}
		if _, err := os.Stat(absPath); err != nil {
			return "", fmt.Errorf("IAM_CONFIG_FILE %q: %w", absPath, err)
		}
		return absPath, nil
	}

	candidates := []string{
		filepath.Join("cmd", "iam", ".prod.env"),
		".prod.env",
	}
	if executable, err := os.Executable(); err == nil {
		candidates = append(candidates, filepath.Join(filepath.Dir(executable), ".prod.env"))
	}

	for _, candidate := range candidates {
		absPath, err := filepath.Abs(candidate)
		if err != nil {
			return "", err
		}

		_, err = os.Stat(absPath)
		if err == nil {
			return absPath, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("inspect IAM config %q: %w", absPath, err)
		}
	}
	return "", errors.New("IAM config not found; set IAM_CONFIG_FILE or provide cmd/iam/.prod.env")
}
