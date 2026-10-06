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

package boostrap

//import (
//	"context"
//	"fmt"
//	"log"
//	"os"
//	"os/signal"
//	"sync/atomic"
//	"syscall"
//	"xxx/internal/hub/application/accept_conn"
//	"xxx/internal/hub/application/exchange_key"
//	"xxx/internal/hub/application/usecase"
//	"xxx/internal/hub/connection"
//	"xxx/internal/hub/infrastructure/crypto"
//	"xxx/internal/hub/infrastructure/nats"
//	"xxx/internal/hub/infrastructure/redis"
//	"xxx/internal/hub/protocol"
//	log2 "xxx/pkg/log"
//
//	"go.uber.org/zap"
//)
//
//func Run() {
//	var (
//		logger      = log2.NewLogger(nil)
//		config, err = NewViperConfig("../", ".env.test")
//	)
//
//	if err != nil {
//		logger.Fatal("failed to load config", zap.Error(err))
//		if closeErr := logger.close(); closeErr != nil {
//			log.Fatal("failed to close logger", closeErr)
//		}
//		return
//	}
//
//	var (
//		ctx, cancel   = context.WithCancel(context.Background())
//		infra         = config.Infra()
//		secureChannel = crypto.NewAesGcmChannel()
//		signature     = crypto.NewEd25519Signer()
//		keyDeriver    = crypto.NewHkdfDeriver()
//		encoder       = protocol.NewEncoder(secureChannel)
//		decoder       = protocol.NewDecoder(secureChannel)
//		router        = connection.NewRouter(config.Router(), logger, encoder, signature)
//		connAceptor   = accept_conn.New(router)
//		connActivator = usecase.NewConnActivator(logger, decoder, router)
//		keyExchanger  = exchange_key.New(keyDeriver, nil, router)
//	)
//
//	_, err = redis.NewCache(
//		ctx,
//		infra.HubID,
//		config.Redis(),
//		logger,
//		router,
//	)
//	if err != nil {
//		logger.Fatal("failed to init Redis cache", zap.Error(err))
//		cancel()
//
//		if closeErr := logger.close(); closeErr != nil {
//			log.Fatal("failed to close logger", closeErr)
//		}
//		return
//	}
//
//	var (
//		isShuttingDown = atomic.Bool{}
//		cc             = config.NatsConn()
//	)
//	cc.Name = fmt.Sprintf("hub:%d:%s", infra.HubID, cc.Name)
//
//	nc, js, err := nats.Connect(
//		infra.NatsURL,
//		cc,
//		&isShuttingDown,
//		logger,
//		nil,
//	)
//	if err != nil {
//		logger.Fatal("failed to init NATS connection", zap.Error(err))
//		cancel()
//
//		if closeErr := logger.close(); closeErr != nil {
//			log.Fatal("failed to close logger", closeErr)
//		}
//		return
//	}
//
//	_, err = nats.NewBroker(
//		infra.HubID,
//		js,
//		config.Nats(),
//		logger,
//		router,
//	)
//	if err != nil {
//		logger.Fatal("failed to init NATS router", zap.Error(err))
//		cancel()
//
//		if closeErr := logger.close(); closeErr != nil {
//			log.Fatal("failed to close logger", closeErr)
//		}
//		return
//	}
//
//	sigChan := make(chan os.Signal, 1)
//	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
//
//	go func() {
//		<-sigChan
//		cancel()
//
//		isShuttingDown.Store(true)
//		nc.Close()
//
//		if closeErr := logger.close(); closeErr != nil {
//			log.Fatal("failed to close logger", closeErr)
//		}
//	}()
//}
